// Sprint 1 / J1 — EIP-712 order signing for the Polymarket CTF Exchange.
//
// Polymarket's CLOB requires every order to be signed off-chain by the
// trader's wallet. The exchange contract verifies the signature on-chain
// when the order matches. Our job is to produce a byte-exact EIP-712
// signature that the contract will accept.
//
// EIP-712 procedure:
//   1. Hash the domain:    domain_separator = keccak256(DOMAIN_TYPEHASH || domain fields)
//   2. Hash the struct:    struct_hash      = keccak256(ORDER_TYPEHASH  || order fields)
//   3. Final digest:       digest           = keccak256(0x1901 || domain_separator || struct_hash)
//   4. Sign:               (r, s, v)        = ECDSA_secp256k1(digest, private_key)
//   5. Encode signature:   sig              = r || s || v   (65 bytes)
//
// All hashing is keccak256 (NOT sha3-256 — these differ in padding constant).
// Pure-Rust deps: k256 (ECDSA), tiny-keccak (keccak), no gcc required.

use anyhow::{bail, Context, Result};
use k256::ecdsa::{RecoveryId, Signature, SigningKey, VerifyingKey};
use tiny_keccak::{Hasher, Keccak};

// ── Polymarket production constants ───────────────────────────────────────────

/// CTF Exchange contract on Polygon mainnet (Polymarket production).
/// If Polymarket migrates, override this in `Domain` rather than editing here.
pub const POLYMARKET_CTF_EXCHANGE_POLYGON: &str = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E";
pub const POLYGON_CHAIN_ID:                u64  = 137;
pub const POLYMARKET_DOMAIN_NAME:          &str = "Polymarket CTF Exchange";
pub const POLYMARKET_DOMAIN_VERSION:       &str = "1";

// ── EIP-712 type strings ─────────────────────────────────────────────────────
// These exact strings are hashed with keccak256 to produce the typehashes.

const DOMAIN_TYPE: &str =
    "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)";

const ORDER_TYPE: &str = "Order(uint256 salt,address maker,address signer,address taker,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint256 expiration,uint256 nonce,uint256 feeRateBps,uint8 side,uint8 signatureType)";

// ── Public types ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Domain {
    pub name:               String,
    pub version:            String,
    pub chain_id:           u64,
    pub verifying_contract: [u8; 20],
}

impl Domain {
    /// Standard production domain for Polymarket CTF Exchange on Polygon.
    pub fn polymarket_polygon() -> Self {
        Self {
            name:               POLYMARKET_DOMAIN_NAME.to_string(),
            version:            POLYMARKET_DOMAIN_VERSION.to_string(),
            chain_id:           POLYGON_CHAIN_ID,
            verifying_contract: hex_to_address(POLYMARKET_CTF_EXCHANGE_POLYGON)
                .expect("hardcoded address must parse"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    Buy  = 0,
    Sell = 1,
}

impl Side {
    /// Wire format for the CLOB REST API.
    pub fn as_str(&self) -> &'static str {
        match self {
            Side::Buy  => "BUY",
            Side::Sell => "SELL",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SignatureType {
    /// Standard externally-owned account
    Eoa         = 0,
    /// Polymarket Magic-link smart wallet
    PolyProxy   = 1,
    /// Polymarket Gnosis Safe
    PolyGnosis  = 2,
}

impl SignatureType {
    /// Parse from the canonical name (matches Polymarket's docs):
    /// "EOA" / "POLY_PROXY" / "POLY_GNOSIS_SAFE", case-insensitive.
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        match s.to_ascii_uppercase().as_str() {
            "EOA" | "0"               => Ok(Self::Eoa),
            "POLY_PROXY" | "PROXY" | "1" => Ok(Self::PolyProxy),
            "POLY_GNOSIS_SAFE" | "GNOSIS" | "2" => Ok(Self::PolyGnosis),
            other => anyhow::bail!(
                "unknown signature type: {other:?} (expected EOA / POLY_PROXY / POLY_GNOSIS_SAFE)"
            ),
        }
    }
}

/// Order fields exactly matching the on-chain struct schema.
/// All `[u8; 32]` fields are big-endian 256-bit integers.
#[derive(Debug, Clone)]
pub struct Order {
    pub salt:           [u8; 32],
    pub maker:          [u8; 20],   // funded wallet (or proxy)
    pub signer:         [u8; 20],   // EOA actually signing
    pub taker:          [u8; 20],   // 0x0 = anyone can take
    pub token_id:       [u8; 32],
    pub maker_amount:   [u8; 32],   // amount maker is offering
    pub taker_amount:   [u8; 32],   // amount maker wants
    pub expiration:     [u8; 32],   // unix timestamp seconds
    pub nonce:          [u8; 32],
    pub fee_rate_bps:   [u8; 32],
    pub side:           Side,
    pub signature_type: SignatureType,
}

#[derive(Debug, Clone)]
pub struct SignedOrder {
    pub order:     Order,
    /// 65-byte signature: r (32) || s (32) || v (1).
    /// `v` is ETH-style (27 or 28).
    pub signature: [u8; 65],
}

// ── Hashing helpers ──────────────────────────────────────────────────────────

fn keccak256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak::v256();
    let mut out = [0u8; 32];
    hasher.update(data);
    hasher.finalize(&mut out);
    out
}

/// Pad a 20-byte address to a 32-byte EIP-712 word (left-pad with zeros).
fn pad_addr(addr: &[u8; 20]) -> [u8; 32] {
    let mut buf = [0u8; 32];
    buf[12..].copy_from_slice(addr);
    buf
}

/// Pad a u8 to a 32-byte EIP-712 word.
fn pad_u8(v: u8) -> [u8; 32] {
    let mut buf = [0u8; 32];
    buf[31] = v;
    buf
}

/// Pad a u64 to a 32-byte EIP-712 word (big-endian).
fn pad_u64(v: u64) -> [u8; 32] {
    let mut buf = [0u8; 32];
    buf[24..].copy_from_slice(&v.to_be_bytes());
    buf
}

// ── Domain separator ─────────────────────────────────────────────────────────

pub fn domain_separator(domain: &Domain) -> [u8; 32] {
    let domain_typehash = keccak256(DOMAIN_TYPE.as_bytes());
    let name_hash       = keccak256(domain.name.as_bytes());
    let version_hash    = keccak256(domain.version.as_bytes());

    let mut buf = Vec::with_capacity(32 * 5);
    buf.extend_from_slice(&domain_typehash);
    buf.extend_from_slice(&name_hash);
    buf.extend_from_slice(&version_hash);
    buf.extend_from_slice(&pad_u64(domain.chain_id));
    buf.extend_from_slice(&pad_addr(&domain.verifying_contract));
    keccak256(&buf)
}

// ── Order struct hash ────────────────────────────────────────────────────────

pub fn order_struct_hash(order: &Order) -> [u8; 32] {
    let order_typehash = keccak256(ORDER_TYPE.as_bytes());

    let mut buf = Vec::with_capacity(32 * 13);
    buf.extend_from_slice(&order_typehash);
    buf.extend_from_slice(&order.salt);
    buf.extend_from_slice(&pad_addr(&order.maker));
    buf.extend_from_slice(&pad_addr(&order.signer));
    buf.extend_from_slice(&pad_addr(&order.taker));
    buf.extend_from_slice(&order.token_id);
    buf.extend_from_slice(&order.maker_amount);
    buf.extend_from_slice(&order.taker_amount);
    buf.extend_from_slice(&order.expiration);
    buf.extend_from_slice(&order.nonce);
    buf.extend_from_slice(&order.fee_rate_bps);
    buf.extend_from_slice(&pad_u8(order.side as u8));
    buf.extend_from_slice(&pad_u8(order.signature_type as u8));
    keccak256(&buf)
}

// ── Final digest ─────────────────────────────────────────────────────────────

pub fn eip712_digest(domain: &Domain, order: &Order) -> [u8; 32] {
    let ds = domain_separator(domain);
    let sh = order_struct_hash(order);
    let mut buf = Vec::with_capacity(2 + 32 + 32);
    buf.push(0x19);
    buf.push(0x01);
    buf.extend_from_slice(&ds);
    buf.extend_from_slice(&sh);
    keccak256(&buf)
}

// ── Signing ──────────────────────────────────────────────────────────────────

/// Sign the EIP-712 digest of an order with the given wallet private key.
///
/// `private_key_hex` is expected as 0x-prefixed 64-hex (32 bytes).
pub fn sign_order(order: Order, domain: &Domain, private_key_hex: &str) -> Result<SignedOrder> {
    let pk_bytes = parse_private_key(private_key_hex)?;
    let signing_key = SigningKey::from_bytes((&pk_bytes).into())
        .context("invalid secp256k1 private key")?;

    let digest = eip712_digest(domain, &order);
    let (signature, recovery_id): (Signature, RecoveryId) = signing_key
        .sign_prehash_recoverable(&digest)
        .context("ECDSA signing failed")?;

    let sig_bytes = signature.to_bytes();    // r || s, 64 bytes
    let mut sig65 = [0u8; 65];
    sig65[..64].copy_from_slice(&sig_bytes);
    // ETH convention: v = 27 + recovery_id (which is 0 or 1)
    sig65[64] = 27 + recovery_id.to_byte();

    Ok(SignedOrder { order, signature: sig65 })
}

/// Recover the signing wallet's address from a signed order.
/// Useful for verifying that a signed order was signed by the expected key.
pub fn recover_signer(domain: &Domain, signed: &SignedOrder) -> Result<[u8; 20]> {
    let digest = eip712_digest(domain, &signed.order);
    let r_s: [u8; 64] = signed.signature[..64].try_into().expect("len 64");
    let signature = Signature::from_bytes((&r_s).into())
        .context("malformed signature")?;
    let v = signed.signature[64];
    if v != 27 && v != 28 {
        bail!("unexpected v value: {}", v);
    }
    let recovery_id = RecoveryId::try_from(v - 27)
        .context("invalid recovery id")?;
    let verifying_key = VerifyingKey::recover_from_prehash(&digest, &signature, recovery_id)
        .context("ecrecover failed")?;
    Ok(verifying_key_to_address(&verifying_key))
}

/// Compute the Ethereum address from a verifying key.
pub fn verifying_key_to_address(vk: &VerifyingKey) -> [u8; 20] {
    let pk_bytes = vk.to_encoded_point(false);
    let pk_uncompressed = &pk_bytes.as_bytes()[1..];   // strip 0x04 prefix
    let hash = keccak256(pk_uncompressed);
    let mut addr = [0u8; 20];
    addr.copy_from_slice(&hash[12..]);
    addr
}

/// Derive the Ethereum address from a private key (for setting `signer`).
pub fn private_key_to_address(private_key_hex: &str) -> Result<[u8; 20]> {
    let pk_bytes = parse_private_key(private_key_hex)?;
    let signing_key = SigningKey::from_bytes((&pk_bytes).into())
        .context("invalid secp256k1 private key")?;
    Ok(verifying_key_to_address(signing_key.verifying_key()))
}

// ── Hex helpers ──────────────────────────────────────────────────────────────

pub fn hex_to_address(s: &str) -> Result<[u8; 20]> {
    let stripped = s.strip_prefix("0x").unwrap_or(s);
    let bytes = hex::decode(stripped).context("address hex decode failed")?;
    if bytes.len() != 20 {
        bail!("expected 20-byte address, got {} bytes", bytes.len());
    }
    let mut out = [0u8; 20];
    out.copy_from_slice(&bytes);
    Ok(out)
}

pub fn address_to_hex(addr: &[u8; 20]) -> String {
    format!("0x{}", hex::encode(addr))
}

pub fn parse_private_key(s: &str) -> Result<[u8; 32]> {
    let stripped = s.strip_prefix("0x").unwrap_or(s);
    let bytes = hex::decode(stripped).context("private key hex decode failed")?;
    if bytes.len() != 32 {
        bail!("expected 32-byte private key, got {} bytes", bytes.len());
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

/// Big-endian 256-bit encoding of a u64 (right-aligned in the 32-byte word).
pub fn u256_from_u64(v: u64) -> [u8; 32] {
    let mut buf = [0u8; 32];
    buf[24..].copy_from_slice(&v.to_be_bytes());
    buf
}

/// Big-endian 256-bit encoding of a u128 (right-aligned in the 32-byte word).
pub fn u256_from_u128(v: u128) -> [u8; 32] {
    let mut buf = [0u8; 32];
    buf[16..].copy_from_slice(&v.to_be_bytes());
    buf
}

/// Convert a 32-byte big-endian uint256 to its decimal string (no leading zeros).
/// The CLOB REST API expects amounts and tokenIds as decimal strings.
pub fn u256_to_dec(b: &[u8; 32]) -> String {
    if b.iter().all(|&x| x == 0) {
        return "0".to_string();
    }
    let mut v = *b;
    let mut digits = Vec::new();
    while !v.iter().all(|&x| x == 0) {
        let mut rem: u32 = 0;
        for byte in v.iter_mut() {
            let cur = rem * 256 + *byte as u32;
            *byte = (cur / 10) as u8;
            rem = cur % 10;
        }
        digits.push(b'0' + rem as u8);
    }
    digits.reverse();
    String::from_utf8(digits).expect("digits are ascii")
}

pub fn u256_from_dec(s: &str) -> Result<[u8; 32]> {
    // Decimal string → 32-byte big-endian. We use a simple loop because we
    // don't need a full bignum for the values we encounter in orders.
    let mut acc = [0u8; 32];
    for ch in s.bytes() {
        if !ch.is_ascii_digit() {
            bail!("non-digit in u256 input: {}", s);
        }
        let d = (ch - b'0') as u16;
        // Multiply acc by 10
        let mut carry: u16 = 0;
        for byte in acc.iter_mut().rev() {
            let v = (*byte as u16) * 10 + carry;
            *byte = (v & 0xff) as u8;
            carry = v >> 8;
        }
        if carry != 0 {
            bail!("u256 overflow in {}", s);
        }
        // Add d
        let mut carry: u16 = d;
        for byte in acc.iter_mut().rev() {
            let v = (*byte as u16) + carry;
            *byte = (v & 0xff) as u8;
            carry = v >> 8;
            if carry == 0 { break; }
        }
        if carry != 0 {
            bail!("u256 overflow in {}", s);
        }
    }
    Ok(acc)
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // Known test vector: Vitalik's well-publicised dev wallet (NEVER USE FOR REAL FUNDS)
    // Private key:   0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
    // Address:       0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
    // This is the canonical first account from Hardhat / Anvil, widely used in tests.
    const TEST_PK_1: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
    const TEST_ADDR_1: &str = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266";

    // Second Hardhat account
    const TEST_PK_2: &str = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";
    const TEST_ADDR_2: &str = "0x70997970c51812dc3a010c7d01b50e0d17dc79c8";

    fn sample_order() -> Order {
        Order {
            salt:           [1u8; 32],
            maker:          hex_to_address(TEST_ADDR_1).unwrap(),
            signer:         hex_to_address(TEST_ADDR_1).unwrap(),
            taker:          [0u8; 20],
            token_id:       [42u8; 32],
            maker_amount:   u256_from_dec("10000000").unwrap(),
            taker_amount:   u256_from_dec("20000000").unwrap(),
            expiration:     u256_from_dec("1700000000").unwrap(),
            nonce:          u256_from_dec("0").unwrap(),
            fee_rate_bps:   u256_from_dec("0").unwrap(),
            side:           Side::Buy,
            signature_type: SignatureType::Eoa,
        }
    }

    // ── Hex helpers ───────────────────────────────────────────────────────

    #[test]
    fn hex_to_address_round_trip() {
        let a = hex_to_address("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266").unwrap();
        assert_eq!(address_to_hex(&a), TEST_ADDR_1);
    }

    #[test]
    fn hex_to_address_rejects_wrong_length() {
        assert!(hex_to_address("0x1234").is_err());
        let too_long = "0x".to_string() + &"00".repeat(21);
        assert!(hex_to_address(&too_long).is_err());
    }

    #[test]
    fn parse_private_key_strips_prefix() {
        let pk1 = parse_private_key(TEST_PK_1).unwrap();
        let pk2 = parse_private_key(&TEST_PK_1.replacen("0x", "", 1)).unwrap();
        assert_eq!(pk1, pk2);
    }

    #[test]
    fn private_key_derives_correct_address() {
        let addr = private_key_to_address(TEST_PK_1).unwrap();
        assert_eq!(address_to_hex(&addr), TEST_ADDR_1);
        let addr2 = private_key_to_address(TEST_PK_2).unwrap();
        assert_eq!(address_to_hex(&addr2), TEST_ADDR_2);
    }

    // ── u256 encoding ─────────────────────────────────────────────────────

    #[test]
    fn u256_zero() {
        assert_eq!(u256_from_dec("0").unwrap(), [0u8; 32]);
    }

    #[test]
    fn u256_one() {
        let mut expected = [0u8; 32];
        expected[31] = 1;
        assert_eq!(u256_from_dec("1").unwrap(), expected);
    }

    #[test]
    fn u256_arbitrary_value() {
        // 1234567 = 0x12d687
        let v = u256_from_dec("1234567").unwrap();
        assert_eq!(&v[29..], &[0x12, 0xd6, 0x87]);
    }

    #[test]
    fn u256_rejects_non_digit() {
        assert!(u256_from_dec("123a").is_err());
    }

    #[test]
    fn u256_from_u64_round_trips_via_dec() {
        for v in [0u64, 1, 42, 1_000_000, u64::MAX] {
            let encoded = u256_from_u64(v);
            assert_eq!(u256_to_dec(&encoded), v.to_string());
        }
    }

    #[test]
    fn u256_from_u128_handles_large_values() {
        let v: u128 = 123_456_789_012_345_678_901_234_567_890;
        let encoded = u256_from_u128(v);
        assert_eq!(u256_to_dec(&encoded), v.to_string());
    }

    #[test]
    fn u256_to_dec_round_trips_with_from_dec() {
        for s in ["0", "1", "255", "1000000", "1234567890",
                  "115792089237316195423570985008687907853269984665640564039457584007913129639935"] {
            let bytes = u256_from_dec(s).unwrap();
            assert_eq!(u256_to_dec(&bytes), s);
        }
    }

    #[test]
    fn u256_to_dec_zero_is_zero() {
        assert_eq!(u256_to_dec(&[0u8; 32]), "0");
    }

    #[test]
    fn side_as_str_matches_clob_wire_format() {
        assert_eq!(Side::Buy.as_str(),  "BUY");
        assert_eq!(Side::Sell.as_str(), "SELL");
    }

    #[test]
    fn signature_type_parse_accepts_canonical_and_aliases() {
        assert_eq!(SignatureType::parse("EOA").unwrap(),               SignatureType::Eoa);
        assert_eq!(SignatureType::parse("eoa").unwrap(),               SignatureType::Eoa);
        assert_eq!(SignatureType::parse("0").unwrap(),                 SignatureType::Eoa);
        assert_eq!(SignatureType::parse("POLY_PROXY").unwrap(),        SignatureType::PolyProxy);
        assert_eq!(SignatureType::parse("proxy").unwrap(),             SignatureType::PolyProxy);
        assert_eq!(SignatureType::parse("1").unwrap(),                 SignatureType::PolyProxy);
        assert_eq!(SignatureType::parse("POLY_GNOSIS_SAFE").unwrap(),  SignatureType::PolyGnosis);
        assert_eq!(SignatureType::parse("gnosis").unwrap(),            SignatureType::PolyGnosis);
        assert_eq!(SignatureType::parse("2").unwrap(),                 SignatureType::PolyGnosis);
        assert!(SignatureType::parse("XYZ").is_err());
    }

    // ── Keccak256 ─────────────────────────────────────────────────────────

    #[test]
    fn keccak_empty_matches_known_value() {
        // keccak256("") = c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470
        let h = keccak256(b"");
        assert_eq!(
            hex::encode(h),
            "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
        );
    }

    #[test]
    fn keccak_abc_matches_known_value() {
        // keccak256("abc") = 4e03657aea45a94fc7d47ba826c8d667c0d1e6e33a64a036ec44f58fa12d6c45
        let h = keccak256(b"abc");
        assert_eq!(
            hex::encode(h),
            "4e03657aea45a94fc7d47ba826c8d667c0d1e6e33a64a036ec44f58fa12d6c45"
        );
    }

    #[test]
    fn domain_typehash_matches_eip712_spec() {
        // The exact string and hash from EIP-712 spec.
        let h = keccak256(DOMAIN_TYPE.as_bytes());
        assert_eq!(
            hex::encode(h),
            "8b73c3c69bb8fe3d512ecc4cf759cc79239f7b179b0ffacaa9a75d522b39400f"
        );
    }

    // ── EIP-712 domain separator ──────────────────────────────────────────

    #[test]
    fn domain_separator_deterministic() {
        let domain = Domain::polymarket_polygon();
        let ds1 = domain_separator(&domain);
        let ds2 = domain_separator(&domain);
        assert_eq!(ds1, ds2);
    }

    #[test]
    fn domain_separator_differs_between_chain_ids() {
        let polygon = Domain::polymarket_polygon();
        let mut other = polygon.clone();
        other.chain_id = 1; // Ethereum mainnet
        assert_ne!(domain_separator(&polygon), domain_separator(&other));
    }

    #[test]
    fn domain_separator_differs_between_contracts() {
        let mut a = Domain::polymarket_polygon();
        let mut b = a.clone();
        b.verifying_contract = [0u8; 20];
        assert_ne!(domain_separator(&a), domain_separator(&b));
        // Mutating `a` shouldn't affect `b`'s prior result
        a.name = "Different".to_string();
        assert_ne!(domain_separator(&a), domain_separator(&b));
    }

    // ── Order struct hash ─────────────────────────────────────────────────

    #[test]
    fn order_hash_changes_with_each_field() {
        let base = sample_order();
        let h0 = order_struct_hash(&base);

        // Change salt
        let mut o = base.clone(); o.salt[0] ^= 0xff;
        assert_ne!(order_struct_hash(&o), h0);

        // Change side
        let mut o = base.clone(); o.side = Side::Sell;
        assert_ne!(order_struct_hash(&o), h0);

        // Change maker
        let mut o = base.clone(); o.maker[0] ^= 0xff;
        assert_ne!(order_struct_hash(&o), h0);

        // Change token_id
        let mut o = base.clone(); o.token_id[0] ^= 0xff;
        assert_ne!(order_struct_hash(&o), h0);
    }

    // ── Signing round-trip ────────────────────────────────────────────────

    #[test]
    fn signed_order_recovers_to_signer_address() {
        let domain = Domain::polymarket_polygon();
        let order  = sample_order();
        let signed = sign_order(order, &domain, TEST_PK_1).unwrap();
        let recovered = recover_signer(&domain, &signed).unwrap();
        let expected  = hex_to_address(TEST_ADDR_1).unwrap();
        assert_eq!(recovered, expected);
    }

    #[test]
    fn different_keys_produce_different_signatures() {
        let domain = Domain::polymarket_polygon();
        let order  = sample_order();
        let s1 = sign_order(order.clone(), &domain, TEST_PK_1).unwrap();
        let s2 = sign_order(order,         &domain, TEST_PK_2).unwrap();
        assert_ne!(s1.signature, s2.signature);
        // And recovers to different addresses
        let a1 = recover_signer(&domain, &s1).unwrap();
        let a2 = recover_signer(&domain, &s2).unwrap();
        assert_ne!(a1, a2);
    }

    #[test]
    fn tampering_with_signature_breaks_recovery() {
        let domain = Domain::polymarket_polygon();
        let order  = sample_order();
        let mut signed = sign_order(order, &domain, TEST_PK_1).unwrap();
        // Flip a byte in the signature
        signed.signature[0] ^= 0xff;
        let expected = hex_to_address(TEST_ADDR_1).unwrap();
        match recover_signer(&domain, &signed) {
            Err(_) => {} // ecrecover legitimately rejects malformed sig — fine
            Ok(addr) => assert_ne!(addr, expected, "tampered sig should not recover original signer"),
        }
    }

    #[test]
    fn tampering_with_order_breaks_recovery() {
        let domain = Domain::polymarket_polygon();
        let order  = sample_order();
        let mut signed = sign_order(order, &domain, TEST_PK_1).unwrap();
        // Flip a byte in the order
        signed.order.salt[0] ^= 0xff;
        let recovered = recover_signer(&domain, &signed).unwrap();
        let expected  = hex_to_address(TEST_ADDR_1).unwrap();
        assert_ne!(recovered, expected);
    }

    #[test]
    fn signing_is_deterministic_for_same_inputs() {
        // ECDSA with deterministic-k (RFC 6979) gives same sig for same inputs.
        // k256 implements RFC 6979 by default.
        let domain = Domain::polymarket_polygon();
        let order  = sample_order();
        let s1 = sign_order(order.clone(), &domain, TEST_PK_1).unwrap();
        let s2 = sign_order(order,         &domain, TEST_PK_1).unwrap();
        assert_eq!(s1.signature, s2.signature);
    }

    // ── sign_order rejects bad input ──────────────────────────────────────

    #[test]
    fn sign_rejects_bad_private_key() {
        let domain = Domain::polymarket_polygon();
        assert!(sign_order(sample_order(), &domain, "0xnotvalid").is_err());
        assert!(sign_order(sample_order(), &domain, "0x").is_err());
    }

    // ── Property: signature length and v range ────────────────────────────

    #[test]
    fn signature_is_65_bytes_with_valid_v() {
        let domain = Domain::polymarket_polygon();
        let order  = sample_order();
        let signed = sign_order(order, &domain, TEST_PK_1).unwrap();
        assert_eq!(signed.signature.len(), 65);
        assert!(signed.signature[64] == 27 || signed.signature[64] == 28);
    }

    // ── EIP-712 prefix sanity ─────────────────────────────────────────────

    #[test]
    fn digest_prefix_is_1901() {
        // Just confirm the digest computation includes the 0x1901 prefix
        // by checking it differs from a hash without the prefix.
        let domain = Domain::polymarket_polygon();
        let order  = sample_order();
        let with_prefix    = eip712_digest(&domain, &order);
        let ds = domain_separator(&domain);
        let sh = order_struct_hash(&order);
        let no_prefix = keccak256(&[ds, sh].concat());
        assert_ne!(with_prefix, no_prefix);
    }
}
