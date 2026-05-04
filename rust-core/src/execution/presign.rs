// Sprint 2 / J3 — Pre-signed order pool.
//
// At the moment a 5-minute BTC window opens we know the current Polymarket
// midpoint. We pre-sign BUY orders at every 1-cent tick around the midpoint
// (default ±15 cents = up to 31 orders) and stash them in a DashMap keyed by
// (asset_id, side, price_cents). When a signal fires later in the window the
// hot path just looks up the matching order and POSTs it — no signing on the
// trade-decision path.
//
// Why pre-sign? ECDSA signing is ~500 µs but more importantly we want the
// signal→submit latency budget reserved for HTTP only. Pre-signing 31 orders
// up-front is a fixed ~15 ms cost paid at window open.

use anyhow::{anyhow, bail, Context, Result};
use dashmap::DashMap;
use rayon::prelude::*;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

use crate::execution::orders::{
    sign_order, u256_from_u128, u256_from_u64, Domain, Order, Side, SignatureType, SignedOrder,
};

// Polymarket / Polygon: USDC and CTF tokens both use 6 decimals.
// LOT_SIZE_SCALE matches rs-clob-client — share count truncates to 0.01 precision.
const USDC_DECIMALS:    u32 = 6;
const LOT_SIZE_SCALE:   u32 = 2;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct PoolKey {
    pub asset_id:    [u8; 32],
    pub side:        Side,
    pub price_cents: u8,    // 1..=99
}

/// Inputs that stay constant across all orders pre-signed for a single window.
#[derive(Debug, Clone)]
pub struct PopulateParams {
    pub asset_id:        [u8; 32],
    pub maker:           [u8; 20],
    pub signer:          [u8; 20],
    pub taker:           [u8; 20],   // 0x0 = anyone
    pub bet_dollars:     f64,        // USDC amount per order (e.g. 5.00)
    pub expiration_unix: u64,        // 0 = no expiry
    pub nonce:           u64,        // wallet nonce (Polymarket cancellation key)
    pub fee_rate_bps:    u64,
    pub signature_type:  SignatureType,
}

#[derive(Debug, Default)]
pub struct OrderPool {
    inner: DashMap<PoolKey, SignedOrder>,
}

impl OrderPool {
    pub fn new() -> Self {
        Self { inner: DashMap::new() }
    }

    /// Pre-sign BUY orders at every 1-cent tick from `min_cents..=max_cents`.
    /// Returns the count of orders signed.
    ///
    /// **Parallel:** uses rayon to fan out ECDSA signing across the global
    /// thread pool. On a 20-core box, signing 31 orders drops from ~15ms
    /// sequential to ~2ms.
    pub fn populate_buy_range(
        &self,
        domain:          &Domain,
        params:          &PopulateParams,
        private_key_hex: &str,
        min_cents:       u8,
        max_cents:       u8,
    ) -> Result<usize> {
        if min_cents == 0 || min_cents > 99 || max_cents == 0 || max_cents > 99 {
            bail!("price ticks must be in 1..=99, got {}..={}", min_cents, max_cents);
        }
        if min_cents > max_cents {
            bail!("min_cents {} > max_cents {}", min_cents, max_cents);
        }
        if !params.bet_dollars.is_finite() || params.bet_dollars <= 0.0 {
            bail!("invalid bet_dollars: {}", params.bet_dollars);
        }

        // Parallel ECDSA signing — each tick is independent.
        // Fail-fast: collect into Result<Vec<_>>, return first Err.
        let signed: Vec<(PoolKey, SignedOrder)> = (min_cents..=max_cents)
            .into_par_iter()
            .map(|c| -> Result<(PoolKey, SignedOrder)> {
                let order  = build_buy_order(params, c)?;
                let signed = sign_order(order, domain, private_key_hex)
                    .with_context(|| format!("sign_order failed at {}c", c))?;
                let key = PoolKey { asset_id: params.asset_id, side: Side::Buy, price_cents: c };
                Ok((key, signed))
            })
            .collect::<Result<_>>()?;

        let count = signed.len();
        for (k, v) in signed {
            self.inner.insert(k, v);
        }
        Ok(count)
    }

    /// Pre-sign BUY orders at midpoint ± `depth` cents, clamped to 1..=99.
    /// Returns the count actually signed (clamping may reduce the range).
    pub fn populate_around_midpoint(
        &self,
        domain:          &Domain,
        params:          &PopulateParams,
        private_key_hex: &str,
        midpoint_cents:  u8,
        depth:           u8,
    ) -> Result<usize> {
        if depth == 0 {
            bail!("depth must be > 0");
        }
        let lo = midpoint_cents.saturating_sub(depth).max(1);
        let hi = midpoint_cents.saturating_add(depth).min(99);
        if lo > hi {
            bail!("midpoint {} ± {} is outside 1..=99", midpoint_cents, depth);
        }
        self.populate_buy_range(domain, params, private_key_hex, lo, hi)
    }

    /// Atomically remove and return the order at this key.
    pub fn take(&self, key: &PoolKey) -> Option<SignedOrder> {
        self.inner.remove(key).map(|(_, v)| v)
    }

    /// Read-only peek (does not remove).
    pub fn get(&self, key: &PoolKey) -> Option<SignedOrder> {
        self.inner.get(key).map(|r| r.value().clone())
    }

    pub fn contains(&self, key: &PoolKey) -> bool {
        self.inner.contains_key(key)
    }

    pub fn len(&self) -> usize { self.inner.len() }
    pub fn is_empty(&self) -> bool { self.inner.is_empty() }
    pub fn clear(&self) { self.inner.clear(); }
}

/// Generate a random u64 salt and place it in the low 8 bytes of a 32-byte
/// big-endian uint256 word.
///
/// rs-clob-client (Polymarket's official Rust client) generates a u64 salt
/// and serializes it as a JSON **number** in the wire format. We match that
/// here so our wire format is byte-equivalent. The EIP-712 hash still folds
/// in all 32 bytes (the high 24 are zero, which is fine — keccak doesn't
/// care).
///
/// Source of randomness: `uuid::Uuid::new_v4` uses `getrandom` (OS CSPRNG).
/// We pull 8 bytes off the end of a UUID-v4 to avoid adding a `rand` dep.
fn random_salt() -> [u8; 32] {
    let v = uuid::Uuid::new_v4();
    let bytes = v.as_bytes();
    let mut u64_bytes = [0u8; 8];
    u64_bytes.copy_from_slice(&bytes[8..16]);
    let salt_u64 = u64::from_be_bytes(u64_bytes);
    u256_from_u64(salt_u64)
}

/// Inputs for building one BUY order. Standalone struct so callers don't
/// need a full PopulateParams to sign a single order at an arbitrary
/// bet size — used by the executor's Kelly-aware sign-on-demand path.
#[derive(Debug, Clone)]
pub struct BuyOrderInputs {
    pub asset_id:        [u8; 32],
    pub maker:           [u8; 20],
    pub signer:          [u8; 20],
    pub taker:           [u8; 20],
    /// Per-order USDC. The amount kelly recommends for THIS specific
    /// trade — not the bot's max_bet.
    pub bet_dollars:     f64,
    pub price_cents:     u8,
    pub expiration_unix: u64,
    pub nonce:           u64,
    pub fee_rate_bps:    u64,
    pub signature_type:  SignatureType,
}

/// Build + sign one BUY order at exactly `bet_dollars` for the given tick.
/// This is the Kelly-aware path: caller decides the size, we sign at that
/// size. Sign latency in release mode is ~150µs.
pub fn sign_buy_now(
    inputs:          &BuyOrderInputs,
    domain:          &Domain,
    private_key_hex: &str,
) -> Result<SignedOrder> {
    let order = build_buy_order_inner(inputs)?;
    sign_order(order, domain, private_key_hex)
        .with_context(|| format!("sign_buy_now at {}c", inputs.price_cents))
}

/// Internal: shared between build_buy_order (pool path) and sign_buy_now
/// (Kelly path). Encapsulates the rust_decimal tick-aligned math.
///
/// Mirrors rs-clob-client::OrderBuilder:
///   size_shares    = trunc(bet_dollars / price, scale=LOT_SIZE_SCALE=2)
///   maker_amount   = trunc(size_shares * price, scale=USDC_DECIMALS+LOT_SIZE_SCALE=8)
///   maker_micros   = floor(maker_amount * 10^USDC_DECIMALS)
///   taker_micros   = floor(size_shares * 10^USDC_DECIMALS)
fn build_buy_order_inner(inputs: &BuyOrderInputs) -> Result<Order> {
    if !(1..=99).contains(&inputs.price_cents) {
        bail!("price_cents must be in 1..=99, got {}", inputs.price_cents);
    }
    if !inputs.bet_dollars.is_finite() || inputs.bet_dollars <= 0.0 {
        bail!("invalid bet_dollars: {}", inputs.bet_dollars);
    }

    let bet_cents = (inputs.bet_dollars * 100.0).round() as i64;
    if bet_cents <= 0 {
        bail!("bet too small after rounding to cents: {}", inputs.bet_dollars);
    }
    let bet_usd = Decimal::new(bet_cents, 2);
    let price   = Decimal::new(inputs.price_cents as i64, 2);

    let size = (bet_usd / price).trunc_with_scale(LOT_SIZE_SCALE);
    if size.is_zero() {
        bail!(
            "size truncates to zero: bet ${} at {}c too small",
            inputs.bet_dollars, inputs.price_cents,
        );
    }
    let maker_amount = (size * price).trunc_with_scale(USDC_DECIMALS + LOT_SIZE_SCALE);

    let maker_micros = decimal_to_micros(maker_amount)?;
    let taker_micros = decimal_to_micros(size)?;
    if maker_micros == 0 {
        bail!("maker_micros is zero after rounding: bet ${}", inputs.bet_dollars);
    }

    Ok(Order {
        salt:           random_salt(),
        maker:          inputs.maker,
        signer:         inputs.signer,
        taker:          inputs.taker,
        token_id:       inputs.asset_id,
        maker_amount:   u256_from_u128(maker_micros),
        taker_amount:   u256_from_u128(taker_micros),
        expiration:     u256_from_u64(inputs.expiration_unix),
        nonce:          u256_from_u64(inputs.nonce),
        fee_rate_bps:   u256_from_u64(inputs.fee_rate_bps),
        side:           Side::Buy,
        signature_type: inputs.signature_type,
    })
}

/// Build a BUY order at a given 1¢ price tick, sized to `bet_dollars`.
///
/// Wraps `build_buy_order_inner` for the pool path which works in
/// PopulateParams (fixed bet across the ladder).
fn build_buy_order(params: &PopulateParams, price_cents: u8) -> Result<Order> {
    build_buy_order_inner(&BuyOrderInputs {
        asset_id:        params.asset_id,
        maker:           params.maker,
        signer:          params.signer,
        taker:           params.taker,
        bet_dollars:     params.bet_dollars,
        price_cents,
        expiration_unix: params.expiration_unix,
        nonce:           params.nonce,
        fee_rate_bps:    params.fee_rate_bps,
        signature_type:  params.signature_type,
    })
}

/// Convert a non-negative Decimal to fixed-point u128 micros (10^USDC_DECIMALS).
fn decimal_to_micros(d: Decimal) -> Result<u128> {
    if d.is_sign_negative() {
        bail!("decimal cannot be negative for USDC micros: {}", d);
    }
    let scale_factor = Decimal::from(1_000_000_u64);
    let scaled       = d * scale_factor;
    let truncated    = scaled.trunc();
    truncated.to_u128()
        .ok_or_else(|| anyhow!("decimal does not fit in u128: {}", d))
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::orders::{hex_to_address, recover_signer, u256_to_dec};

    // Hardhat account #0 — public test key, NEVER use for real funds.
    const TEST_PK:   &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
    const TEST_ADDR: &str = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266";

    fn sample_params() -> PopulateParams {
        let addr = hex_to_address(TEST_ADDR).unwrap();
        PopulateParams {
            asset_id:        [7u8; 32],
            maker:           addr,
            signer:          addr,
            taker:           [0u8; 20],
            bet_dollars:     5.00,
            expiration_unix: 0,
            nonce:           0,
            fee_rate_bps:    0,
            signature_type:  SignatureType::Eoa,
        }
    }

    // ── Construction ──────────────────────────────────────────────────────

    #[test]
    fn new_pool_is_empty() {
        let p = OrderPool::new();
        assert_eq!(p.len(), 0);
        assert!(p.is_empty());
    }

    // ── populate_buy_range ────────────────────────────────────────────────

    #[test]
    fn populate_buy_range_inclusive_count() {
        let pool = OrderPool::new();
        let domain = Domain::polymarket_polygon();
        let n = pool.populate_buy_range(&domain, &sample_params(), TEST_PK, 35, 65).unwrap();
        assert_eq!(n, 31);          // 35..=65 inclusive
        assert_eq!(pool.len(), 31);
    }

    #[test]
    fn populate_rejects_zero_min_cents() {
        let pool = OrderPool::new();
        let domain = Domain::polymarket_polygon();
        assert!(pool.populate_buy_range(&domain, &sample_params(), TEST_PK, 0, 10).is_err());
    }

    #[test]
    fn populate_rejects_max_above_99() {
        let pool = OrderPool::new();
        let domain = Domain::polymarket_polygon();
        assert!(pool.populate_buy_range(&domain, &sample_params(), TEST_PK, 90, 100).is_err());
    }

    #[test]
    fn populate_rejects_inverted_range() {
        let pool = OrderPool::new();
        let domain = Domain::polymarket_polygon();
        assert!(pool.populate_buy_range(&domain, &sample_params(), TEST_PK, 70, 50).is_err());
    }

    #[test]
    fn populate_rejects_invalid_bet_size() {
        let pool = OrderPool::new();
        let domain = Domain::polymarket_polygon();
        let mut p = sample_params(); p.bet_dollars = 0.0;
        assert!(pool.populate_buy_range(&domain, &p, TEST_PK, 1, 5).is_err());
        p.bet_dollars = -1.0;
        assert!(pool.populate_buy_range(&domain, &p, TEST_PK, 1, 5).is_err());
        p.bet_dollars = f64::NAN;
        assert!(pool.populate_buy_range(&domain, &p, TEST_PK, 1, 5).is_err());
    }

    // ── populate_around_midpoint ──────────────────────────────────────────

    #[test]
    fn populate_around_midpoint_centered_50_depth_15() {
        let pool = OrderPool::new();
        let domain = Domain::polymarket_polygon();
        let n = pool.populate_around_midpoint(
            &domain, &sample_params(), TEST_PK, 50, 15,
        ).unwrap();
        // 35..=65 inclusive = 31
        assert_eq!(n, 31);
    }

    #[test]
    fn populate_around_midpoint_clamps_low_edge() {
        let pool = OrderPool::new();
        let domain = Domain::polymarket_polygon();
        // midpoint 5 ± 15 → clamped to 1..=20 = 20 entries
        let n = pool.populate_around_midpoint(
            &domain, &sample_params(), TEST_PK, 5, 15,
        ).unwrap();
        assert_eq!(n, 20);
    }

    #[test]
    fn populate_around_midpoint_clamps_high_edge() {
        let pool = OrderPool::new();
        let domain = Domain::polymarket_polygon();
        // midpoint 95 ± 15 → clamped to 80..=99 = 20 entries
        let n = pool.populate_around_midpoint(
            &domain, &sample_params(), TEST_PK, 95, 15,
        ).unwrap();
        assert_eq!(n, 20);
    }

    #[test]
    fn populate_around_midpoint_rejects_zero_depth() {
        let pool = OrderPool::new();
        let domain = Domain::polymarket_polygon();
        assert!(pool.populate_around_midpoint(
            &domain, &sample_params(), TEST_PK, 50, 0,
        ).is_err());
    }

    // ── take / get / contains / clear ─────────────────────────────────────

    #[test]
    fn take_removes_and_returns_order() {
        let pool = OrderPool::new();
        let domain = Domain::polymarket_polygon();
        let params = sample_params();
        pool.populate_buy_range(&domain, &params, TEST_PK, 50, 50).unwrap();
        let key = PoolKey { asset_id: params.asset_id, side: Side::Buy, price_cents: 50 };
        assert!(pool.contains(&key));
        let signed = pool.take(&key);
        assert!(signed.is_some());
        assert!(!pool.contains(&key));
        // Second take returns None
        assert!(pool.take(&key).is_none());
    }

    #[test]
    fn get_does_not_remove() {
        let pool = OrderPool::new();
        let domain = Domain::polymarket_polygon();
        let params = sample_params();
        pool.populate_buy_range(&domain, &params, TEST_PK, 50, 50).unwrap();
        let key = PoolKey { asset_id: params.asset_id, side: Side::Buy, price_cents: 50 };
        assert!(pool.get(&key).is_some());
        assert!(pool.get(&key).is_some());
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn clear_empties_pool() {
        let pool = OrderPool::new();
        let domain = Domain::polymarket_polygon();
        pool.populate_buy_range(&domain, &sample_params(), TEST_PK, 30, 70).unwrap();
        assert_eq!(pool.len(), 41);
        pool.clear();
        assert_eq!(pool.len(), 0);
    }

    // ── Cryptographic correctness ─────────────────────────────────────────

    #[test]
    fn each_order_recovers_to_signer_address() {
        let pool = OrderPool::new();
        let domain = Domain::polymarket_polygon();
        let params = sample_params();
        pool.populate_buy_range(&domain, &params, TEST_PK, 30, 70).unwrap();
        let expected = hex_to_address(TEST_ADDR).unwrap();
        for c in 30u8..=70 {
            let key = PoolKey { asset_id: params.asset_id, side: Side::Buy, price_cents: c };
            let signed = pool.get(&key).unwrap();
            let recovered = recover_signer(&domain, &signed).unwrap();
            assert_eq!(recovered, expected, "recover failed at {}c", c);
        }
    }

    #[test]
    fn different_price_ticks_have_different_amounts() {
        let pool = OrderPool::new();
        let domain = Domain::polymarket_polygon();
        let params = sample_params();
        pool.populate_buy_range(&domain, &params, TEST_PK, 25, 75).unwrap();

        let a = pool.get(&PoolKey { asset_id: params.asset_id, side: Side::Buy, price_cents: 25 }).unwrap();
        let b = pool.get(&PoolKey { asset_id: params.asset_id, side: Side::Buy, price_cents: 75 }).unwrap();
        // taker (tokens) differs — same dollars buys more shares at 25¢ than 75¢
        assert_ne!(a.order.taker_amount, b.order.taker_amount);
        // Both maker_amounts are within a few cents of $5 (tick-aligned math
        // produces slightly less than the requested bet for non-divisible prices)
        let a_micros = u256_to_dec(&a.order.maker_amount).parse::<u128>().unwrap();
        let b_micros = u256_to_dec(&b.order.maker_amount).parse::<u128>().unwrap();
        assert!((4_900_000..=5_000_000).contains(&a_micros), "got {}", a_micros);
        assert!((4_900_000..=5_000_000).contains(&b_micros), "got {}", b_micros);
    }

    #[test]
    fn each_order_has_unique_salt() {
        let pool = OrderPool::new();
        let domain = Domain::polymarket_polygon();
        let params = sample_params();
        pool.populate_buy_range(&domain, &params, TEST_PK, 30, 70).unwrap();

        let mut salts = std::collections::HashSet::new();
        for c in 30u8..=70 {
            let key = PoolKey { asset_id: params.asset_id, side: Side::Buy, price_cents: c };
            let salt = pool.get(&key).unwrap().order.salt;
            assert!(salts.insert(salt), "duplicate salt at {}c", c);
        }
    }

    // ── Amount math ───────────────────────────────────────────────────────

    #[test]
    fn buy_order_amounts_match_dollar_per_share_math() {
        // $5 buy at 50¢ → 10 shares, $5 spent (exact tick — divisible)
        let params = sample_params();
        let order = build_buy_order(&params, 50).unwrap();
        assert_eq!(u256_to_dec(&order.maker_amount), "5000000");
        assert_eq!(u256_to_dec(&order.taker_amount), "10000000");

        // $5 at 25¢ → 20 shares, $5 spent (exact)
        let order = build_buy_order(&params, 25).unwrap();
        assert_eq!(u256_to_dec(&order.maker_amount), "5000000");
        assert_eq!(u256_to_dec(&order.taker_amount), "20000000");
    }

    #[test]
    fn buy_order_amounts_tick_aligned_for_non_divisible_price() {
        // $5 at 33¢: 5 / 0.33 = 15.151515... → trunc to 15.15 shares
        // maker_amount = 15.15 * 0.33 = 4.9995 USDC
        // maker_micros = 4999500, taker_micros = 15150000
        // Crucially: maker / taker = 4999500 / 15150000 = 0.33 EXACTLY,
        // which means the contract's price tick check passes.
        let params = sample_params();
        let order = build_buy_order(&params, 33).unwrap();
        assert_eq!(u256_to_dec(&order.maker_amount), "4999500");
        assert_eq!(u256_to_dec(&order.taker_amount), "15150000");
        // Ratio sanity: 4999500 * 100 == 15150000 * 33
        assert_eq!(4_999_500_u128 * 100, 15_150_000_u128 * 33);
    }

    #[test]
    fn buy_order_at_67c_also_tick_aligned() {
        // 5 / 0.67 = 7.4626... → 7.46 shares
        // 7.46 * 0.67 = 4.9982 USDC
        let params = sample_params();
        let order = build_buy_order(&params, 67).unwrap();
        assert_eq!(u256_to_dec(&order.maker_amount), "4998200");
        assert_eq!(u256_to_dec(&order.taker_amount), "7460000");
        assert_eq!(4_998_200_u128 * 100, 7_460_000_u128 * 67);
    }

    #[test]
    fn build_buy_rejects_invalid_price_cents() {
        let params = sample_params();
        assert!(build_buy_order(&params, 0).is_err());
        assert!(build_buy_order(&params, 100).is_err());
    }

    // ── G5: sign_buy_now (Kelly-aware path) ──────────────────────────────

    fn sample_inputs(bet: f64, price_cents: u8) -> BuyOrderInputs {
        let p = sample_params();
        BuyOrderInputs {
            asset_id:        p.asset_id,
            maker:           p.maker,
            signer:          p.signer,
            taker:           p.taker,
            bet_dollars:     bet,
            price_cents,
            expiration_unix: 0,
            nonce:           0,
            fee_rate_bps:    0,
            signature_type:  p.signature_type,
        }
    }

    #[test]
    fn sign_buy_now_returns_valid_signed_order() {
        use crate::execution::orders::recover_signer;
        let domain = Domain::polymarket_polygon();
        let signed = sign_buy_now(&sample_inputs(5.0, 50), &domain, TEST_PK).unwrap();
        // Signature recovers to the signer derived from TEST_PK
        let expected = crate::execution::orders::hex_to_address(TEST_ADDR).unwrap();
        let recovered = recover_signer(&domain, &signed).unwrap();
        assert_eq!(recovered, expected);
    }

    #[test]
    fn sign_buy_now_at_smaller_kelly_amount() {
        // Kelly says $5 at 45¢ — sign exactly that, regardless of any
        // pre-signed pool that may have $30 orders.
        use crate::execution::orders::u256_to_dec;
        let domain = Domain::polymarket_polygon();
        let signed = sign_buy_now(&sample_inputs(5.0, 45), &domain, TEST_PK).unwrap();
        // Math: 5/0.45 = 11.111... → trunc 11.11 shares
        // maker = 11.11 * 0.45 = 4.9995 USDC
        // micros: 4_999_500 maker, 11_110_000 taker
        assert_eq!(u256_to_dec(&signed.order.maker_amount), "4999500");
        assert_eq!(u256_to_dec(&signed.order.taker_amount), "11110000");
    }

    #[test]
    fn sign_buy_now_each_call_has_unique_salt() {
        // Two consecutive signs at identical inputs must produce distinct
        // orders (different salts) so Polymarket doesn't reject duplicates.
        let domain = Domain::polymarket_polygon();
        let s1 = sign_buy_now(&sample_inputs(10.0, 50), &domain, TEST_PK).unwrap();
        let s2 = sign_buy_now(&sample_inputs(10.0, 50), &domain, TEST_PK).unwrap();
        assert_ne!(s1.order.salt, s2.order.salt);
        assert_ne!(s1.signature, s2.signature);
    }

    #[test]
    fn sign_buy_now_rejects_invalid_inputs() {
        let domain = Domain::polymarket_polygon();
        // bad bet
        assert!(sign_buy_now(&sample_inputs(0.0, 50), &domain, TEST_PK).is_err());
        assert!(sign_buy_now(&sample_inputs(-5.0, 50), &domain, TEST_PK).is_err());
        assert!(sign_buy_now(&sample_inputs(f64::NAN, 50), &domain, TEST_PK).is_err());
        // bad price
        assert!(sign_buy_now(&sample_inputs(5.0, 0), &domain, TEST_PK).is_err());
        assert!(sign_buy_now(&sample_inputs(5.0, 100), &domain, TEST_PK).is_err());
    }

    #[test]
    fn sign_buy_now_amounts_match_pool_path() {
        // The Kelly path and the pool path must produce identical amounts
        // for the same (bet, price) — i.e., they share the same math.
        let inputs = sample_inputs(5.0, 33);
        let sn = build_buy_order_inner(&inputs).unwrap();
        let pp = build_buy_order(&sample_params(), 33).unwrap();   // params.bet_dollars = 5.0
        assert_eq!(sn.maker_amount, pp.maker_amount);
        assert_eq!(sn.taker_amount, pp.taker_amount);
    }

    #[test]
    fn build_buy_rejects_bet_too_small_for_tick() {
        // $0.001 bet at 99¢ → size = 0.001/0.99 → trunc(0.00..., 2) = 0
        let mut params = sample_params();
        params.bet_dollars = 0.001;
        // 0.001 * 100 = 0.1 → rounds to 0 cents → rejected at the cent check
        assert!(build_buy_order(&params, 99).is_err());
    }

    // ── Concurrent access (DashMap should handle) ─────────────────────────

    #[test]
    fn parallel_populate_beats_sequential_signing() {
        // Compare populate (rayon-parallel) vs single-thread reference.
        // Asserts a measurable speedup rather than an absolute deadline,
        // so the test is portable across slow/fast CI boxes and debug/release.
        let domain = Domain::polymarket_polygon();
        let params = sample_params();

        // Warm rayon's global pool — first call has init overhead.
        OrderPool::new().populate_buy_range(&domain, &params, TEST_PK, 1, 4).unwrap();

        // Sequential reference
        let start_seq = std::time::Instant::now();
        for c in 1u8..=99 {
            let order = build_buy_order(&params, c).unwrap();
            let _ = sign_order(order, &domain, TEST_PK).unwrap();
        }
        let seq = start_seq.elapsed();

        // Parallel via populate
        let pool = OrderPool::new();
        let start_par = std::time::Instant::now();
        pool.populate_buy_range(&domain, &params, TEST_PK, 1, 99).unwrap();
        let par = start_par.elapsed();

        assert_eq!(pool.len(), 99);
        // On any multi-core box, parallel should beat sequential by at least 1.5x
        // (we only need a rough sanity check that rayon is engaged).
        assert!(
            par.as_micros() * 3 < seq.as_micros() * 2,
            "parallel ({}us) should be < 2/3 of sequential ({}us)",
            par.as_micros(), seq.as_micros(),
        );
    }

    #[test]
    fn concurrent_take_is_atomic() {
        use std::sync::Arc;
        let pool = Arc::new(OrderPool::new());
        let domain = Domain::polymarket_polygon();
        let params = sample_params();
        pool.populate_buy_range(&domain, &params, TEST_PK, 1, 50).unwrap();
        assert_eq!(pool.len(), 50);

        let mut handles = vec![];
        for _ in 0..4 {
            let p = pool.clone();
            let asset = params.asset_id;
            handles.push(std::thread::spawn(move || {
                let mut taken = 0;
                for c in 1u8..=50 {
                    let key = PoolKey { asset_id: asset, side: Side::Buy, price_cents: c };
                    if p.take(&key).is_some() { taken += 1; }
                }
                taken
            }));
        }
        let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        // Each of the 50 keys removed exactly once across all threads.
        assert_eq!(total, 50);
        assert!(pool.is_empty());
    }
}
