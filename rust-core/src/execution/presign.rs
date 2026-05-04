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

use anyhow::{bail, Context, Result};
use dashmap::DashMap;
use uuid::Uuid;

use crate::execution::orders::{
    sign_order, u256_from_u64, Domain, Order, Side, SignatureType, SignedOrder,
};

// Polymarket / Polygon: USDC and CTF tokens both use 6 decimals.
const TOKEN_DECIMALS_FACTOR: f64 = 1_000_000.0;

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

        let mut count = 0;
        for c in min_cents..=max_cents {
            let order  = build_buy_order(params, c)?;
            let signed = sign_order(order, domain, private_key_hex)
                .with_context(|| format!("sign_order failed at {}c", c))?;
            let key = PoolKey { asset_id: params.asset_id, side: Side::Buy, price_cents: c };
            self.inner.insert(key, signed);
            count += 1;
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

/// Two concatenated UUID-v4s = 32 random bytes. uuid::Uuid::new_v4 uses
/// `getrandom` (cryptographic OS entropy), which is what we want for salt.
fn random_salt() -> [u8; 32] {
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let mut s = [0u8; 32];
    s[..16].copy_from_slice(a.as_bytes());
    s[16..].copy_from_slice(b.as_bytes());
    s
}

/// Build a BUY order at a given 1¢ price tick, sized to `bet_dollars`.
/// maker_amount = USDC offered (6 decimals), taker_amount = tokens bought (6 decimals).
fn build_buy_order(params: &PopulateParams, price_cents: u8) -> Result<Order> {
    if !(1..=99).contains(&price_cents) {
        bail!("price_cents must be in 1..=99, got {}", price_cents);
    }
    let price = price_cents as f64 / 100.0;

    let usdc_micro  = (params.bet_dollars * TOKEN_DECIMALS_FACTOR).round() as u64;
    if usdc_micro == 0 {
        bail!("bet too small to encode in USDC micros: {}", params.bet_dollars);
    }
    let shares      = params.bet_dollars / price;
    let token_micro = (shares * TOKEN_DECIMALS_FACTOR).round() as u64;

    Ok(Order {
        salt:           random_salt(),
        maker:          params.maker,
        signer:         params.signer,
        taker:          params.taker,
        token_id:       params.asset_id,
        maker_amount:   u256_from_u64(usdc_micro),
        taker_amount:   u256_from_u64(token_micro),
        expiration:     u256_from_u64(params.expiration_unix),
        nonce:          u256_from_u64(params.nonce),
        fee_rate_bps:   u256_from_u64(params.fee_rate_bps),
        side:           Side::Buy,
        signature_type: params.signature_type,
    })
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
        // maker (USDC) is the same — fixed bet size
        assert_eq!(a.order.maker_amount, b.order.maker_amount);
        // taker (tokens) differs — same dollars buys more shares at 25¢ than 75¢
        assert_ne!(a.order.taker_amount, b.order.taker_amount);
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
        // $5 buy at 50¢ should give 10_000_000 token-micros (10 shares)
        let params = sample_params();
        let order = build_buy_order(&params, 50).unwrap();
        assert_eq!(u256_to_dec(&order.maker_amount), "5000000");
        assert_eq!(u256_to_dec(&order.taker_amount), "10000000");

        // $5 at 25¢ → 20 shares
        let order = build_buy_order(&params, 25).unwrap();
        assert_eq!(u256_to_dec(&order.maker_amount), "5000000");
        assert_eq!(u256_to_dec(&order.taker_amount), "20000000");
    }

    #[test]
    fn build_buy_rejects_invalid_price_cents() {
        let params = sample_params();
        assert!(build_buy_order(&params, 0).is_err());
        assert!(build_buy_order(&params, 100).is_err());
    }

    // ── Concurrent access (DashMap should handle) ─────────────────────────

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
