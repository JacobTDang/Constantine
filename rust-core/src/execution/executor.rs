// Sprint 3 / J6 — Strategy → execution adapter.
//
// Bridges signal_loop and the rest of the execution stack:
//
//   signal --(SignalDecision)--> execute_signal
//      └─> resolve target (market_id → token_id, direction → side)
//      └─> kelly_size for bet amount
//      └─> RiskLimits.can_trade  (kill switch / loss / exposure / count)
//      └─> OrderPool.take        (consume one pre-signed order)
//      └─> ClobClient.submit_order (DRY_RUN-aware)
//      └─> RiskLimits.record_open on success
//
// Order matters: risk gate runs BEFORE pool.take, so a denied trade does not
// burn a pre-signed order. Once we take, we own the slot — even on transport
// failure we don't put the order back, because each salt is one-shot.

use anyhow::{Context, Result};

use crate::execution::clob::ClobClient;
use crate::execution::kelly::kelly_size;
use crate::execution::orders::{
    private_key_to_address, u256_from_dec, Domain, SignatureType, Side,
};
use crate::execution::presign::{OrderPool, PoolKey, PopulateParams};
use crate::features::FeatureState;
use crate::risk::limits::{RiskConfig, RiskLimits};
use crate::signals::{Direction, SignalDecision};
use crate::streams::polymarket::PolyMarket;

/// Side identifier in human-readable form (mirrors PolyMarket Up/Down).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SideLabel {
    Yes,    // Up token (clobTokenIds[0])
    No,     // Down token (clobTokenIds[1])
}

impl SideLabel {
    pub fn as_str(&self) -> &'static str {
        match self { SideLabel::Yes => "yes", SideLabel::No => "no" }
    }
}

/// Outcome of a single execution attempt. Every variant is a terminal state —
/// callers don't need to match further to know whether bookkeeping happened.
#[derive(Debug, Clone)]
pub enum ExecOutcome {
    /// Bailed out before any state change. Pool untouched, no exposure recorded.
    Skipped { reason: String },
    /// Order accepted by the CLOB (or short-circuited in DRY_RUN). Exposure
    /// has been recorded with the risk module.
    Submitted {
        bet_dollars: f64,
        price:       f64,
        side:        SideLabel,
        order_id:    String,
        dry_run:     bool,
        attempts:    u32,
    },
    /// CLOB returned a definitive failure (4xx or success=false). Pool order
    /// was consumed, exposure was NOT recorded.
    Rejected { reason: String, http_status: u16 },
    /// Transport failure even after retries. Pool order was consumed, exposure
    /// was NOT recorded.
    Errored { error: String },
}

/// Read-only references the executor needs each call. Keeping this as a
/// borrow-bag avoids forcing the caller to clone Arc handles per tick.
pub struct ExecContext<'a> {
    pub state:    &'a FeatureState,
    pub markets:  &'a [PolyMarket],
    pub risk:     &'a RiskLimits,
    pub risk_cfg: &'a RiskConfig,
    pub pool:     &'a OrderPool,
    pub client:   &'a ClobClient,
}

pub async fn execute_signal(
    decision: &SignalDecision,
    ctx:      &ExecContext<'_>,
) -> ExecOutcome {
    // 1. Resolve which token + price + p_win we'd buy.
    let target = match resolve_target(decision, ctx.state, ctx.markets) {
        Ok(t)  => t,
        Err(r) => {
            tracing::debug!(reason = %r, "execute_signal skipped");
            return ExecOutcome::Skipped { reason: r };
        }
    };

    // 2. Kelly sizing — caller's bankroll/kelly_fraction/max_bet from RiskConfig.
    let bet = kelly_size(
        target.p_win,
        target.price,
        ctx.risk_cfg.bankroll,
        ctx.risk_cfg.kelly_fraction,
        ctx.risk_cfg.max_bet_dollars,
    );
    if bet <= 0.0 {
        let reason = format!(
            "kelly returned 0 (p_win={:.3}, price={:.3})",
            target.p_win, target.price
        );
        tracing::debug!(reason = %reason, "execute_signal skipped");
        return ExecOutcome::Skipped { reason };
    }

    // 3. Risk gate — kill switch / daily / weekly / exposure / count / bet size.
    if let Err(e) = ctx.risk.can_trade(bet, ctx.risk_cfg) {
        let reason = format!("risk: {e}");
        tracing::debug!(reason = %reason, bet, "execute_signal skipped");
        return ExecOutcome::Skipped { reason };
    }

    // 4. Pre-signed order lookup. Must come AFTER risk so a denied trade
    //    doesn't burn a pre-signed slot.
    let asset_id = match u256_from_dec(&target.token_id) {
        Ok(id) => id,
        Err(e) => {
            return ExecOutcome::Skipped {
                reason: format!("token id parse failed: {e}"),
            };
        }
    };
    let price_cents = price_to_cents(target.price);
    let key = PoolKey { asset_id, side: Side::Buy, price_cents };
    let signed = match ctx.pool.take(&key) {
        Some(s) => s,
        None => {
            let reason = format!(
                "no pre-signed order at {}c on {}",
                price_cents,
                target.side.as_str(),
            );
            tracing::warn!(reason = %reason, "execute_signal skipped — pool miss");
            return ExecOutcome::Skipped { reason };
        }
    };

    // 5. Submit. Errors here mean we've already consumed the pool slot.
    let outcome = match ctx.client.submit_order(&signed).await {
        Ok(o)  => o,
        Err(e) => {
            tracing::error!(error = %e, "CLOB submit transport error");
            return ExecOutcome::Errored { error: e.to_string() };
        }
    };

    if !outcome.success {
        tracing::warn!(
            reason = %outcome.error_msg,
            http_status = outcome.http_status,
            "CLOB rejected order"
        );
        return ExecOutcome::Rejected {
            reason:      outcome.error_msg,
            http_status: outcome.http_status,
        };
    }

    // 6. Success → record exposure, return Submitted.
    ctx.risk.record_open(bet);
    tracing::info!(
        bet,
        price = target.price,
        side = target.side.as_str(),
        order_id = %outcome.order_id,
        dry_run = outcome.dry_run,
        attempts = outcome.attempts,
        "ORDER SUBMITTED"
    );
    ExecOutcome::Submitted {
        bet_dollars: bet,
        price:       target.price,
        side:        target.side,
        order_id:    outcome.order_id,
        dry_run:     outcome.dry_run,
        attempts:    outcome.attempts,
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

struct Target {
    token_id: String,
    price:    f64,
    p_win:    f64,
    side:     SideLabel,
}

fn resolve_target(
    decision: &SignalDecision,
    state:    &FeatureState,
    markets:  &[PolyMarket],
) -> Result<Target, String> {
    let oracle = match decision {
        SignalDecision::None              => return Err("no signal".to_string()),
        SignalDecision::Intramarket(_)    => {
            // Multi-leg execution lands in Sprint 4. Single-shot oracle path
            // for now.
            return Err("intramarket multi-leg not yet supported".to_string());
        }
        SignalDecision::Oracle(s) => s,
    };

    let market_id = state.primary_market_id.as_deref()
        .ok_or_else(|| "no primary market in state".to_string())?;
    let market = markets.iter().find(|m| m.id == market_id)
        .ok_or_else(|| format!("market {market_id} not in market list"))?;

    let (token_id, side) = match oracle.direction {
        Direction::Up   => (market.up_token_id.clone(),   SideLabel::Yes),
        Direction::Down => (market.down_token_id.clone(), SideLabel::No),
    };

    Ok(Target {
        token_id,
        price:    oracle.market_price,
        p_win:    oracle.fair_value,
        side,
    })
}

/// Polymarket BTC binary markets trade in 1¢ ticks. Convert a (0,1) probability
/// to its tick, clamped into the populatable range 1..=99.
fn price_to_cents(price: f64) -> u8 {
    let c = (price * 100.0).round();
    c.clamp(1.0, 99.0) as u8
}

// ── J7: Window-open pool population ─────────────────────────────────────────

/// Inputs needed to pre-sign one BTC window's worth of orders.
///
/// Held by the strategy layer; passed in fresh on every window-open hook so
/// nonce / expiration / depth can be tuned per window.
#[derive(Debug, Clone)]
pub struct WindowSetup<'a> {
    pub market:           &'a PolyMarket,
    /// Current YES ask in (0,1). Pool centred on this ¢ tick.
    pub midpoint_yes:     f64,
    /// Current NO ask in (0,1). Pool centred on this ¢ tick.
    pub midpoint_no:      f64,
    /// Per-order USDC (sized for the max kelly bet so we don't have to
    /// re-sign at submit time).
    pub bet_dollars:      f64,
    /// Unix seconds at which orders should expire (use market.close_time_ms / 1000
    /// for window-aligned expiry, or 0 for never).
    pub expiration_unix:  u64,
    /// ±N cents around each midpoint to pre-sign.
    pub depth:            u8,
    pub nonce:            u64,
    pub fee_rate_bps:     u64,
    pub signature_type:   SignatureType,
    pub private_key_hex:  String,
}

/// Counts of orders signed for each side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowPoolCounts {
    pub yes_count: usize,
    pub no_count:  usize,
}

/// Pre-sign BUY ladders for both YES and NO tokens of a single BTC window.
/// Caller is responsible for clearing stale state from `pool` before invoking
/// — repeated calls overwrite same-key entries but leave entries for OTHER
/// markets in place.
pub fn populate_pool_for_window(
    pool:  &OrderPool,
    setup: &WindowSetup<'_>,
) -> Result<WindowPoolCounts> {
    let domain = Domain::polymarket_polygon();
    let signer = private_key_to_address(&setup.private_key_hex)
        .context("derive signer address from private key")?;
    // EOA: maker = signer. PolyProxy / PolyGnosis would diverge — Sprint 9.
    let maker = signer;

    let yes_asset = u256_from_dec(&setup.market.up_token_id)
        .context("parse up_token_id")?;
    let no_asset  = u256_from_dec(&setup.market.down_token_id)
        .context("parse down_token_id")?;

    let common = |asset: [u8; 32]| PopulateParams {
        asset_id:        asset,
        maker,
        signer,
        taker:           [0u8; 20],
        bet_dollars:     setup.bet_dollars,
        expiration_unix: setup.expiration_unix,
        nonce:           setup.nonce,
        fee_rate_bps:    setup.fee_rate_bps,
        signature_type:  setup.signature_type,
    };

    let yes_mid = price_to_cents(setup.midpoint_yes);
    let no_mid  = price_to_cents(setup.midpoint_no);

    let yes_count = pool.populate_around_midpoint(
        &domain, &common(yes_asset), &setup.private_key_hex, yes_mid, setup.depth,
    ).context("pre-sign YES ladder")?;
    let no_count = pool.populate_around_midpoint(
        &domain, &common(no_asset), &setup.private_key_hex, no_mid, setup.depth,
    ).context("pre-sign NO ladder")?;

    tracing::info!(
        market = %setup.market.id,
        yes_count, no_count,
        yes_mid_cents = yes_mid, no_mid_cents = no_mid,
        depth = setup.depth,
        "pool populated for window"
    );

    Ok(WindowPoolCounts { yes_count, no_count })
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::auth::ApiCreds;
    use crate::execution::clob::{ClobClient, ClobConfig};
    use crate::execution::orders::{hex_to_address, Domain, SignatureType};
    use crate::execution::presign::{OrderPool, PopulateParams};
    use crate::risk::limits::{RiskConfig, RiskLimits};
    use crate::signals::oracle_arb::OracleArbSignal;
    use crate::signals::SignalDecision;
    use crate::streams::polymarket::PolyMarket;

    const TEST_PK:   &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
    const TEST_ADDR: &str = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266";

    // Decimal token-ids — Polymarket asset_ids are uint256, fit easily in u128 here.
    const UP_TOKEN_ID:   &str = "770286165307075478407821058745";
    const DOWN_TOKEN_ID: &str = "842741244417177364311129620769";

    fn sample_creds() -> ApiCreds {
        use base64::{engine::general_purpose::URL_SAFE, Engine as _};
        ApiCreds {
            api_key:        "00000000-1111-2222-3333-444444444444".to_string(),
            api_secret:     URL_SAFE.encode(b"test-secret-bytes-here-32-chars!"),
            api_passphrase: "passphrase".to_string(),
        }
    }

    fn sample_market() -> PolyMarket {
        PolyMarket {
            id:             "btc-5m-window-1".to_string(),
            question:       "BTC Up or Down?".to_string(),
            up_token_id:    UP_TOKEN_ID.to_string(),
            down_token_id:  DOWN_TOKEN_ID.to_string(),
            close_time_ms:  2_000_000_000_000,
            duration_min:   5,
            liquidity_usd:  1_000.0,
        }
    }

    fn sample_state() -> FeatureState {
        let mut s = FeatureState::default();
        s.primary_market_id = Some("btc-5m-window-1".to_string());
        s.poly_yes_price    = 0.45;
        s.poly_no_price     = 0.55;
        s.spot_price        = 80_300.0;
        s.window_open_price = 80_000.0;
        s
    }

    fn populate_pool_for_market(pool: &OrderPool, asset_id_dec: &str) {
        let domain = Domain::polymarket_polygon();
        let asset_id = crate::execution::orders::u256_from_dec(asset_id_dec).unwrap();
        let addr = hex_to_address(TEST_ADDR).unwrap();
        let params = PopulateParams {
            asset_id,
            maker:           addr,
            signer:          addr,
            taker:           [0u8; 20],
            bet_dollars:     5.00,
            expiration_unix: 0,
            nonce:           0,
            fee_rate_bps:    0,
            signature_type:  SignatureType::Eoa,
        };
        pool.populate_around_midpoint(&domain, &params, TEST_PK, 50, 15).unwrap();
    }

    fn make_client_dry_run() -> ClobClient {
        let cfg = ClobConfig::new(TEST_ADDR, sample_creds(), /*dry_run=*/true);
        ClobClient::new(cfg).unwrap()
    }

    fn oracle_up_signal(market_price: f64, fair_value: f64) -> SignalDecision {
        SignalDecision::Oracle(OracleArbSignal {
            direction:          Direction::Up,
            edge:               fair_value - market_price,
            fair_value,
            market_price,
            confidence:         0.8,
            time_to_close_secs: 60.0,
        })
    }

    fn oracle_down_signal(market_price: f64, fair_value: f64) -> SignalDecision {
        SignalDecision::Oracle(OracleArbSignal {
            direction:          Direction::Down,
            edge:               fair_value - market_price,
            fair_value,
            market_price,
            confidence:         0.8,
            time_to_close_secs: 60.0,
        })
    }

    // ── price_to_cents ────────────────────────────────────────────────────

    #[test]
    fn price_to_cents_rounds_to_nearest_tick() {
        assert_eq!(price_to_cents(0.45),  45);
        assert_eq!(price_to_cents(0.451), 45);
        assert_eq!(price_to_cents(0.456), 46);
        assert_eq!(price_to_cents(0.50),  50);
    }

    #[test]
    fn price_to_cents_clamps_endpoints() {
        assert_eq!(price_to_cents(0.005), 1);   // 0.5¢ → clamped to 1
        assert_eq!(price_to_cents(0.995), 99);  // 99.5¢ → clamped to 99
        assert_eq!(price_to_cents(-1.0),  1);
        assert_eq!(price_to_cents(2.0),   99);
    }

    // ── side_label ────────────────────────────────────────────────────────

    #[test]
    fn side_label_strings() {
        assert_eq!(SideLabel::Yes.as_str(), "yes");
        assert_eq!(SideLabel::No.as_str(),  "no");
    }

    // ── resolve_target ────────────────────────────────────────────────────

    #[test]
    fn resolve_target_picks_up_token_for_up_signal() {
        let dec = oracle_up_signal(0.45, 0.85);
        let st  = sample_state();
        let mk  = vec![sample_market()];
        let t = resolve_target(&dec, &st, &mk).unwrap();
        assert_eq!(t.token_id, UP_TOKEN_ID);
        assert_eq!(t.side, SideLabel::Yes);
        assert!((t.price - 0.45).abs() < 1e-9);
        assert!((t.p_win - 0.85).abs() < 1e-9);
    }

    #[test]
    fn resolve_target_picks_down_token_for_down_signal() {
        let dec = oracle_down_signal(0.50, 0.80);
        let t = resolve_target(&dec, &sample_state(), &[sample_market()]).unwrap();
        assert_eq!(t.token_id, DOWN_TOKEN_ID);
        assert_eq!(t.side, SideLabel::No);
    }

    #[test]
    fn resolve_target_errors_when_no_primary_market_id() {
        let mut st = sample_state();
        st.primary_market_id = None;
        let dec = oracle_up_signal(0.45, 0.85);
        assert!(resolve_target(&dec, &st, &[sample_market()]).is_err());
    }

    #[test]
    fn resolve_target_errors_when_market_not_in_list() {
        let dec = oracle_up_signal(0.45, 0.85);
        // Empty markets list → market_id won't match
        assert!(resolve_target(&dec, &sample_state(), &[]).is_err());
    }

    #[test]
    fn resolve_target_errors_for_intramarket() {
        use crate::signals::intramarket::IntramarketArbSignal;
        let dec = SignalDecision::Intramarket(IntramarketArbSignal {
            yes_ask: 0.40, no_ask: 0.40,
            gross_total: 0.80, gross_profit: 0.20,
            net_profit: 0.18, total_fees: 0.014,
        });
        assert!(resolve_target(&dec, &sample_state(), &[sample_market()]).is_err());
    }

    #[test]
    fn resolve_target_errors_for_no_signal() {
        assert!(resolve_target(&SignalDecision::None, &sample_state(), &[sample_market()]).is_err());
    }

    // ── execute_signal happy path ────────────────────────────────────────

    #[tokio::test]
    async fn execute_signal_submits_oracle_up_in_dry_run() {
        let pool = OrderPool::new();
        populate_pool_for_market(&pool, UP_TOKEN_ID);
        let pool_size_before = pool.len();

        let risk    = RiskLimits::new();
        let cfg     = RiskConfig::default();
        let client  = make_client_dry_run();
        let state   = sample_state();
        let markets = vec![sample_market()];

        let ctx = ExecContext {
            state: &state, markets: &markets,
            risk: &risk, risk_cfg: &cfg,
            pool: &pool, client: &client,
        };

        let dec = oracle_up_signal(0.45, 0.85);
        let outcome = execute_signal(&dec, &ctx).await;

        match outcome {
            ExecOutcome::Submitted { bet_dollars, price, side, dry_run, .. } => {
                assert!(bet_dollars > 0.0);
                assert!(bet_dollars <= cfg.max_bet_dollars);
                assert!((price - 0.45).abs() < 1e-9);
                assert_eq!(side, SideLabel::Yes);
                assert!(dry_run);
            }
            other => panic!("expected Submitted, got {other:?}"),
        }

        // Pool was decremented by exactly one
        assert_eq!(pool.len(), pool_size_before - 1);
        // Risk recorded the trade
        assert_eq!(risk.daily_trade_count(), 1);
        assert!(risk.open_exposure_dollars() > 0.0);
    }

    #[tokio::test]
    async fn execute_signal_routes_down_to_no_token() {
        let pool = OrderPool::new();
        populate_pool_for_market(&pool, DOWN_TOKEN_ID);

        let risk    = RiskLimits::new();
        let cfg     = RiskConfig::default();
        let client  = make_client_dry_run();
        let state   = sample_state();
        let markets = vec![sample_market()];
        let ctx = ExecContext {
            state: &state, markets: &markets,
            risk: &risk, risk_cfg: &cfg,
            pool: &pool, client: &client,
        };
        let dec = oracle_down_signal(0.55, 0.90);
        let outcome = execute_signal(&dec, &ctx).await;
        match outcome {
            ExecOutcome::Submitted { side, .. } => assert_eq!(side, SideLabel::No),
            other => panic!("expected Submitted on Down, got {other:?}"),
        }
    }

    // ── execute_signal skip paths ─────────────────────────────────────────

    #[tokio::test]
    async fn execute_signal_skips_when_no_signal() {
        let pool = OrderPool::new();
        let risk = RiskLimits::new();
        let cfg = RiskConfig::default();
        let client = make_client_dry_run();
        let state = sample_state();
        let markets = vec![sample_market()];
        let ctx = ExecContext { state: &state, markets: &markets, risk: &risk, risk_cfg: &cfg, pool: &pool, client: &client };

        let outcome = execute_signal(&SignalDecision::None, &ctx).await;
        assert!(matches!(outcome, ExecOutcome::Skipped { .. }));
        assert_eq!(risk.daily_trade_count(), 0);
    }

    #[tokio::test]
    async fn execute_signal_skips_when_kelly_returns_zero() {
        let pool = OrderPool::new();
        populate_pool_for_market(&pool, UP_TOKEN_ID);
        let pool_size_before = pool.len();

        let risk = RiskLimits::new();
        let cfg = RiskConfig::default();
        let client = make_client_dry_run();
        let state = sample_state();
        let markets = vec![sample_market()];
        let ctx = ExecContext { state: &state, markets: &markets, risk: &risk, risk_cfg: &cfg, pool: &pool, client: &client };

        // fair_value < market_price → no edge → kelly = 0
        let dec = oracle_up_signal(0.55, 0.45);
        let outcome = execute_signal(&dec, &ctx).await;
        assert!(matches!(outcome, ExecOutcome::Skipped { .. }));
        // Pool must NOT have been consumed
        assert_eq!(pool.len(), pool_size_before);
    }

    #[tokio::test]
    async fn execute_signal_skips_when_risk_kill_switch_active() {
        let pool = OrderPool::new();
        populate_pool_for_market(&pool, UP_TOKEN_ID);
        let pool_size_before = pool.len();

        let risk = RiskLimits::new();
        risk.activate_kill_switch();
        let cfg = RiskConfig::default();
        let client = make_client_dry_run();
        let state = sample_state();
        let markets = vec![sample_market()];
        let ctx = ExecContext { state: &state, markets: &markets, risk: &risk, risk_cfg: &cfg, pool: &pool, client: &client };

        let dec = oracle_up_signal(0.45, 0.85);
        let outcome = execute_signal(&dec, &ctx).await;
        match outcome {
            ExecOutcome::Skipped { reason } => assert!(reason.contains("kill switch")),
            other => panic!("expected Skipped(kill switch), got {other:?}"),
        }
        // Pool must NOT have been consumed
        assert_eq!(pool.len(), pool_size_before);
        assert_eq!(risk.daily_trade_count(), 0);
    }

    #[tokio::test]
    async fn execute_signal_skips_when_pool_has_no_entry_at_price() {
        // Pool populated for 35..=65, but signal price is 80¢ — outside range.
        let pool = OrderPool::new();
        populate_pool_for_market(&pool, UP_TOKEN_ID);
        let pool_size_before = pool.len();

        let risk = RiskLimits::new();
        let cfg = RiskConfig::default();
        let client = make_client_dry_run();
        let state = sample_state();
        let markets = vec![sample_market()];
        let ctx = ExecContext { state: &state, markets: &markets, risk: &risk, risk_cfg: &cfg, pool: &pool, client: &client };

        let dec = oracle_up_signal(0.80, 0.95);
        let outcome = execute_signal(&dec, &ctx).await;
        assert!(matches!(outcome, ExecOutcome::Skipped { .. }));
        // Pool size unchanged (no take)
        assert_eq!(pool.len(), pool_size_before);
        // No exposure recorded
        assert_eq!(risk.daily_trade_count(), 0);
    }

    #[tokio::test]
    async fn execute_signal_skips_when_market_unknown() {
        let pool = OrderPool::new();
        populate_pool_for_market(&pool, UP_TOKEN_ID);
        let risk = RiskLimits::new();
        let cfg = RiskConfig::default();
        let client = make_client_dry_run();
        let state = sample_state();
        // Empty markets list — primary_market_id won't resolve.
        let markets: Vec<PolyMarket> = vec![];
        let ctx = ExecContext { state: &state, markets: &markets, risk: &risk, risk_cfg: &cfg, pool: &pool, client: &client };
        let dec = oracle_up_signal(0.45, 0.85);
        let outcome = execute_signal(&dec, &ctx).await;
        assert!(matches!(outcome, ExecOutcome::Skipped { .. }));
    }

    // ── Pool-take ordering — risk MUST come first ─────────────────────────

    #[tokio::test]
    async fn risk_denial_does_not_consume_pool_slot() {
        // Two back-to-back attempts: first is denied (kill switch), second
        // succeeds. The single populated slot must still be available for #2.
        let pool = OrderPool::new();
        populate_pool_for_market(&pool, UP_TOKEN_ID);

        let risk = RiskLimits::new();
        risk.activate_kill_switch();
        let cfg = RiskConfig::default();
        let client = make_client_dry_run();
        let state = sample_state();
        let markets = vec![sample_market()];
        let ctx = ExecContext { state: &state, markets: &markets, risk: &risk, risk_cfg: &cfg, pool: &pool, client: &client };

        let dec = oracle_up_signal(0.45, 0.85);
        let _ = execute_signal(&dec, &ctx).await;        // denied
        risk.reset_kill_switch();
        let outcome = execute_signal(&dec, &ctx).await;  // now should submit
        assert!(matches!(outcome, ExecOutcome::Submitted { .. }));
    }

    // ── Repeated execution at same price exhausts pool ────────────────────

    // ── J7: populate_pool_for_window ─────────────────────────────────────

    fn sample_setup<'a>(market: &'a PolyMarket) -> WindowSetup<'a> {
        WindowSetup {
            market,
            midpoint_yes:    0.45,
            midpoint_no:     0.55,
            bet_dollars:     5.00,
            expiration_unix: 0,
            depth:           15,
            nonce:           0,
            fee_rate_bps:    0,
            signature_type:  SignatureType::Eoa,
            private_key_hex: TEST_PK.to_string(),
        }
    }

    #[test]
    fn populate_pool_for_window_signs_both_sides() {
        let pool = OrderPool::new();
        let market = sample_market();
        let setup = sample_setup(&market);
        let counts = populate_pool_for_window(&pool, &setup).unwrap();
        // depth 15, midpoints not near edges → exactly 31 orders each
        assert_eq!(counts.yes_count, 31);
        assert_eq!(counts.no_count,  31);
        assert_eq!(pool.len(), 62);
    }

    #[test]
    fn populate_pool_for_window_keys_yes_under_up_token() {
        let pool = OrderPool::new();
        let market = sample_market();
        let setup = sample_setup(&market);
        populate_pool_for_window(&pool, &setup).unwrap();

        let up_asset   = crate::execution::orders::u256_from_dec(UP_TOKEN_ID).unwrap();
        let down_asset = crate::execution::orders::u256_from_dec(DOWN_TOKEN_ID).unwrap();

        // YES midpoint 45¢ → 30..=60 keyed under up_asset
        let key_yes = PoolKey { asset_id: up_asset,   side: Side::Buy, price_cents: 45 };
        let key_no  = PoolKey { asset_id: down_asset, side: Side::Buy, price_cents: 55 };
        assert!(pool.contains(&key_yes));
        assert!(pool.contains(&key_no));
        // Cross-asset keys at the same cent level should NOT exist
        let cross_yes = PoolKey { asset_id: up_asset,   side: Side::Buy, price_cents: 55 };
        let cross_no  = PoolKey { asset_id: down_asset, side: Side::Buy, price_cents: 45 };
        // 55 is in 30..=60 for YES, so cross_yes IS present
        // 45 is in 40..=70 for NO, so cross_no IS present too
        assert!(pool.contains(&cross_yes));
        assert!(pool.contains(&cross_no));
    }

    #[test]
    fn populate_pool_for_window_rejects_bad_private_key() {
        let pool = OrderPool::new();
        let market = sample_market();
        let mut setup = sample_setup(&market);
        setup.private_key_hex = "0xnotvalid".to_string();
        assert!(populate_pool_for_window(&pool, &setup).is_err());
        assert!(pool.is_empty());   // partial population must not leak
    }

    #[test]
    fn populate_pool_for_window_rejects_bad_token_id() {
        let pool = OrderPool::new();
        let mut market = sample_market();
        market.up_token_id = "not-a-decimal".to_string();
        let setup = sample_setup(&market);
        assert!(populate_pool_for_window(&pool, &setup).is_err());
    }

    #[test]
    fn populate_pool_for_window_clamps_midpoint_at_edge() {
        let pool = OrderPool::new();
        let market = sample_market();
        let mut setup = sample_setup(&market);
        // Extreme midpoint near 0 → window clamps to 1..=15+midpoint_cents
        setup.midpoint_yes = 0.02;     // 2¢ → clamps to 1..=17 = 17 orders
        let counts = populate_pool_for_window(&pool, &setup).unwrap();
        assert_eq!(counts.yes_count, 17);
    }

    // J8: end-to-end — populate via window helper, then execute_signal succeeds.
    #[tokio::test]
    async fn end_to_end_window_setup_then_oracle_execute() {
        let pool = OrderPool::new();
        let market = sample_market();

        let counts = populate_pool_for_window(&pool, &sample_setup(&market)).unwrap();
        assert_eq!(counts.yes_count + counts.no_count, pool.len());

        let risk    = RiskLimits::new();
        let cfg     = RiskConfig::default();
        let client  = make_client_dry_run();
        let state   = sample_state();
        let markets = vec![market];
        let ctx = ExecContext {
            state: &state, markets: &markets,
            risk: &risk, risk_cfg: &cfg,
            pool: &pool, client: &client,
        };

        // Up signal at 45¢ — should hit the YES ladder we just signed.
        let dec = oracle_up_signal(0.45, 0.85);
        let outcome = execute_signal(&dec, &ctx).await;
        assert!(matches!(outcome, ExecOutcome::Submitted { .. }));
        assert_eq!(risk.daily_trade_count(), 1);

        // Down signal at 55¢ — should hit NO ladder, independently.
        let dec_d = oracle_down_signal(0.55, 0.85);
        let outcome_d = execute_signal(&dec_d, &ctx).await;
        assert!(matches!(outcome_d, ExecOutcome::Submitted { .. }));
        assert_eq!(risk.daily_trade_count(), 2);
    }

    #[tokio::test]
    async fn second_call_at_same_price_reports_pool_miss() {
        let pool = OrderPool::new();
        populate_pool_for_market(&pool, UP_TOKEN_ID);
        let risk = RiskLimits::new();
        let cfg = RiskConfig::default();
        let client = make_client_dry_run();
        let state = sample_state();
        let markets = vec![sample_market()];
        let ctx = ExecContext { state: &state, markets: &markets, risk: &risk, risk_cfg: &cfg, pool: &pool, client: &client };

        let dec = oracle_up_signal(0.45, 0.85);
        let first = execute_signal(&dec, &ctx).await;
        assert!(matches!(first, ExecOutcome::Submitted { .. }));
        // Same key should now be missing
        let second = execute_signal(&dec, &ctx).await;
        match second {
            ExecOutcome::Skipped { reason } => assert!(reason.contains("no pre-signed")),
            other => panic!("expected Skipped(pool miss), got {other:?}"),
        }
    }
}
