// Strategy 2 — Multi-leg event arb portfolio orchestration.
//
// Given an ArbEdge (= an exclusive-winner event with N captureable
// legs), submit one GTC limit order per leg as a maker. Resting orders
// fill when retail crosses our prices.
//
// What this layer does:
//   - For each leg in the portfolio, sign and submit a GTC BUY-NO order
//   - Track which legs were accepted by Polymarket vs rejected
//   - Record each accepted leg as a Position in PositionStore (tagged
//     with event_id so settlement can group them)
//
// What this layer does NOT do (deferred):
//   - Active management of resting orders (refresh, re-price as book moves)
//   - Auto-cancel orders that don't fill within X minutes (Phase D)
//   - Portfolio re-balancing if a candidate is added mid-event (GAPS.md)
//
// Sizing: per-leg capital cap = max_bet_dollars / portfolio.len(). Caps
// total event exposure at max_bet, leaving the rest of the bankroll for
// other strategies.

use std::sync::Arc;

use crate::execution::clob::{ClobClient, OrderType, SubmitOutcome};
use crate::execution::executor::{ExecContext, SigningParams};
use crate::execution::orders::{
    hex_to_address, private_key_to_address,
    u256_from_dec, SignatureType, SignedOrder,
};
use crate::execution::presign::{sign_buy_now, BuyOrderInputs};
use crate::risk::limits::{RiskConfig, RiskLimits};
use crate::signals::event_arb::ArbEdge;
use crate::storage::PositionStore;

/// Outcome of submitting one leg of a portfolio. Captured separately
/// per leg so partial-success scenarios can be inspected.
#[derive(Debug, Clone)]
pub struct LegOutcome {
    pub market_id:    String,
    pub candidate:    String,
    pub price_cents:  u8,
    pub bet_dollars:  f64,
    pub success:      bool,
    pub order_id:     String,
    pub error_msg:    String,
    pub dry_run:      bool,
}

/// Result of submitting a full event-arb portfolio.
#[derive(Debug, Clone)]
pub struct PortfolioOutcome {
    pub event_id:     String,
    pub title:        String,
    pub legs:         Vec<LegOutcome>,
}

impl PortfolioOutcome {
    pub fn n_accepted(&self) -> usize {
        self.legs.iter().filter(|l| l.success).count()
    }
    pub fn n_rejected(&self) -> usize {
        self.legs.iter().filter(|l| !l.success).count()
    }
}

/// Per-event sizing — caps total event exposure at `event_budget_usd`
/// regardless of leg count.
#[derive(Debug, Clone, Copy)]
pub struct EventArbConfig {
    pub event_budget_usd: f64,
    pub min_leg_dollars:  f64,    // skip legs below this (Polymarket min = $5)
}

impl Default for EventArbConfig {
    fn default() -> Self {
        Self {
            event_budget_usd: 60.0,    // 2× max single-trade default
            min_leg_dollars:  5.0,
        }
    }
}

/// Submit one event-arb portfolio as a sequence of GTC maker orders.
///
/// The legs are placed sequentially (not via tokio::join) because:
///   1. Polymarket may have per-account rate limits we want to respect
///   2. Sequential placement makes it easier to halt mid-portfolio if
///      risk gates trip after the first few legs
///   3. The latency benefit of parallel submit is small relative to the
///      hours-to-months hold horizon
///
/// Risk model: total event exposure = sum(leg.bet_dollars). risk gate
/// runs ONCE on the total at the start. Per-leg the existing exposure
/// counter accumulates so concurrent strategies see correct totals.
pub async fn execute_event_arb(
    edge:      &ArbEdge,
    cfg:       &EventArbConfig,
    ctx_risk:  &Arc<RiskLimits>,
    risk_cfg:  &RiskConfig,
    signing:   &Arc<SigningParams>,
    client:    &Arc<ClobClient>,
    positions: Option<&Arc<PositionStore>>,
) -> PortfolioOutcome {
    let n = edge.portfolio.len();
    if n < 2 {
        return PortfolioOutcome {
            event_id: edge.event_id.clone(),
            title:    edge.title.clone(),
            legs:     vec![],
        };
    }

    // Per-strategy kill switch — distinct from the global kill checked
    // by `can_trade` below. Lets us halt event-arb specifically without
    // taking the whole bot down.
    if ctx_risk.is_strategy_killed(crate::risk::limits::Strategy::EventArb) {
        tracing::warn!(event = %edge.title,
            "event arb skipped — strategy kill switch active");
        return PortfolioOutcome {
            event_id: edge.event_id.clone(),
            title:    edge.title.clone(),
            legs:     vec![],
        };
    }

    // Per-leg sizing
    let per_leg = (cfg.event_budget_usd / n as f64).max(cfg.min_leg_dollars);
    let total_bet = per_leg * n as f64;

    // Risk gating for multi-leg portfolios:
    //   1. each leg must satisfy can_trade (max_bet, kill switches, etc.)
    //   2. total_bet must fit under max_open_exposure
    // Calling can_trade with total_bet would fail BetSizeInvalid because
    // total typically > max_bet_dollars by design.
    if let Err(e) = ctx_risk.can_trade(per_leg, risk_cfg) {
        tracing::warn!(error = %e, event = %edge.title, per_leg,
            "event arb skipped — per-leg risk gate");
        return PortfolioOutcome {
            event_id: edge.event_id.clone(),
            title:    edge.title.clone(),
            legs:     vec![],
        };
    }
    // Separate exposure check on the combined portfolio.
    let current_exposure = ctx_risk.open_exposure_dollars();
    if current_exposure + total_bet > risk_cfg.max_open_exposure_dollars {
        tracing::warn!(
            event = %edge.title,
            current_exposure, total_bet,
            limit = risk_cfg.max_open_exposure_dollars,
            "event arb skipped — exposure cap"
        );
        return PortfolioOutcome {
            event_id: edge.event_id.clone(),
            title:    edge.title.clone(),
            legs:     vec![],
        };
    }

    let mut leg_outcomes = Vec::with_capacity(n);

    // Resolve maker / signer addresses ONCE
    let signer = match private_key_to_address(&signing.private_key_hex) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "event arb: signer derivation failed");
            return PortfolioOutcome {
                event_id: edge.event_id.clone(),
                title:    edge.title.clone(),
                legs:     vec![],
            };
        }
    };
    let maker = match (signing.signature_type, signing.funder_address.as_deref()) {
        (SignatureType::Eoa, _) => signer,
        (_, Some(addr)) => match hex_to_address(addr) {
            Ok(a) => a,
            Err(e) => {
                tracing::error!(error = %e, "event arb: funder parse failed");
                return PortfolioOutcome {
                    event_id: edge.event_id.clone(),
                    title:    edge.title.clone(),
                    legs:     vec![],
                };
            }
        },
        (sig, None) => {
            tracing::error!("{:?} requires funder_address", sig);
            return PortfolioOutcome {
                event_id: edge.event_id.clone(),
                title:    edge.title.clone(),
                legs:     vec![],
            };
        }
    };

    for leg in &edge.portfolio {
        // BUY NO at our limit price = leg's no_ask. Maker fill will
        // happen when retail crosses; until then the order rests.
        let no_price = leg.no_ask();
        let price_cents = (no_price * 100.0).round().clamp(1.0, 99.0) as u8;

        let asset_id = match u256_from_dec(&leg.no_token_id) {
            Ok(id) => id,
            Err(e) => {
                leg_outcomes.push(LegOutcome {
                    market_id: leg.market_id.clone(),
                    candidate: leg.candidate.clone(),
                    price_cents, bet_dollars: per_leg,
                    success: false, order_id: "".into(),
                    error_msg: format!("token id parse: {e}"),
                    dry_run: false,
                });
                continue;
            }
        };

        // Build + sign one BUY order at the limit price
        let inputs = BuyOrderInputs {
            asset_id,
            maker,
            signer,
            taker:           signing.taker,
            bet_dollars:     per_leg,
            price_cents,
            // Long-hold: expire 30 days out as a safety valve
            expiration_unix: chrono::Utc::now().timestamp() as u64 + 30 * 86_400,
            nonce:           signing.nonce,
            fee_rate_bps:    signing.fee_rate_bps,
            signature_type:  signing.signature_type,
        };
        let signed = match sign_buy_now(&inputs, &signing.domain, &signing.private_key_hex) {
            Ok(s) => s,
            Err(e) => {
                leg_outcomes.push(LegOutcome {
                    market_id: leg.market_id.clone(),
                    candidate: leg.candidate.clone(),
                    price_cents, bet_dollars: per_leg,
                    success: false, order_id: "".into(),
                    error_msg: format!("sign: {e}"),
                    dry_run: false,
                });
                continue;
            }
        };

        // Submit as GTC (rests on book) — this is the maker path
        let submit_res = client.submit_order_as(&signed, OrderType::Gtc, None).await;
        let outcome = leg_outcome_from_submit(&signed, leg, per_leg, price_cents, submit_res);

        // Record to PositionStore on success
        if outcome.success {
            if !outcome.dry_run {
                ctx_risk.record_open(per_leg);
            }
            if let Some(positions) = positions {
                let order_id = if outcome.order_id.is_empty() {
                    format!("local-{}", leg.market_id)
                } else {
                    outcome.order_id.clone()
                };
                let _ = positions.record_open(
                    &order_id, &leg.market_id, "no", per_leg, no_price,
                );
            }
        }

        leg_outcomes.push(outcome);
    }

    let p = PortfolioOutcome {
        event_id: edge.event_id.clone(),
        title:    edge.title.clone(),
        legs:     leg_outcomes,
    };
    tracing::info!(
        event = %edge.title,
        legs_total    = n,
        legs_accepted = p.n_accepted(),
        legs_rejected = p.n_rejected(),
        captureable_dollars = edge.captureable_dollars,
        "event arb portfolio submitted"
    );
    p
}

/// Project a (signed_order, submit_result) pair into a LegOutcome.
fn leg_outcome_from_submit(
    _signed:    &SignedOrder,
    leg:        &crate::signals::event_arb::ArbLeg,
    per_leg:    f64,
    price_cents: u8,
    submit_res: anyhow::Result<SubmitOutcome>,
) -> LegOutcome {
    match submit_res {
        Ok(o) => LegOutcome {
            market_id:   leg.market_id.clone(),
            candidate:   leg.candidate.clone(),
            price_cents, bet_dollars: per_leg,
            success:     o.success,
            order_id:    o.order_id,
            error_msg:   o.error_msg,
            dry_run:     o.dry_run,
        },
        Err(e) => LegOutcome {
            market_id:   leg.market_id.clone(),
            candidate:   leg.candidate.clone(),
            price_cents, bet_dollars: per_leg,
            success:     false,
            order_id:    String::new(),
            error_msg:   format!("transport: {e}"),
            dry_run:     false,
        },
    }
}

// Re-export ExecContext to satisfy unused-import lint when we expand
// this module to take a full context shape later.
#[allow(dead_code)]
fn _ctx_warmup(_: &ExecContext<'_>) {}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::auth::ApiCreds;
    use crate::execution::clob::{ClobClient, ClobConfig};
    use crate::execution::orders::Domain;
    use crate::signals::event_arb::ArbLeg;
    use base64::{engine::general_purpose::URL_SAFE, Engine as _};
    use std::path::PathBuf;

    const TEST_PK: &str =
        "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
    const TEST_ADDR: &str = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266";

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "constantine-eventarb-{}", uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn sample_creds() -> ApiCreds {
        ApiCreds {
            api_key:        "00000000-1111-2222-3333-444444444444".into(),
            api_secret:     URL_SAFE.encode(b"test-secret-bytes-here-32-chars!"),
            api_passphrase: "passphrase".into(),
        }
    }

    fn sample_signing() -> Arc<SigningParams> {
        Arc::new(SigningParams {
            domain:          Domain::polymarket_polygon(),
            private_key_hex: TEST_PK.into(),
            signature_type:  SignatureType::Eoa,
            funder_address:  None,
            nonce:           0,
            fee_rate_bps:    0,
            taker:           [0u8; 20],
        })
    }

    fn make_dry_client() -> Arc<ClobClient> {
        let cfg = ClobConfig::new(TEST_ADDR, sample_creds(), true);
        Arc::new(ClobClient::new(cfg).unwrap())
    }

    fn sample_leg(cand: &str, no_ask: f64) -> ArbLeg {
        let yes_bid = 1.0 - no_ask;
        let yes_ask = yes_bid + 0.01;
        ArbLeg {
            // Use real decimal-format token ids so u256_from_dec parses.
            market_id:    format!("market_{cand}"),
            candidate:    cand.to_string(),
            yes_token_id: "1234567890".into(),
            no_token_id:  "9876543210".into(),
            yes_ask, yes_bid,
            liquidity:    1000.0,
            volume_24h:   100.0,
        }
    }

    fn sample_edge() -> ArbEdge {
        ArbEdge {
            event_id: "evt1".into(),
            title:    "Test Champion".into(),
            portfolio: vec![
                sample_leg("a", 0.70),
                sample_leg("b", 0.70),
                sample_leg("c", 0.75),
                sample_leg("d", 0.75),
                sample_leg("e", 0.90),
            ],
            captureable_dollars: 0.20,
            captureable_roi:     0.05,
        }
    }

    #[tokio::test]
    async fn portfolio_submitted_per_leg_in_dry_run() {
        let edge = sample_edge();
        let signing = sample_signing();
        let client = make_dry_client();
        let risk = Arc::new(RiskLimits::new());
        let positions = Arc::new(PositionStore::open(&temp_dir()).unwrap());

        let cfg = EventArbConfig::default();
        let p = execute_event_arb(
            &edge, &cfg, &risk, &RiskConfig::default(),
            &signing, &client, Some(&positions),
        ).await;

        assert_eq!(p.legs.len(), 5);
        assert_eq!(p.n_accepted(), 5);   // all legs succeed in DRY_RUN
        // Per-leg sizing: 60 / 5 = 12 each
        for l in &p.legs {
            assert!((l.bet_dollars - 12.0).abs() < 1e-9);
            assert!(l.dry_run);
        }
        // 5 positions recorded
        assert_eq!(positions.len(), 5);
        let all = positions.all();
        for pos in &all {
            assert_eq!(pos.side, "no");
        }
    }

    #[tokio::test]
    async fn portfolio_skipped_when_too_few_legs() {
        let mut edge = sample_edge();
        edge.portfolio.truncate(1);
        let signing = sample_signing();
        let client = make_dry_client();
        let risk = Arc::new(RiskLimits::new());
        let positions = Arc::new(PositionStore::open(&temp_dir()).unwrap());

        let p = execute_event_arb(
            &edge, &EventArbConfig::default(), &risk, &RiskConfig::default(),
            &signing, &client, Some(&positions),
        ).await;
        assert_eq!(p.legs.len(), 0);
        assert_eq!(positions.len(), 0);
    }

    #[tokio::test]
    async fn portfolio_skipped_on_risk_gate_kill_switch() {
        let edge = sample_edge();
        let signing = sample_signing();
        let client = make_dry_client();
        let risk = Arc::new(RiskLimits::new());
        risk.activate_kill_switch();
        let positions = Arc::new(PositionStore::open(&temp_dir()).unwrap());

        let p = execute_event_arb(
            &edge, &EventArbConfig::default(), &risk, &RiskConfig::default(),
            &signing, &client, Some(&positions),
        ).await;
        assert_eq!(p.legs.len(), 0);
        assert_eq!(positions.len(), 0);
    }

    #[tokio::test]
    async fn portfolio_dry_run_does_not_consume_exposure() {
        let edge = sample_edge();
        let signing = sample_signing();
        let client = make_dry_client();
        let risk = Arc::new(RiskLimits::new());
        let positions = Arc::new(PositionStore::open(&temp_dir()).unwrap());

        let _ = execute_event_arb(
            &edge, &EventArbConfig::default(), &risk, &RiskConfig::default(),
            &signing, &client, Some(&positions),
        ).await;
        // DRY_RUN: positions ledger filled but risk_limits.exposure stays 0
        assert_eq!(risk.open_exposure_dollars(), 0.0);
        assert_eq!(positions.len(), 5);
    }

    #[test]
    fn n_accepted_n_rejected_counts() {
        let p = PortfolioOutcome {
            event_id: "e".into(), title: "T".into(),
            legs: vec![
                LegOutcome {
                    market_id: "m1".into(), candidate: "a".into(),
                    price_cents: 70, bet_dollars: 10.0,
                    success: true, order_id: "ok".into(),
                    error_msg: "".into(), dry_run: true,
                },
                LegOutcome {
                    market_id: "m2".into(), candidate: "b".into(),
                    price_cents: 70, bet_dollars: 10.0,
                    success: false, order_id: "".into(),
                    error_msg: "rejected".into(), dry_run: false,
                },
            ],
        };
        assert_eq!(p.n_accepted(), 1);
        assert_eq!(p.n_rejected(), 1);
    }
}
