// Strategy 07 — UMA dispute + LLM gate signal layer.
//
// Pure-logic evaluator that takes an active UMA dispute (proposer's
// claim + current Polymarket price) and an LLM classification of which
// side UMA will ultimately vote for, and decides whether to take a
// position on the proposer's side.
//
// Three gates per spec:
//   1. LLM confidence >= 0.85
//   2. LLM agrees with proposer (we only trade when both signals
//      point the same way — independent probabilities multiply)
//   3. Market price leaves enough edge after fees (<= 0.92)
//
// Plus our position-sizing rule: $50 cap per dispute, 3% bankroll cap.
// The sizing decision happens in the executor layer; this module just
// emits the entry decision.

use serde::{Deserialize, Serialize};

use crate::signals::Direction;

pub const MIN_LLM_CONFIDENCE: f64 = 0.85;

/// Skip when proposer's side is already trading above this — no spread
/// left to capture after Polymarket's 1.8% taker fee.
pub const MAX_ENTRY_PRICE: f64 = 0.92;

/// Recommended position size in dollars (cap, not floor). The runner's
/// risk gate can lower this further; we don't allow it higher.
pub const RECOMMENDED_SIZE_USD: f64 = 50.0;

/// Fraction of bankroll cap. With a $1500 bankroll, $50 × 6 disputes
/// = $300 = 20% — but per-position cap stays at 3% (= $50).
pub const PER_POSITION_BANKROLL_FRACTION: f64 = 0.03;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum DisputeSide {
    Yes,
    No,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DisputeEvent {
    pub uma_request_id:     String,
    pub market_id:          String,
    pub yes_token_id:       String,
    pub no_token_id:        String,
    pub proposer_side:      DisputeSide,
    pub dispute_started_ms: u64,
    /// Current Polymarket YES ask (we'll derive NO from this).
    pub yes_ask:            f64,
    pub yes_bid:            f64,
    /// Total notional of this market (for governance-attack risk filter).
    pub total_notional_usd: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LlmClassification {
    pub predicted_side: DisputeSide,
    pub confidence:     f64,
    pub key_source_url: String,
    pub rationale:      String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UmaDisputeDecision {
    pub market_id:        String,
    pub direction:        Direction,
    pub entry_price:      f64,
    pub recommended_size_usd: f64,
    pub combined_confidence:  f64,
    pub key_source_url:   String,
}

pub fn evaluate_uma_dispute(
    dispute:        &DisputeEvent,
    llm_verdict:    &LlmClassification,
) -> Option<UmaDisputeDecision> {
    // Gate 1: LLM confidence
    if llm_verdict.confidence < MIN_LLM_CONFIDENCE {
        return None;
    }

    // Gate 2: LLM must agree with proposer (signals concur → product
    // probability holds; if they disagree we're betting against the 99%
    // proposer base rate, which is the worst-case trade).
    if llm_verdict.predicted_side != dispute.proposer_side {
        return None;
    }

    // Gate 3: market still has spread to capture
    let (direction, entry_price) = match dispute.proposer_side {
        DisputeSide::Yes => (Direction::Up, dispute.yes_ask),
        DisputeSide::No  => (Direction::Down, 1.0 - dispute.yes_bid),
    };
    if entry_price <= 0.0 || entry_price >= 1.0 {
        return None;
    }
    if entry_price > MAX_ENTRY_PRICE {
        return None;
    }

    // Gate 4: governance-attack risk — large markets attract attacks. Skip
    // anything > $1M total notional unless LLM is very confident.
    if dispute.total_notional_usd > 1_000_000.0 && llm_verdict.confidence < 0.95 {
        return None;
    }

    // Combined confidence approximation. 0.99 = proposer base rate (per spec).
    let combined_confidence = 0.99 * llm_verdict.confidence;

    Some(UmaDisputeDecision {
        market_id:            dispute.market_id.clone(),
        direction,
        entry_price,
        recommended_size_usd: RECOMMENDED_SIZE_USD,
        combined_confidence,
        key_source_url:       llm_verdict.key_source_url.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dispute(proposer: DisputeSide, yes_ask: f64, yes_bid: f64, notional: f64) -> DisputeEvent {
        DisputeEvent {
            uma_request_id:     "0xreq".into(),
            market_id:          "0xmarket".into(),
            yes_token_id:       "yes".into(),
            no_token_id:        "no".into(),
            proposer_side:      proposer,
            dispute_started_ms: 1_000_000_000_000,
            yes_ask,
            yes_bid,
            total_notional_usd: notional,
        }
    }

    fn llm(side: DisputeSide, conf: f64) -> LlmClassification {
        LlmClassification {
            predicted_side: side,
            confidence:     conf,
            key_source_url: "https://example.com/source".into(),
            rationale:      "rule cites X; source confirms Y".into(),
        }
    }

    #[test]
    fn happy_path_proposer_yes_llm_agrees_fires() {
        let d = dispute(DisputeSide::Yes, 0.85, 0.83, 50_000.0);
        let v = llm(DisputeSide::Yes, 0.90);
        let dec = evaluate_uma_dispute(&d, &v).expect("fires");
        assert_eq!(dec.direction, Direction::Up);
        assert!((dec.entry_price - 0.85).abs() < 1e-9);
        assert!((dec.recommended_size_usd - 50.0).abs() < 1e-9);
        assert!(dec.combined_confidence > 0.88);
    }

    #[test]
    fn happy_path_proposer_no_llm_agrees_fires() {
        let d = dispute(DisputeSide::No, 0.20, 0.18, 50_000.0);
        let v = llm(DisputeSide::No, 0.90);
        let dec = evaluate_uma_dispute(&d, &v).expect("fires");
        assert_eq!(dec.direction, Direction::Down);
        assert!((dec.entry_price - 0.82).abs() < 1e-9);
    }

    #[test]
    fn llm_disagrees_with_proposer_rejected() {
        let d = dispute(DisputeSide::Yes, 0.85, 0.83, 50_000.0);
        let v = llm(DisputeSide::No, 0.90);
        assert!(evaluate_uma_dispute(&d, &v).is_none());
    }

    #[test]
    fn low_llm_confidence_rejected() {
        let d = dispute(DisputeSide::Yes, 0.85, 0.83, 50_000.0);
        let v = llm(DisputeSide::Yes, 0.80);   // below 0.85
        assert!(evaluate_uma_dispute(&d, &v).is_none());
    }

    #[test]
    fn entry_too_high_rejected() {
        let d = dispute(DisputeSide::Yes, 0.95, 0.93, 50_000.0);
        let v = llm(DisputeSide::Yes, 0.90);
        assert!(evaluate_uma_dispute(&d, &v).is_none());
    }

    #[test]
    fn large_market_with_marginal_llm_conf_rejected() {
        // $2M notional, LLM only 0.90 (below 0.95 high bar)
        let d = dispute(DisputeSide::Yes, 0.85, 0.83, 2_000_000.0);
        let v = llm(DisputeSide::Yes, 0.90);
        assert!(evaluate_uma_dispute(&d, &v).is_none());
    }

    #[test]
    fn large_market_with_high_llm_conf_accepted() {
        let d = dispute(DisputeSide::Yes, 0.85, 0.83, 2_000_000.0);
        let v = llm(DisputeSide::Yes, 0.97);   // above 0.95 high bar
        assert!(evaluate_uma_dispute(&d, &v).is_some());
    }

    #[test]
    fn boundary_llm_confidence_passes() {
        let d = dispute(DisputeSide::Yes, 0.85, 0.83, 50_000.0);
        let v = llm(DisputeSide::Yes, MIN_LLM_CONFIDENCE);
        assert!(evaluate_uma_dispute(&d, &v).is_some());
    }

    #[test]
    fn boundary_entry_price_passes() {
        let d = dispute(DisputeSide::Yes, MAX_ENTRY_PRICE, MAX_ENTRY_PRICE - 0.01, 50_000.0);
        let v = llm(DisputeSide::Yes, 0.90);
        assert!(evaluate_uma_dispute(&d, &v).is_some());
    }

    #[test]
    fn invalid_yes_ask_rejected() {
        let d = dispute(DisputeSide::Yes, 0.0, 0.0, 50_000.0);
        let v = llm(DisputeSide::Yes, 0.90);
        assert!(evaluate_uma_dispute(&d, &v).is_none());
    }

    #[test]
    fn combined_confidence_is_product() {
        let d = dispute(DisputeSide::Yes, 0.85, 0.83, 50_000.0);
        let v = llm(DisputeSide::Yes, 0.90);
        let dec = evaluate_uma_dispute(&d, &v).unwrap();
        assert!((dec.combined_confidence - 0.99 * 0.90).abs() < 1e-9);
    }

    #[test]
    fn key_source_url_propagates() {
        let d = dispute(DisputeSide::Yes, 0.85, 0.83, 50_000.0);
        let mut v = llm(DisputeSide::Yes, 0.90);
        v.key_source_url = "https://specific.example/page".into();
        let dec = evaluate_uma_dispute(&d, &v).unwrap();
        assert_eq!(dec.key_source_url, "https://specific.example/page");
    }
}
