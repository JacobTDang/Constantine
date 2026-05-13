// Strategy 02 — Resolution-proximity decay signal layer.
//
// Promotes EDGE-E (hibernating scanner, observe-only) to executable by
// adding a per-resolution-source truth gate. The scanner identifies
// candidates near close; this module accepts (candidate + KnownOutcome
// from a truth source) and decides whether to enter the buy-and-hold
// trade.
//
// Truth sources are external (ESPN box score, Binance OHLCV, Chainlink
// historical reads, CourtListener). They live as Phase 3 sidecars and
// emit KnownOutcome rows via JSONL/IPC. This module just consumes them.

use serde::{Deserialize, Serialize};

use crate::signals::Direction;

/// Don't enter unless the truth source is at-or-above this confidence.
/// At 0.99 we require "source has printed FINAL" — no in-play / interim
/// numbers.
pub const MIN_SOURCE_CONFIDENCE: f64 = 0.99;

/// Skip if YES ask is above this — no spread left to capture.
pub const MAX_ENTRY_YES_ASK: f64 = 0.97;

/// Skip if YES ask is below this AND outcome is YES — we'd be entering
/// against the market consensus AT this price, which means our truth
/// source disagrees with retail. Suspect the source.
pub const MIN_ENTRY_PRICE: f64 = 0.50;

/// Outcome learnt from the specified resolution source.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct KnownOutcome {
    pub outcome_up:           bool,        // true = YES won, false = NO won
    pub confidence:           f64,         // 1.0 = source has printed final
    pub source_timestamp_ms:  u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DecayCandidate {
    pub market_id:        String,
    pub condition_id:     String,
    pub yes_token_id:     String,
    pub no_token_id:      String,
    pub yes_ask:          f64,
    pub yes_bid:          f64,
    pub end_date_ms:      i64,
    pub source_url:       String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DecayDecision {
    pub market_id:     String,
    pub direction:     Direction,
    pub entry_price:   f64,
    pub expected_payoff: f64,
    pub gross_edge:    f64,
}

pub fn evaluate_resolution_decay(
    candidate: &DecayCandidate,
    outcome:   &KnownOutcome,
) -> Option<DecayDecision> {
    // Gate 1: source confidence must be high
    if outcome.confidence < MIN_SOURCE_CONFIDENCE {
        return None;
    }

    // Gate 2: market hasn't already resolved past usable spread
    if candidate.yes_ask <= 0.0 || candidate.yes_ask >= 1.0 {
        return None;
    }
    if candidate.yes_bid < 0.0 || candidate.yes_bid >= 1.0 {
        return None;
    }

    // Compute entry price on the side that won
    let (direction, entry_price) = if outcome.outcome_up {
        (Direction::Up, candidate.yes_ask)
    } else {
        // Buying NO at touch = paying (1 - yes_bid)
        let no_ask = 1.0 - candidate.yes_bid;
        (Direction::Down, no_ask)
    };

    // Gate 3: entry must be inside the profitable band
    if entry_price > MAX_ENTRY_YES_ASK {
        return None;                       // no spread left
    }
    if entry_price < MIN_ENTRY_PRICE {
        return None;                       // suspiciously cheap; trust the market
    }

    let expected_payoff = 1.0;
    let gross_edge = expected_payoff - entry_price;

    Some(DecayDecision {
        market_id:       candidate.market_id.clone(),
        direction,
        entry_price,
        expected_payoff,
        gross_edge,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cand(yes_ask: f64, yes_bid: f64) -> DecayCandidate {
        DecayCandidate {
            market_id:    "0xmarket".into(),
            condition_id: "0xcond".into(),
            yes_token_id: "yes".into(),
            no_token_id:  "no".into(),
            yes_ask,
            yes_bid,
            end_date_ms:  1_000_000_000_000,
            source_url:   "https://espn.com/box".into(),
        }
    }

    fn outcome(up: bool, conf: f64) -> KnownOutcome {
        KnownOutcome {
            outcome_up: up,
            confidence: conf,
            source_timestamp_ms: 999_000_000_000,
        }
    }

    #[test]
    fn happy_path_yes_wins_market_priced_low_fires() {
        let c = cand(0.92, 0.90);
        let o = outcome(true, 1.0);
        let d = evaluate_resolution_decay(&c, &o).expect("fires");
        assert_eq!(d.direction, Direction::Up);
        assert!((d.entry_price - 0.92).abs() < 1e-9);
        assert!((d.gross_edge - 0.08).abs() < 1e-9);
    }

    #[test]
    fn happy_path_no_wins_market_priced_low_fires() {
        // YES bid 0.10 → NO ask = 0.90. NO wins → buy NO at 0.90
        let c = cand(0.12, 0.10);
        let o = outcome(false, 1.0);
        let d = evaluate_resolution_decay(&c, &o).expect("fires");
        assert_eq!(d.direction, Direction::Down);
        assert!((d.entry_price - 0.90).abs() < 1e-9);
    }

    #[test]
    fn low_source_confidence_rejected() {
        let c = cand(0.92, 0.90);
        let o = outcome(true, 0.95);   // below 0.99 floor
        assert!(evaluate_resolution_decay(&c, &o).is_none());
    }

    #[test]
    fn entry_too_high_rejected() {
        let c = cand(0.98, 0.97);
        let o = outcome(true, 1.0);
        assert!(evaluate_resolution_decay(&c, &o).is_none());
    }

    #[test]
    fn entry_too_low_rejected_yes_wins() {
        // YES wins but market is at 0.40 — distrust the truth source
        let c = cand(0.40, 0.38);
        let o = outcome(true, 1.0);
        assert!(evaluate_resolution_decay(&c, &o).is_none());
    }

    #[test]
    fn entry_too_low_rejected_no_wins() {
        // NO wins but NO ask is 0.30 (YES bid = 0.70) → too cheap
        let c = cand(0.72, 0.70);
        let o = outcome(false, 1.0);
        assert!(evaluate_resolution_decay(&c, &o).is_none());
    }

    #[test]
    fn invalid_yes_ask_rejected() {
        let c = cand(0.0, 0.0);
        let o = outcome(true, 1.0);
        assert!(evaluate_resolution_decay(&c, &o).is_none());
        let c2 = cand(1.0, 1.0);
        assert!(evaluate_resolution_decay(&c2, &o).is_none());
    }

    #[test]
    fn boundary_confidence_passes() {
        let c = cand(0.92, 0.90);
        let o = outcome(true, MIN_SOURCE_CONFIDENCE);
        assert!(evaluate_resolution_decay(&c, &o).is_some());
    }

    #[test]
    fn boundary_max_entry_passes() {
        let c = cand(MAX_ENTRY_YES_ASK, MAX_ENTRY_YES_ASK - 0.01);
        let o = outcome(true, 1.0);
        assert!(evaluate_resolution_decay(&c, &o).is_some());
    }

    #[test]
    fn gross_edge_calculated_correctly() {
        let c = cand(0.85, 0.83);
        let o = outcome(true, 1.0);
        let d = evaluate_resolution_decay(&c, &o).unwrap();
        assert!((d.gross_edge - 0.15).abs() < 1e-9);
        assert!((d.expected_payoff - 1.0).abs() < 1e-9);
    }
}
