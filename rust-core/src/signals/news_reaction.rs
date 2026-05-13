// Strategy 01 — News-reaction signal layer.
//
// Pure-logic evaluator that takes a classified news item (headline +
// LLM-predicted direction + confidence) plus the current Polymarket
// price for the matched market, and returns a NewsReactionDecision if
// the predicted edge clears all thresholds.
//
// The classifier itself runs in a sidecar (Strategy 01 spec: scripts/
// news_classifier.py or equivalent). This module assumes the
// classifier has produced a NewsClassification struct and the runner
// has freshly fetched the market's touch price.
//
// Why pure-logic-only: the runner's existing risk gates handle
// cooldown, sizing, and kill-switch. We only decide IF to fire.

use serde::{Deserialize, Serialize};

use crate::signals::Direction;

/// Minimum LLM confidence before we trust the classification.
pub const MIN_CONFIDENCE: f64 = 0.70;

/// Minimum predicted price shift (in absolute pp) before we trade.
/// Below 3pp, fees + slippage erase the edge.
pub const MIN_PREDICTED_SHIFT_PP: f64 = 3.0;

/// Minimum edge between predicted fair value and current touch price.
/// Distinct from predicted shift: even if the LLM predicts a 10pp move,
/// if the market has already moved 9pp we have only 1pp of edge left.
pub const MIN_EDGE: f64 = 0.03;

/// News items older than this are stale — price has likely already
/// incorporated the information.
pub const MAX_AGE_SECS: u64 = 600;          // 10 minutes

/// One classified news item from the sidecar pipeline.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NewsClassification {
    pub headline:           String,
    pub source_url:         String,
    pub published_at_ms:    u64,
    /// Polymarket condition_id the classifier matched against
    pub market_id:          String,
    /// Direction the headline implies for this market (Up = YES, Down = NO)
    pub direction:          Direction,
    /// Classifier's predicted *fair value* of the relevant side after the
    /// market fully prices the news, in [0, 1].
    pub predicted_fair:     f64,
    /// Classifier's confidence in this prediction, in [0, 1].
    pub confidence:         f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewsReactionDecision {
    pub market_id:    String,
    pub direction:    Direction,
    pub predicted_fair: f64,
    pub market_price: f64,
    pub edge:         f64,
    pub age_secs:     u64,
}

/// Evaluate a single news classification against the current touch.
///
/// `market_price` is the relevant side's ask: if direction is Up, pass
/// the YES ask; if Down, pass the NO ask (= 1 - YES bid).
pub fn evaluate_news_reaction(
    classification: &NewsClassification,
    market_price:   f64,
    now_ms:         u64,
) -> Option<NewsReactionDecision> {
    // Gate 1: confidence
    if classification.confidence < MIN_CONFIDENCE {
        return None;
    }

    // Gate 2: predicted shift size — distance between predicted fair and
    // a neutral 0.5 baseline. Avoids firing on micro-edge classifications.
    let predicted_shift_pp = (classification.predicted_fair - 0.5).abs() * 100.0;
    if predicted_shift_pp < MIN_PREDICTED_SHIFT_PP {
        return None;
    }

    // Gate 3: usable edge — predicted_fair vs current market_price.
    // For Up signals, we want predicted_fair > market_price (room to rise).
    // For Down signals, we want predicted_fair < market_price (room to fall).
    let edge = match classification.direction {
        Direction::Up   => classification.predicted_fair - market_price,
        Direction::Down => market_price - classification.predicted_fair,
    };
    if edge < MIN_EDGE {
        return None;
    }

    // Gate 4: staleness
    let age_ms = now_ms.saturating_sub(classification.published_at_ms);
    let age_secs = age_ms / 1000;
    if age_secs > MAX_AGE_SECS {
        return None;
    }

    // Gate 5: prices must be inside [0.02, 0.98] for the touch to make sense
    if market_price <= 0.02 || market_price >= 0.98 {
        return None;
    }
    if classification.predicted_fair <= 0.02 || classification.predicted_fair >= 0.98 {
        return None;
    }

    Some(NewsReactionDecision {
        market_id:      classification.market_id.clone(),
        direction:      classification.direction,
        predicted_fair: classification.predicted_fair,
        market_price,
        edge,
        age_secs,
    })
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn clf(direction: Direction, predicted: f64, conf: f64, age_ms: u64) -> NewsClassification {
        NewsClassification {
            headline:        "Fed cuts rates 50bps".into(),
            source_url:      "https://example.com/article".into(),
            published_at_ms: 1_000_000_000_000 - age_ms,
            market_id:       "0xmarket".into(),
            direction,
            predicted_fair:  predicted,
            confidence:      conf,
        }
    }

    const NOW: u64 = 1_000_000_000_000;

    #[test]
    fn happy_path_up_signal_fires() {
        let c = clf(Direction::Up, 0.75, 0.85, 30_000);
        let d = evaluate_news_reaction(&c, 0.60, NOW).expect("should fire");
        assert_eq!(d.direction, Direction::Up);
        assert!((d.edge - 0.15).abs() < 1e-9);
        assert_eq!(d.age_secs, 30);
    }

    #[test]
    fn happy_path_down_signal_fires() {
        let c = clf(Direction::Down, 0.20, 0.80, 60_000);
        let d = evaluate_news_reaction(&c, 0.50, NOW).expect("should fire");
        assert_eq!(d.direction, Direction::Down);
        assert!((d.edge - 0.30).abs() < 1e-9);
    }

    #[test]
    fn low_confidence_rejected() {
        let c = clf(Direction::Up, 0.80, 0.50, 0);  // below 0.70 floor
        assert!(evaluate_news_reaction(&c, 0.50, NOW).is_none());
    }

    #[test]
    fn small_predicted_shift_rejected() {
        // predicted 0.52 — only 2pp from neutral, below 3pp floor
        let c = clf(Direction::Up, 0.52, 0.90, 0);
        assert!(evaluate_news_reaction(&c, 0.40, NOW).is_none());
    }

    #[test]
    fn already_priced_in_rejected() {
        // Predicted 0.75, market already at 0.74 → edge = 1pp < 3pp floor
        let c = clf(Direction::Up, 0.75, 0.90, 0);
        assert!(evaluate_news_reaction(&c, 0.74, NOW).is_none());
    }

    #[test]
    fn stale_headline_rejected() {
        // 700s old, MAX_AGE_SECS = 600
        let c = clf(Direction::Up, 0.80, 0.90, 700_000);
        assert!(evaluate_news_reaction(&c, 0.50, NOW).is_none());
    }

    #[test]
    fn extreme_price_rejected() {
        let c = clf(Direction::Up, 0.99, 0.90, 0);  // fair too close to 1
        assert!(evaluate_news_reaction(&c, 0.50, NOW).is_none());
        let c2 = clf(Direction::Up, 0.80, 0.90, 0);
        assert!(evaluate_news_reaction(&c2, 0.99, NOW).is_none());  // market at cap
    }

    #[test]
    fn down_direction_edge_calc_correct() {
        // Down signal — predicted 0.20, market currently 0.45 → edge = 0.25
        let c = clf(Direction::Down, 0.20, 0.90, 0);
        let d = evaluate_news_reaction(&c, 0.45, NOW).expect("should fire");
        assert!((d.edge - 0.25).abs() < 1e-9);
    }

    #[test]
    fn down_signal_with_price_below_fair_rejected() {
        // Down signal but market already below predicted fair → negative edge
        let c = clf(Direction::Down, 0.40, 0.90, 0);
        assert!(evaluate_news_reaction(&c, 0.30, NOW).is_none());
    }

    #[test]
    fn boundary_confidence_passes() {
        let c = clf(Direction::Up, 0.80, MIN_CONFIDENCE, 0);
        assert!(evaluate_news_reaction(&c, 0.50, NOW).is_some());
    }

    #[test]
    fn boundary_edge_just_below_rejected() {
        let c = clf(Direction::Up, 0.55, 0.90, 0);  // predicted 0.55, market 0.5249 → edge ~0.0251
        assert!(evaluate_news_reaction(&c, 0.5249, NOW).is_none());
    }
}
