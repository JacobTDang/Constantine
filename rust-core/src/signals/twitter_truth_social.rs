// Strategy 06 — Twitter / Truth Social signal layer.
//
// Layered on top of the Strategy 01 news pipeline. Reuses the same LLM
// classifier output shape but applies stricter thresholds: monitored-
// account posts have higher base reliability than scraped wire-news
// headlines, but they also propagate faster, so the fade-decay window
// is shorter.
//
// Wire shape: the sidecar (scripts/twitter_truth_social_tracker.py,
// wired in Phase 3) emits PostClassification rows containing the
// post text, the monitored account that authored it, the LLM-
// classified target market_id, and a predicted fair value with
// confidence.

use serde::{Deserialize, Serialize};

use crate::signals::Direction;

/// Tighter confidence floor than news pipeline (curated accounts are
/// higher-precision but require sharper discrimination on edge cases).
pub const MIN_CONFIDENCE: f64 = 0.75;

/// Higher predicted-shift bar than Strategy 01 to filter noise from
/// chatty accounts (Trump posts dozens of times per day; only the
/// big ones move markets).
pub const MIN_PREDICTED_SHIFT_PP: f64 = 4.0;

/// Minimum edge between predicted fair and current touch.
pub const MIN_EDGE: f64 = 0.03;

/// Posts older than this — other bots have already arbed them.
pub const MAX_AGE_SECS: u64 = 180;          // 3 minutes

/// Trusted account allowlist categories. Spec lists ~10 specific accounts;
/// keep enum small for future filtering. Sidecar tags each post with a
/// category so the Rust layer can apply category-specific thresholds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum AccountCategory {
    /// Trump on Truth Social or X — political markets, multi-min windows
    TrumpPolitical,
    /// Schefter, Shams, Rapoport, etc. — sports beat reporters
    SportsBeat,
    /// Timiraos (Fed) — macro markets
    MacroEconomic,
    /// Generic curated account (Reuters, AP, Bloomberg)
    Newswire,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PostClassification {
    pub post_id:            String,
    pub account_handle:     String,
    pub account_category:   AccountCategory,
    pub post_text_excerpt:  String,
    pub post_url:           String,
    pub posted_at_ms:       u64,
    pub market_id:          String,
    pub direction:          Direction,
    pub predicted_fair:     f64,
    pub confidence:         f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TwitterDecision {
    pub market_id:    String,
    pub direction:    Direction,
    pub account_category: AccountCategory,
    pub predicted_fair: f64,
    pub market_price: f64,
    pub edge:         f64,
    pub age_secs:     u64,
}

pub fn evaluate_twitter_signal(
    post:          &PostClassification,
    market_price:  f64,
    now_ms:        u64,
) -> Option<TwitterDecision> {
    // Sports beat in-play is unwinnable from US hardware — skip
    if post.account_category == AccountCategory::SportsBeat {
        // We DO accept SportsBeat for pre-game and trade-deadline content
        // but the Rust layer can't distinguish "in-play vs not" from the
        // PostClassification alone; the sidecar should set a flag, but
        // for now we apply a TIGHTER staleness window for sports.
        let age_ms = now_ms.saturating_sub(post.posted_at_ms);
        if age_ms / 1000 > 30 {
            return None;
        }
    }

    if post.confidence < MIN_CONFIDENCE {
        return None;
    }

    let predicted_shift_pp = (post.predicted_fair - 0.5).abs() * 100.0;
    if predicted_shift_pp < MIN_PREDICTED_SHIFT_PP {
        return None;
    }

    let edge = match post.direction {
        Direction::Up   => post.predicted_fair - market_price,
        Direction::Down => market_price - post.predicted_fair,
    };
    if edge < MIN_EDGE {
        return None;
    }

    let age_ms = now_ms.saturating_sub(post.posted_at_ms);
    let age_secs = age_ms / 1000;
    if age_secs > MAX_AGE_SECS {
        return None;
    }

    if market_price <= 0.02 || market_price >= 0.98 {
        return None;
    }
    if post.predicted_fair <= 0.02 || post.predicted_fair >= 0.98 {
        return None;
    }

    Some(TwitterDecision {
        market_id:        post.market_id.clone(),
        direction:        post.direction,
        account_category: post.account_category,
        predicted_fair:   post.predicted_fair,
        market_price,
        edge,
        age_secs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn post(cat: AccountCategory, predicted: f64, conf: f64, age_ms: u64, dir: Direction) -> PostClassification {
        PostClassification {
            post_id:           "p1".into(),
            account_handle:    "@realDonaldTrump".into(),
            account_category:  cat,
            post_text_excerpt: "Open the F— Strait".into(),
            post_url:          "https://truthsocial.com/x/y".into(),
            posted_at_ms:      1_000_000_000_000 - age_ms,
            market_id:         "0xmarket".into(),
            direction:         dir,
            predicted_fair:    predicted,
            confidence:        conf,
        }
    }

    const NOW: u64 = 1_000_000_000_000;

    #[test]
    fn happy_path_trump_post_fires() {
        let p = post(AccountCategory::TrumpPolitical, 0.75, 0.85, 30_000, Direction::Up);
        let d = evaluate_twitter_signal(&p, 0.55, NOW).expect("fires");
        assert!((d.edge - 0.20).abs() < 1e-9);
        assert_eq!(d.account_category, AccountCategory::TrumpPolitical);
    }

    #[test]
    fn low_confidence_rejected() {
        let p = post(AccountCategory::TrumpPolitical, 0.80, 0.70, 0, Direction::Up);
        assert!(evaluate_twitter_signal(&p, 0.50, NOW).is_none());
    }

    #[test]
    fn small_predicted_shift_rejected() {
        // 0.53 → only 3pp shift, MIN is 4pp
        let p = post(AccountCategory::TrumpPolitical, 0.53, 0.90, 0, Direction::Up);
        assert!(evaluate_twitter_signal(&p, 0.40, NOW).is_none());
    }

    #[test]
    fn stale_post_rejected() {
        let p = post(AccountCategory::TrumpPolitical, 0.80, 0.90, 200_000, Direction::Up);
        assert!(evaluate_twitter_signal(&p, 0.50, NOW).is_none());
    }

    #[test]
    fn sports_beat_tighter_age_window() {
        // 40s old — over 30s sports cap, would pass 180s general cap
        let p = post(AccountCategory::SportsBeat, 0.80, 0.90, 40_000, Direction::Up);
        assert!(evaluate_twitter_signal(&p, 0.50, NOW).is_none());
    }

    #[test]
    fn sports_beat_within_window_fires() {
        let p = post(AccountCategory::SportsBeat, 0.80, 0.90, 20_000, Direction::Up);
        assert!(evaluate_twitter_signal(&p, 0.50, NOW).is_some());
    }

    #[test]
    fn already_priced_in_rejected() {
        let p = post(AccountCategory::MacroEconomic, 0.75, 0.85, 0, Direction::Up);
        assert!(evaluate_twitter_signal(&p, 0.73, NOW).is_none());
    }

    #[test]
    fn down_signal_fires_correctly() {
        let p = post(AccountCategory::Newswire, 0.25, 0.85, 0, Direction::Down);
        let d = evaluate_twitter_signal(&p, 0.55, NOW).expect("fires");
        assert!((d.edge - 0.30).abs() < 1e-9);
        assert_eq!(d.direction, Direction::Down);
    }

    #[test]
    fn extreme_market_price_rejected() {
        // predicted_fair above the 0.98 ceiling
        let p = post(AccountCategory::MacroEconomic, 0.99, 0.85, 0, Direction::Up);
        assert!(evaluate_twitter_signal(&p, 0.50, NOW).is_none());
        // market price at the ceiling
        let p2 = post(AccountCategory::MacroEconomic, 0.80, 0.85, 0, Direction::Up);
        assert!(evaluate_twitter_signal(&p2, 0.99, NOW).is_none());
    }

    #[test]
    fn boundary_confidence_passes() {
        let p = post(AccountCategory::TrumpPolitical, 0.75, MIN_CONFIDENCE, 0, Direction::Up);
        assert!(evaluate_twitter_signal(&p, 0.50, NOW).is_some());
    }

    #[test]
    fn account_category_preserved_in_decision() {
        let p = post(AccountCategory::MacroEconomic, 0.80, 0.90, 0, Direction::Up);
        let d = evaluate_twitter_signal(&p, 0.50, NOW).unwrap();
        assert_eq!(d.account_category, AccountCategory::MacroEconomic);
    }
}
