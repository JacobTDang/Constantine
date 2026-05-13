// Strategy 03 — Whale-fade signal layer.
//
// Mirror image of whale_follow.rs. Reads `data/whale_fade_trades.jsonl`
// (written by scripts/whale_fade_tracker.py, sidecar to be wired in
// Phase 3) and emits a CONTRARIAN signal whenever a loser-cohort wallet
// makes a fresh BUY large enough to matter.
//
// Fade logic: whale buys outcome X at price p; we buy the opposite
// outcome at (1 - p) + tick. Hold until +3 ticks favorable, -2 ticks
// stop, or 48h timeout.

use serde::{Deserialize, Serialize};

use crate::signals::Direction;

/// Minimum loser-whale trade size to fade. Smaller trades are noise.
pub const FADE_MIN_SIZE_USD: f64 = 100.0;

/// Trades older than this are stale; the fade window has likely closed.
pub const FADE_MAX_AGE_SECS: u64 = 300;

/// Don't fade if the whale's fill price is outside this band — the
/// edge is in 0.20-0.80 where reversal still has headroom.
pub const FADE_MIN_PRICE: f64 = 0.20;
pub const FADE_MAX_PRICE: f64 = 0.80;

/// One trade from the loser-cohort wallet, as written by the sidecar.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LoserWhaleTrade {
    pub trade_id:       String,
    pub wallet:         String,
    pub market_id:      String,
    pub yes_token_id:   String,
    pub no_token_id:    String,
    pub side:           String,    // "BUY" / "SELL"
    /// 0 = YES side bought, 1 = NO side bought
    pub outcome_index:  i32,
    pub price:          f64,
    pub size_shares:    f64,
    pub size_usd:       f64,
    pub ts_ms:          u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WhaleFadeDecision {
    pub market_id:        String,
    /// Direction WE take — opposite of the whale
    pub direction:        Direction,
    /// Price we'd post our fade limit at
    pub fade_entry_price: f64,
    pub whale_price:      f64,
    pub age_secs:         u64,
    pub size_usd:         f64,
}

/// Evaluate a fresh loser-whale trade and decide whether to fade it.
pub fn evaluate_whale_fade(
    trade:  &LoserWhaleTrade,
    now_ms: u64,
) -> Option<WhaleFadeDecision> {
    // Gate 1: only BUY trades — sells are exit signals, not fadable
    if trade.side != "BUY" {
        return None;
    }

    // Gate 2: outcome index must be 0 or 1
    if trade.outcome_index != 0 && trade.outcome_index != 1 {
        return None;
    }

    // Gate 3: size floor — small trades don't move the price enough to fade
    if trade.size_usd < FADE_MIN_SIZE_USD {
        return None;
    }

    // Gate 4: price band — avoid deep ITM/OTM where reversal is limited
    if trade.price < FADE_MIN_PRICE || trade.price > FADE_MAX_PRICE {
        return None;
    }

    // Gate 5: staleness
    let age_ms = now_ms.saturating_sub(trade.ts_ms);
    let age_secs = age_ms / 1000;
    if age_secs > FADE_MAX_AGE_SECS {
        return None;
    }

    // Compute fade direction (opposite of whale) and our entry price
    let direction = if trade.outcome_index == 0 {
        Direction::Down       // whale bought YES → we buy NO
    } else {
        Direction::Up         // whale bought NO  → we buy YES
    };
    let fade_entry_price = (1.0 - trade.price) + 0.01;
    if fade_entry_price <= 0.0 || fade_entry_price >= 1.0 {
        return None;
    }

    Some(WhaleFadeDecision {
        market_id:        trade.market_id.clone(),
        direction,
        fade_entry_price,
        whale_price:      trade.price,
        age_secs,
        size_usd:         trade.size_usd,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(side: &str, idx: i32, price: f64, size_usd: f64, age_ms: u64) -> LoserWhaleTrade {
        LoserWhaleTrade {
            trade_id:      "tx1".into(),
            wallet:        "0xabc".into(),
            market_id:     "0xmarket".into(),
            yes_token_id:  "yes".into(),
            no_token_id:   "no".into(),
            side:          side.into(),
            outcome_index: idx,
            price,
            size_shares:   size_usd / price,
            size_usd,
            ts_ms:         1_000_000_000_000 - age_ms,
        }
    }

    const NOW: u64 = 1_000_000_000_000;

    #[test]
    fn happy_path_yes_buy_fade_to_no() {
        let tr = t("BUY", 0, 0.40, 500.0, 10_000);
        let d = evaluate_whale_fade(&tr, NOW).expect("should fade");
        assert_eq!(d.direction, Direction::Down);
        assert!((d.fade_entry_price - 0.61).abs() < 1e-9);
        assert_eq!(d.age_secs, 10);
    }

    #[test]
    fn happy_path_no_buy_fade_to_yes() {
        let tr = t("BUY", 1, 0.30, 500.0, 0);
        let d = evaluate_whale_fade(&tr, NOW).expect("should fade");
        assert_eq!(d.direction, Direction::Up);
        assert!((d.fade_entry_price - 0.71).abs() < 1e-9);
    }

    #[test]
    fn sell_trade_not_fadable() {
        let tr = t("SELL", 0, 0.40, 500.0, 0);
        assert!(evaluate_whale_fade(&tr, NOW).is_none());
    }

    #[test]
    fn small_size_rejected() {
        let tr = t("BUY", 0, 0.40, 50.0, 0);    // below $100 floor
        assert!(evaluate_whale_fade(&tr, NOW).is_none());
    }

    #[test]
    fn deep_otm_rejected() {
        let tr = t("BUY", 0, 0.10, 500.0, 0);   // below 0.20
        assert!(evaluate_whale_fade(&tr, NOW).is_none());
        let tr2 = t("BUY", 0, 0.85, 500.0, 0);  // above 0.80
        assert!(evaluate_whale_fade(&tr2, NOW).is_none());
    }

    #[test]
    fn stale_trade_rejected() {
        let tr = t("BUY", 0, 0.40, 500.0, 400_000);   // 400s, > 300s cap
        assert!(evaluate_whale_fade(&tr, NOW).is_none());
    }

    #[test]
    fn invalid_outcome_index_rejected() {
        let tr = t("BUY", 5, 0.40, 500.0, 0);
        assert!(evaluate_whale_fade(&tr, NOW).is_none());
        let tr2 = t("BUY", -1, 0.40, 500.0, 0);
        assert!(evaluate_whale_fade(&tr2, NOW).is_none());
    }

    #[test]
    fn boundary_size_at_floor_passes() {
        let tr = t("BUY", 0, 0.40, FADE_MIN_SIZE_USD, 0);
        assert!(evaluate_whale_fade(&tr, NOW).is_some());
    }

    #[test]
    fn boundary_price_at_lower_band_passes() {
        let tr = t("BUY", 0, FADE_MIN_PRICE, 500.0, 0);
        assert!(evaluate_whale_fade(&tr, NOW).is_some());
    }

    #[test]
    fn fade_entry_price_correct_at_high_whale_price() {
        let tr = t("BUY", 0, 0.79, 500.0, 0);
        let d = evaluate_whale_fade(&tr, NOW).unwrap();
        assert!((d.fade_entry_price - 0.22).abs() < 1e-9);
    }

    #[test]
    fn age_exactly_at_cap_passes() {
        let tr = t("BUY", 0, 0.40, 500.0, FADE_MAX_AGE_SECS * 1000);
        assert!(evaluate_whale_fade(&tr, NOW).is_some());
    }
}
