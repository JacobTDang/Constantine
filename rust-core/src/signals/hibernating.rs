// EDGE-E — Hibernating market scanner.
//
// Scans Polymarket for low-volume markets close to resolution where the
// price is far from the actual outcome. Pure-Rust scanner: hits Gamma
// /events on a slow cadence (default 30 min), filters by:
//   - end_date_ms within next 14 days
//   - 24h volume below MAX_VOLUME_USD (the alpha hides in thin markets)
//   - YES price either deep in [0.02, 0.10] or [0.90, 0.98]
// and emits HibernatingCandidate rows.
//
// What this layer does NOT do (deferred):
//   - "Truth source" matching (sports box scores, news scrapers). Without
//     a truth source we can only flag candidates; trading them blindly is
//     equivalent to retail buying long-shots, which loses money on
//     average. So this scanner runs in OBSERVE mode -- it logs candidates
//     to data/db/signals.jsonl with strategy_tag = "hibernating" and
//     emits no SignalDecision::Oracle by default. Once a manual review
//     of the JSONL shows a winning pattern, we wire it in.
//
// The cache + scan loop pattern matches event_arb.rs.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;
use tokio::sync::RwLock;

/// How many days out the close must be to count as "near resolution".
pub const MAX_DAYS_TO_CLOSE: f64 = 14.0;
/// Minimum days out -- markets resolving within an hour are "ghost" (likely
/// already resolved server-side). Skip them.
pub const MIN_DAYS_TO_CLOSE: f64 = 0.04;       // ~1 hour

/// Skip anything liquid -- the alpha is in stale, thin markets.
pub const MAX_VOLUME_USD_24H: f64 = 5_000.0;

/// Edge thresholds. We flag when YES price is far from 0/1 -- i.e. the
/// market is treating an effectively-decided outcome as uncertain.
pub const DEEP_LOW_THRESHOLD:  f64 = 0.10;     // YES <= 0.10 = "long shot"
pub const DEEP_HIGH_THRESHOLD: f64 = 0.90;     // YES >= 0.90 = "near certain"

/// Don't flag if best ask is 0 or 1 (resolution lock).
pub const MIN_PRICE_FOR_TRADE: f64 = 0.02;
pub const MAX_PRICE_FOR_TRADE: f64 = 0.98;

#[derive(Debug, Clone)]
pub struct HibernatingCandidate {
    pub market_id:    String,
    pub condition_id: String,
    pub question:     String,
    pub yes_token_id: String,
    pub no_token_id:  String,
    pub end_date_ms:  i64,
    pub yes_ask:      f64,
    pub yes_bid:      f64,
    pub volume_24h:   f64,
    pub liquidity:    f64,
    pub side_to_take: SideToTake,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SideToTake {
    /// YES is at 0.90+ and looks like a near-certain win — buy YES.
    Yes,
    /// YES is at 0.10- and looks like a long shot — buy NO (= 1 - 0.10 = 0.90 reward).
    No,
}

impl SideToTake {
    pub fn as_str(&self) -> &'static str {
        match self {
            SideToTake::Yes => "yes",
            SideToTake::No  => "no",
        }
    }
}

#[derive(Debug, Default)]
pub struct HibernatingCache {
    pub candidates: RwLock<Vec<HibernatingCandidate>>,
}

impl HibernatingCache {
    pub fn new() -> Self { Self::default() }

    pub async fn replace(&self, fresh: Vec<HibernatingCandidate>) {
        *self.candidates.write().await = fresh;
    }

    pub async fn snapshot(&self) -> Vec<HibernatingCandidate> {
        self.candidates.read().await.clone()
    }

    pub async fn len(&self) -> usize {
        self.candidates.read().await.len()
    }
}

// ── Gamma response shape (subset) ──────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct GammaEventWire {
    #[serde(default)] markets: Vec<GammaMarketWire>,
}

#[derive(Debug, Deserialize)]
pub struct GammaMarketWire {
    #[serde(default)] id:           serde_json::Value,
    #[serde(default)] question:     Option<String>,
    #[serde(rename = "conditionId", default)] condition_id: String,
    #[serde(rename = "endDate",     default)] end_date:     Option<String>,
    #[serde(rename = "clobTokenIds", default)] clob_token_ids: Option<String>,
    #[serde(rename = "bestBid",     default)] best_bid:     serde_json::Value,
    #[serde(rename = "bestAsk",     default)] best_ask:     serde_json::Value,
    #[serde(default)]                          volume:       serde_json::Value,
    #[serde(default)]                          liquidity:    serde_json::Value,
}

fn coerce_f64(v: &serde_json::Value) -> Option<f64> {
    v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

fn iso_to_ms(iso: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(iso)
        .ok()
        .map(|dt| dt.with_timezone(&chrono::Utc).timestamp_millis())
}

/// Classify a market as a hibernating candidate, if any.
pub fn classify(m: &GammaMarketWire, now_ms: i64) -> Option<HibernatingCandidate> {
    let question = m.question.clone()?;
    if question.is_empty() { return None; }
    let end_iso = m.end_date.as_deref()?;
    let end_ms  = iso_to_ms(end_iso)?;
    let days_remaining = (end_ms - now_ms) as f64 / (24.0 * 3_600_000.0);
    if days_remaining < MIN_DAYS_TO_CLOSE || days_remaining > MAX_DAYS_TO_CLOSE {
        return None;
    }
    let yes_ask = coerce_f64(&m.best_ask)?;
    let yes_bid = coerce_f64(&m.best_bid).unwrap_or(yes_ask);
    if yes_ask < MIN_PRICE_FOR_TRADE || yes_ask > MAX_PRICE_FOR_TRADE {
        return None;
    }
    let volume = coerce_f64(&m.volume).unwrap_or(0.0);
    if volume > MAX_VOLUME_USD_24H { return None; }

    let side = if yes_ask <= DEEP_LOW_THRESHOLD {
        SideToTake::No
    } else if yes_ask >= DEEP_HIGH_THRESHOLD {
        SideToTake::Yes
    } else {
        return None;
    };

    let raw_clob = m.clob_token_ids.as_deref()?;
    let tokens: Vec<String> = serde_json::from_str(raw_clob).ok()?;
    if tokens.len() != 2 { return None; }

    let market_id = match &m.id {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        _ => return None,
    };

    Some(HibernatingCandidate {
        market_id,
        condition_id: m.condition_id.clone(),
        question,
        yes_token_id: tokens[0].clone(),
        no_token_id:  tokens[1].clone(),
        end_date_ms:  end_ms,
        yes_ask,
        yes_bid,
        volume_24h:   volume,
        liquidity:    coerce_f64(&m.liquidity).unwrap_or(0.0),
        side_to_take: side,
    })
}

/// Hit Gamma /events for active markets, return all hibernating candidates.
pub async fn fetch_candidates(http: &reqwest::Client) -> Result<Vec<HibernatingCandidate>> {
    let mut out = Vec::new();
    let now_ms = chrono::Utc::now().timestamp_millis();

    // Multiple offset pages -- Gamma caps at ~500 events/page; we want
    // wide coverage of low-volume markets.
    for offset in (0..2_000usize).step_by(200) {
        let url = format!(
            "https://gamma-api.polymarket.com/events?\
             active=true&closed=false&limit=200&offset={}&order=endDate&ascending=true",
            offset
        );
        let resp = http.get(&url).send().await
            .with_context(|| format!("gamma events offset={}", offset))?;
        if !resp.status().is_success() {
            tracing::warn!(status = %resp.status(),
                "gamma events non-2xx; stopping pagination");
            break;
        }
        let body = resp.text().await.context("gamma body read")?;
        let events: Vec<GammaEventWire> = match serde_json::from_str(&body) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "gamma parse failed");
                break;
            }
        };
        if events.is_empty() { break; }
        for e in events {
            for m in e.markets {
                if let Some(c) = classify(&m, now_ms) {
                    out.push(c);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Ok(out)
}

/// Spawnable scan loop. Refreshes the cache on `cadence` and logs the count.
pub async fn hibernating_scanner_loop(
    cache:   Arc<HibernatingCache>,
    cadence: Duration,
) {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .expect("reqwest client build (hibernating)");

    let mut tick = tokio::time::interval(cadence);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        match fetch_candidates(&http).await {
            Ok(candidates) => {
                let n = candidates.len();
                let yes_count = candidates.iter()
                    .filter(|c| c.side_to_take == SideToTake::Yes).count();
                let no_count  = candidates.iter()
                    .filter(|c| c.side_to_take == SideToTake::No).count();
                cache.replace(candidates).await;
                tracing::info!(n, yes_count, no_count,
                    "hibernating scanner refreshed");
            }
            Err(e) => tracing::warn!(error = %e, "hibernating scan failed"),
        }
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_market(
        yes_ask: f64,
        end_iso: &str,
        volume: f64,
        clob: &str,
    ) -> GammaMarketWire {
        GammaMarketWire {
            id:           json!("0xMARKET"),
            question:     Some("Will Foo win?".into()),
            condition_id: "0xCONDITION".into(),
            end_date:     Some(end_iso.into()),
            clob_token_ids: Some(clob.into()),
            best_bid:     json!(yes_ask - 0.005),
            best_ask:     json!(yes_ask),
            volume:       json!(volume),
            liquidity:    json!(volume * 0.5),
        }
    }

    #[test]
    fn classify_flags_high_yes_near_resolution() {
        // 7 days out, yes_ask = 0.95 (deep YES territory), low vol
        let now_ms = 1_700_000_000_000;
        let end_iso = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(
            now_ms + 7 * 24 * 3_600_000,
        ).unwrap().to_rfc3339();
        let m = make_market(0.95, &end_iso, 200.0, r#"["y","n"]"#);
        let c = classify(&m, now_ms).expect("should flag");
        assert_eq!(c.side_to_take, SideToTake::Yes);
        assert_eq!(c.yes_ask, 0.95);
    }

    #[test]
    fn classify_flags_low_yes_as_no_side() {
        let now_ms = 1_700_000_000_000;
        let end_iso = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(
            now_ms + 5 * 24 * 3_600_000,
        ).unwrap().to_rfc3339();
        let m = make_market(0.05, &end_iso, 200.0, r#"["y","n"]"#);
        let c = classify(&m, now_ms).expect("should flag");
        assert_eq!(c.side_to_take, SideToTake::No);
    }

    #[test]
    fn classify_skips_mid_priced_markets() {
        let now_ms = 1_700_000_000_000;
        let end_iso = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(
            now_ms + 5 * 24 * 3_600_000,
        ).unwrap().to_rfc3339();
        let m = make_market(0.50, &end_iso, 200.0, r#"["y","n"]"#);
        assert!(classify(&m, now_ms).is_none());
    }

    #[test]
    fn classify_skips_high_volume_markets() {
        let now_ms = 1_700_000_000_000;
        let end_iso = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(
            now_ms + 7 * 24 * 3_600_000,
        ).unwrap().to_rfc3339();
        let m = make_market(0.95, &end_iso, 1_000_000.0, r#"["y","n"]"#);
        assert!(classify(&m, now_ms).is_none(),
            "high-volume market shouldn't be flagged — alpha is gone");
    }

    #[test]
    fn classify_skips_far_out_markets() {
        let now_ms = 1_700_000_000_000;
        // 30 days out -- too far for the "near resolution" thesis
        let end_iso = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(
            now_ms + 30 * 24 * 3_600_000,
        ).unwrap().to_rfc3339();
        let m = make_market(0.95, &end_iso, 200.0, r#"["y","n"]"#);
        assert!(classify(&m, now_ms).is_none());
    }

    #[test]
    fn classify_skips_just_about_to_close() {
        let now_ms = 1_700_000_000_000;
        // 30 minutes out -- might be already resolved server-side
        let end_iso = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(
            now_ms + 30 * 60_000,
        ).unwrap().to_rfc3339();
        let m = make_market(0.95, &end_iso, 200.0, r#"["y","n"]"#);
        assert!(classify(&m, now_ms).is_none());
    }

    #[test]
    fn classify_skips_resolution_lock_prices() {
        let now_ms = 1_700_000_000_000;
        let end_iso = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(
            now_ms + 7 * 24 * 3_600_000,
        ).unwrap().to_rfc3339();
        // yes_ask = 0.99 — too close to lock to provide profit after fees
        let m = make_market(0.99, &end_iso, 200.0, r#"["y","n"]"#);
        assert!(classify(&m, now_ms).is_none());
    }

    #[test]
    fn classify_skips_malformed_token_ids() {
        let now_ms = 1_700_000_000_000;
        let end_iso = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(
            now_ms + 7 * 24 * 3_600_000,
        ).unwrap().to_rfc3339();
        let m = make_market(0.95, &end_iso, 200.0, "not-json");
        assert!(classify(&m, now_ms).is_none());
    }

    #[test]
    fn cache_replace_overwrites() {
        let cache = HibernatingCache::default();
        let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
        runtime.block_on(async {
            let c1 = HibernatingCandidate {
                market_id: "m1".into(),
                condition_id: "c1".into(),
                question: "?".into(),
                yes_token_id: "y".into(),
                no_token_id: "n".into(),
                end_date_ms: 0,
                yes_ask: 0.95, yes_bid: 0.94,
                volume_24h: 100.0, liquidity: 50.0,
                side_to_take: SideToTake::Yes,
            };
            cache.replace(vec![c1]).await;
            assert_eq!(cache.len().await, 1);
            cache.replace(vec![]).await;
            assert_eq!(cache.len().await, 0);
        });
    }

    #[test]
    fn side_to_take_str() {
        assert_eq!(SideToTake::Yes.as_str(), "yes");
        assert_eq!(SideToTake::No.as_str(),  "no");
    }
}
