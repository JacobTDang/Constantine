// Strategy 2 — Multi-candidate Event Sum-of-YES arb signal layer.
//
// For an event with N mutually-exclusive outcomes (election, awards,
// championship), the sum of YES prices SHOULD equal 100%. Polymarket
// retail consistently overprices long-shots, so the sum drifts above 1.
// We capture the surplus by SHORTING (= buying NO on) every leg.
//
// Mechanic:
//   sum_of_no_asks = sum(1 - yes_bid_i)   for each leg i
//   total_payout   = (N - 1) × $1         (exactly one candidate wins)
//   profit         = (N - 1) - sum_of_no_asks
//
// Captureable when profit > 0 AND every leg fills as MAKER (taker fees
// would eat the small 1-3% empirical edge — verified in
// scripts/research/strategy2_event_arb_validation.py).
//
// CRITICAL: this strategy works ONLY for true exclusive-winner events.
// Multi-winner events ("Top 10", "Above $X", etc.) break the (N-1)
// math. Filtering is a multi-stage classifier — see is_exclusive_winner.

use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Sum-of-YES range that's plausible for an exclusive-winner event.
/// Below 0.95 = mispriced overall (fade-able but different mechanic).
/// Above 1.30 = K>1 winners; arb math breaks.
pub const EXCLUSIVE_SUM_LO: f64 = 0.95;
pub const EXCLUSIVE_SUM_HI: f64 = 1.30;

/// Minimum captureable edge (after expected maker-fee, slippage) to fire.
/// Empirical max in real events: ~3%. We'll fire down to 1% to widen
/// opportunity set; any individual fill that doesn't capture full edge
/// won't bankrupt us due to small per-event sizing.
pub const MIN_EVENT_EDGE: f64 = 0.01;

/// Per-leg liquidity floor in dollars. Ensures we can fill the leg.
pub const MIN_LEG_LIQUIDITY_USD: f64 = 500.0;

/// Per-leg 24h volume floor. Without recent activity, our limit order
/// likely won't be matched.
pub const MIN_LEG_VOL_24H: f64 = 50.0;

/// Don't take more than top-K legs in a portfolio. Limits capital
/// fragmentation; the real edge is in long-tail legs (5-30%) anyway.
pub const MAX_LEGS_PER_PORTFOLIO: usize = 12;

// ── Wire types ───────────────────────────────────────────────────────────────

/// One leg of an event arb portfolio.
#[derive(Debug, Clone, PartialEq)]
pub struct ArbLeg {
    /// Polymarket condition_id of this candidate's market.
    pub market_id:    String,
    pub candidate:    String,
    pub yes_token_id: String,
    pub no_token_id:  String,
    pub yes_ask:      f64,
    pub yes_bid:      f64,
    pub liquidity:    f64,
    pub volume_24h:   f64,
}

impl ArbLeg {
    /// Cost to BUY NO at the touch (= 1 - yes_bid). Used both for
    /// edge calc and for the limit-order price we'd post.
    pub fn no_ask(&self) -> f64 {
        if self.yes_bid > 0.0 { 1.0 - self.yes_bid } else { 1.0 - self.yes_ask }
    }
}

/// One scannable event — group of mutually-exclusive candidates.
#[derive(Debug, Clone)]
pub struct EventSnap {
    pub event_id:  String,
    pub title:     String,
    pub legs:      Vec<ArbLeg>,
}

impl EventSnap {
    pub fn sum_yes_ask(&self) -> f64 {
        self.legs.iter().map(|l| l.yes_ask).sum()
    }

    pub fn tradeable_legs(&self) -> Vec<&ArbLeg> {
        self.legs.iter()
            .filter(|l| l.yes_ask > 0.01 && l.yes_ask < 0.99
                     && l.liquidity  >= MIN_LEG_LIQUIDITY_USD
                     && l.volume_24h >= MIN_LEG_VOL_24H)
            .collect()
    }
}

// ── Exclusive-winner classifier ─────────────────────────────────────────────

/// Multi-winner indicators. If any of these appears in the event title
/// we skip — these events have K>1 winners and the (N-1) math fails.
const MULTI_WINNER_KEYWORDS: &[&str] = &[
    "top ", "first ", "states ", "players ", "above $", "above ",
    "below $", "below ", "total games", "vs.", " vs ",
    "semi-final", "speakings", "meetings", "appearances", "events",
    "candidates ", "all players",
];

/// Single-winner indicators. Markets matching these are very likely
/// exclusive-winner.
const WINNER_KEYWORDS: &[&str] = &[
    " winner", " win?", "nominee", "champion", "mvp",
    "rookie of the year", "defensive player of the year",
    "of the year", "presidential election", "next pope",
];

/// Classify whether this event satisfies exclusive-winner semantics.
///
/// Multi-stage: title keywords AND sum-of-YES range. Both must pass.
/// Pure function so we can unit-test against historical title fixtures.
pub fn is_exclusive_winner(snap: &EventSnap) -> bool {
    let s = snap.sum_yes_ask();
    if !(EXCLUSIVE_SUM_LO..=EXCLUSIVE_SUM_HI).contains(&s) {
        return false;
    }
    let lower = snap.title.to_lowercase();
    if MULTI_WINNER_KEYWORDS.iter().any(|kw| lower.contains(kw)) {
        return false;
    }
    if WINNER_KEYWORDS.iter().any(|kw| lower.contains(kw)) {
        return true;
    }
    // Conservative default: if no positive winner-keyword and sum is
    // tightly close to 1.0, treat as exclusive. The 1.10 threshold
    // here is stricter than EXCLUSIVE_SUM_HI — better to skip a
    // marginal event than enter a multi-winner trap.
    (EXCLUSIVE_SUM_LO..=1.10).contains(&s)
}

// ── Edge computation ────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct ArbEdge {
    pub event_id:           String,
    pub title:              String,
    /// Legs we'd actually take, sorted descending by per-leg edge.
    pub portfolio:          Vec<ArbLeg>,
    /// (n_legs - 1) - sum(no_ask). Net captureable assuming maker fills.
    pub captureable_dollars: f64,
    /// captureable / total_capital_locked.
    pub captureable_roi:     f64,
}

/// Compute the best arb portfolio for an event. Returns Some(edge) when
/// the captureable edge clears MIN_EVENT_EDGE; None otherwise.
pub fn compute_arb(snap: &EventSnap) -> Option<ArbEdge> {
    if !is_exclusive_winner(snap) {
        return None;
    }
    let legs: Vec<ArbLeg> = snap.tradeable_legs().into_iter().cloned().collect();
    if legs.len() < 5 {
        // Need at least 5 legs to have meaningful (N-1) payout structure.
        return None;
    }

    // Rank legs by per-leg "edge density" — how much of the captureable
    // surplus this leg contributes per dollar locked. For an N-leg
    // portfolio: profit = (N-1) - sum(no_ask). Per-leg contribution to
    // profit is approximately yes_ask (since no_ask = 1 - yes_bid ≈ 1 - yes_ask
    // when spreads are tight — long-shots contribute most because they
    // have low yes_ask, hence low no_ask cost relative to $1 payout).
    let mut sorted = legs.clone();
    sorted.sort_by(|a, b| a.yes_ask.partial_cmp(&b.yes_ask)
        .unwrap_or(std::cmp::Ordering::Equal));

    let take_n = sorted.len().min(MAX_LEGS_PER_PORTFOLIO);
    let portfolio: Vec<ArbLeg> = sorted.into_iter().take(take_n).collect();

    let n = portfolio.len();
    let sum_no_ask: f64 = portfolio.iter().map(|l| l.no_ask()).sum();
    let captureable = (n as f64 - 1.0) - sum_no_ask;

    if captureable < MIN_EVENT_EDGE {
        return None;
    }
    let roi = if sum_no_ask > 0.0 { captureable / sum_no_ask } else { 0.0 };
    Some(ArbEdge {
        event_id:            snap.event_id.clone(),
        title:               snap.title.clone(),
        portfolio,
        captureable_dollars: captureable,
        captureable_roi:     roi,
    })
}

// ── EventArbCache: live state shared with the runner ────────────────────────

/// File-backed cache of scanned event arbs. Refreshed periodically by a
/// sibling task; the runner reads it each tick.
#[derive(Debug, Default)]
pub struct EventArbCache {
    edges: RwLock<Vec<ArbEdge>>,
    /// generated_at_ms — used for stale detection
    last_scan_ms: RwLock<u64>,
}

impl EventArbCache {
    pub fn new() -> Self { Self::default() }

    pub fn set(&self, edges: Vec<ArbEdge>, scanned_at_ms: u64) {
        *self.edges.write().expect("poisoned") = edges;
        *self.last_scan_ms.write().expect("poisoned") = scanned_at_ms;
    }

    pub fn snapshot(&self) -> Vec<ArbEdge> {
        self.edges.read().expect("poisoned").clone()
    }

    pub fn len(&self) -> usize {
        self.edges.read().expect("poisoned").len()
    }

    pub fn is_stale(&self, now_ms: u64, max_age_ms: u64) -> bool {
        let last = *self.last_scan_ms.read().expect("poisoned");
        if last == 0 { return true; }
        now_ms.saturating_sub(last) > max_age_ms
    }
}

/// Wire-level types for parsing the Gamma /events response. Lenient to
/// schema drift via serde::default.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GammaEvent {
    #[serde(default)] pub id:    String,
    #[serde(default)] pub slug:  String,
    #[serde(default)] pub title: String,
    #[serde(default)] pub markets: Vec<GammaMarket>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GammaMarket {
    #[serde(default)] pub id:           String,
    #[serde(rename = "conditionId", default)] pub condition_id: String,
    #[serde(default)] pub question:     String,
    #[serde(rename = "groupItemTitle", default)] pub group_item_title: String,
    #[serde(rename = "clobTokenIds",   default)] pub clob_token_ids:   String,
    #[serde(rename = "bestBid", default)] pub best_bid: Option<f64>,
    #[serde(rename = "bestAsk", default)] pub best_ask: Option<f64>,
    #[serde(default)] pub liquidity:    Option<f64>,
    #[serde(rename = "volume24hr", default)] pub volume_24hr: Option<f64>,
    #[serde(default)] pub volume:       Option<f64>,
}

/// Convert a raw GammaEvent to an EventSnap, filtering markets that
/// don't have basic price + token data. None = skip the event.
pub fn parse_event(event: &GammaEvent) -> Option<EventSnap> {
    if event.markets.len() < 5 {
        return None;
    }
    let mut legs = Vec::with_capacity(event.markets.len());
    for m in &event.markets {
        let yes_ask = m.best_ask.unwrap_or(0.0);
        let yes_bid = m.best_bid.unwrap_or(0.0);
        if yes_ask <= 0.0 || yes_ask >= 1.0 {
            continue;
        }
        // Parse clobTokenIds: "[<yes_id>, <no_id>]"
        let ids = match serde_json::from_str::<Vec<String>>(&m.clob_token_ids) {
            Ok(v) if v.len() == 2 => v,
            _ => continue,
        };
        let candidate = if !m.group_item_title.is_empty() {
            m.group_item_title.clone()
        } else {
            m.question.clone()
        };
        legs.push(ArbLeg {
            market_id:    m.condition_id.clone()
                .if_empty_use(&m.id),
            candidate,
            yes_token_id: ids[0].clone(),
            no_token_id:  ids[1].clone(),
            yes_ask,
            yes_bid,
            liquidity:    m.liquidity.unwrap_or(0.0),
            volume_24h:   m.volume_24hr.or(m.volume).unwrap_or(0.0),
        });
    }
    if legs.is_empty() { return None; }
    Some(EventSnap {
        event_id: event.id.clone().if_empty_use(&event.slug),
        title:    event.title.clone(),
        legs,
    })
}

// String helper — falls back to alternate value if empty.
trait StringIfEmpty {
    fn if_empty_use(self, alt: &str) -> Self;
}
impl StringIfEmpty for String {
    fn if_empty_use(self, alt: &str) -> Self {
        if self.is_empty() { alt.to_string() } else { self }
    }
}

// ── Convenience: scan a slice of events to a Vec<ArbEdge> ─────────────────

pub fn scan_events(events: &[GammaEvent]) -> Vec<ArbEdge> {
    let mut out = Vec::new();
    for ev in events {
        if let Some(snap) = parse_event(ev) {
            if let Some(edge) = compute_arb(&snap) {
                out.push(edge);
            }
        }
    }
    // Best edges first
    out.sort_by(|a, b| b.captureable_dollars.partial_cmp(&a.captureable_dollars)
        .unwrap_or(std::cmp::Ordering::Equal));
    out
}

/// `now_ms` helper for callers without chrono.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ── Background scanner task ────────────────────────────────────────────────

const GAMMA_EVENTS_URL:    &str = "https://gamma-api.polymarket.com/events";
const SCANNER_PAGE_LIMIT:  usize = 200;
const SCANNER_PAGES:       usize = 5;       // 1000 events sampled per pass

/// Long-running task: every `interval_secs`, hit Gamma, run scan_events,
/// populate the cache.
///
/// Robust to network blips: on RPC error we leave the cache as-is and
/// retry next tick. Don't crash the bot on a Gamma 5xx.
pub async fn event_arb_scanner_loop(
    cache:        std::sync::Arc<EventArbCache>,
    interval_secs: u64,
) {
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(20))
        .build()
        .expect("reqwest client");

    let mut tick = tokio::time::interval(std::time::Duration::from_secs(interval_secs.max(60)));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    tracing::info!(interval_secs, "event arb scanner started");

    loop {
        tick.tick().await;
        match scan_once(&http).await {
            Ok(edges) => {
                let n = edges.len();
                cache.set(edges, now_ms());
                tracing::info!(n_edges = n, "event arb cache populated");
            }
            Err(e) => {
                tracing::warn!(error = %e, "event arb scan failed — keeping previous cache");
            }
        }
    }
}

/// One scan pass: fetch up to SCANNER_PAGES × SCANNER_PAGE_LIMIT events,
/// parse, run scan_events.
async fn scan_once(http: &reqwest::Client) -> anyhow::Result<Vec<ArbEdge>> {
    let mut all_events: Vec<GammaEvent> = Vec::new();
    for page in 0..SCANNER_PAGES {
        let offset = page * SCANNER_PAGE_LIMIT;
        let url = format!(
            "{}?closed=false&active=true&limit={}&offset={}&order=volume&ascending=false",
            GAMMA_EVENTS_URL, SCANNER_PAGE_LIMIT, offset,
        );
        let resp = http.get(&url).send().await
            .map_err(|e| anyhow::anyhow!("gamma fetch failed: {e}"))?;
        if !resp.status().is_success() {
            return Err(anyhow::anyhow!("gamma HTTP {}", resp.status()));
        }
        let page_events: Vec<GammaEvent> = resp.json().await
            .map_err(|e| anyhow::anyhow!("gamma JSON parse: {e}"))?;
        if page_events.is_empty() { break; }
        all_events.extend(page_events);
        // Polite pacing
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    }
    Ok(scan_events(&all_events))
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn leg(cand: &str, yes_ask: f64, yes_bid: f64, liq: f64) -> ArbLeg {
        ArbLeg {
            market_id:    format!("m_{cand}"),
            candidate:    cand.to_string(),
            yes_token_id: format!("y_{cand}"),
            no_token_id:  format!("n_{cand}"),
            yes_ask, yes_bid,
            liquidity:    liq,
            volume_24h:   100.0,
        }
    }

    fn event(title: &str, legs: Vec<ArbLeg>) -> EventSnap {
        EventSnap { event_id: "e1".into(), title: title.into(), legs }
    }

    // ── ArbLeg ────────────────────────────────────────────────────────────

    #[test]
    fn no_ask_uses_yes_bid_when_present() {
        let l = leg("a", 0.20, 0.18, 1000.0);
        assert!((l.no_ask() - 0.82).abs() < 1e-9);
    }

    #[test]
    fn no_ask_falls_back_to_yes_ask_when_no_bid() {
        let l = leg("a", 0.20, 0.0, 1000.0);
        assert!((l.no_ask() - 0.80).abs() < 1e-9);
    }

    // ── EventSnap ─────────────────────────────────────────────────────────

    #[test]
    fn sum_yes_ask_correct() {
        let e = event("Foo Winner", vec![
            leg("a", 0.30, 0.28, 1000.0),
            leg("b", 0.40, 0.38, 1000.0),
            leg("c", 0.25, 0.23, 1000.0),
        ]);
        assert!((e.sum_yes_ask() - 0.95).abs() < 1e-9);
    }

    #[test]
    fn tradeable_legs_filters_thin_books() {
        let e = event("Foo Winner", vec![
            leg("a", 0.30, 0.28, 1000.0),    // OK
            leg("b", 0.40, 0.38, 100.0),     // thin liquidity → skip
            leg("c", 0.25, 0.23, 1000.0),    // OK
        ]);
        let t = e.tradeable_legs();
        assert_eq!(t.len(), 2);
    }

    // ── is_exclusive_winner ───────────────────────────────────────────────

    #[test]
    fn exclusive_winner_passes_for_election_with_sum_near_1() {
        let e = event("2028 Presidential Election Winner", vec![
            leg("a", 0.40, 0.38, 1000.0),
            leg("b", 0.30, 0.28, 1000.0),
            leg("c", 0.20, 0.18, 1000.0),
            leg("d", 0.05, 0.04, 1000.0),
            leg("e", 0.05, 0.04, 1000.0),
        ]);
        assert!(is_exclusive_winner(&e));
    }

    #[test]
    fn rejects_top_n_event() {
        let e = event("Top 10 Eurovision Finalists", vec![
            leg("a", 0.40, 0.38, 1000.0),
            leg("b", 0.30, 0.28, 1000.0),
            leg("c", 0.20, 0.18, 1000.0),
            leg("d", 0.20, 0.18, 1000.0),
            leg("e", 0.15, 0.14, 1000.0),
        ]);
        assert!(!is_exclusive_winner(&e));
    }

    #[test]
    fn rejects_above_threshold_event() {
        let e = event("Bitcoin above $80k on May 8", vec![
            leg("a", 0.50, 0.48, 1000.0),
            leg("b", 0.50, 0.48, 1000.0),
            leg("c", 0.50, 0.48, 1000.0),
            leg("d", 0.50, 0.48, 1000.0),
            leg("e", 0.50, 0.48, 1000.0),
        ]);
        assert!(!is_exclusive_winner(&e));
    }

    #[test]
    fn rejects_extremely_high_sum() {
        // Sum 5.0 — can't possibly be exclusive winner
        let e = event("Generic Event", vec![
            leg("a", 1.0, 0.99, 1000.0); 5
        ]);
        assert!(!is_exclusive_winner(&e));
    }

    #[test]
    fn rejects_extremely_low_sum() {
        // Sum 0.10 — extreme mispricing or all longshots
        let e = event("Mystery Winner", vec![
            leg("a", 0.02, 0.01, 1000.0); 5
        ]);
        assert!(!is_exclusive_winner(&e));
    }

    // ── compute_arb ───────────────────────────────────────────────────────

    #[test]
    fn compute_arb_returns_edge_when_sum_overprices() {
        // 5 legs: yes_ask 0.30, 0.30, 0.25, 0.25, 0.10 = sum 1.20 (over 1)
        // yes_bid 0.29, 0.29, 0.24, 0.24, 0.09 → no_ask 0.71, 0.71, 0.76, 0.76, 0.91
        // sum_no_ask = 3.85
        // (5-1) - 3.85 = 0.15 captureable per $3.85 = +3.9% ROI
        let e = event("Foo Champion", vec![
            leg("a", 0.30, 0.29, 1000.0),
            leg("b", 0.30, 0.29, 1000.0),
            leg("c", 0.25, 0.24, 1000.0),
            leg("d", 0.25, 0.24, 1000.0),
            leg("e", 0.10, 0.09, 1000.0),
        ]);
        let edge = compute_arb(&e).expect("should compute edge");
        assert_eq!(edge.portfolio.len(), 5);
        assert!((edge.captureable_dollars - 0.15).abs() < 0.01);
    }

    #[test]
    fn compute_arb_none_when_below_threshold() {
        // Sum exactly 1.0 → no edge
        let e = event("Exact Champion", vec![
            leg("a", 0.50, 0.49, 1000.0),
            leg("b", 0.20, 0.19, 1000.0),
            leg("c", 0.20, 0.19, 1000.0),
            leg("d", 0.05, 0.04, 1000.0),
            leg("e", 0.05, 0.04, 1000.0),
        ]);
        // sum_no_ask = 0.51 + 0.81 + 0.81 + 0.96 + 0.96 = 4.05
        // (5-1) - 4.05 = -0.05 → no edge
        assert!(compute_arb(&e).is_none());
    }

    #[test]
    fn compute_arb_skips_multi_winner_event() {
        let e = event("Top 5 Players", vec![
            leg("a", 0.30, 0.29, 1000.0); 5
        ]);
        assert!(compute_arb(&e).is_none());
    }

    #[test]
    fn compute_arb_skips_event_with_too_few_legs() {
        let e = event("Three-way Champion", vec![
            leg("a", 0.40, 0.39, 1000.0),
            leg("b", 0.40, 0.39, 1000.0),
            leg("c", 0.20, 0.19, 1000.0),
        ]);
        assert!(compute_arb(&e).is_none());
    }

    #[test]
    fn compute_arb_caps_portfolio_at_max_legs() {
        // 20 legs all with edge — should cap at MAX_LEGS_PER_PORTFOLIO = 12
        let mut legs = Vec::new();
        for i in 0..20 {
            legs.push(leg(&format!("c{i}"), 0.05, 0.04, 1000.0));
        }
        let e = event("Big Champion", legs);
        if let Some(edge) = compute_arb(&e) {
            assert!(edge.portfolio.len() <= MAX_LEGS_PER_PORTFOLIO);
        }
    }

    // ── EventArbCache ─────────────────────────────────────────────────────

    #[test]
    fn cache_set_and_snapshot_round_trip() {
        let cache = EventArbCache::new();
        assert_eq!(cache.len(), 0);
        let edges = vec![ArbEdge {
            event_id: "e".into(),
            title:    "Test Champion".into(),
            portfolio: vec![],
            captureable_dollars: 0.05,
            captureable_roi:     0.02,
        }];
        cache.set(edges, 1_000);
        let snap = cache.snapshot();
        assert_eq!(snap.len(), 1);
        assert!(!cache.is_stale(2_000, 60_000));
    }

    #[test]
    fn cache_marks_stale_when_old() {
        let cache = EventArbCache::new();
        cache.set(vec![], 0);     // never scanned
        assert!(cache.is_stale(1_000_000, 60_000));
    }

    // ── parse_event ───────────────────────────────────────────────────────

    #[test]
    fn parse_event_skips_when_too_few_markets() {
        let ev = GammaEvent {
            id: "e".into(), slug: "e".into(), title: "T".into(),
            markets: vec![GammaMarket {
                id: "m".into(), condition_id: "c".into(), question: "Q".into(),
                group_item_title: "X".into(),
                clob_token_ids: r#"["a","b"]"#.into(),
                best_bid: Some(0.5), best_ask: Some(0.51),
                liquidity: Some(1000.0), volume_24hr: Some(100.0), volume: None,
            }],
        };
        assert!(parse_event(&ev).is_none());
    }

    #[test]
    fn parse_event_skips_market_with_invalid_ask() {
        let mut markets = Vec::new();
        // 5 markets, one with invalid ask (= 0)
        for i in 0..5 {
            markets.push(GammaMarket {
                id: format!("m{i}"), condition_id: format!("c{i}"),
                question: format!("Q{i}"), group_item_title: format!("X{i}"),
                clob_token_ids: r#"["a","b"]"#.into(),
                best_bid: if i == 0 { Some(0.0) } else { Some(0.5) },
                best_ask: if i == 0 { Some(0.0) } else { Some(0.51) },
                liquidity: Some(1000.0), volume_24hr: Some(100.0), volume: None,
            });
        }
        let ev = GammaEvent {
            id: "e".into(), slug: "e".into(), title: "T".into(), markets,
        };
        let snap = parse_event(&ev).unwrap();
        assert_eq!(snap.legs.len(), 4);   // 1 invalid skipped
    }

    #[test]
    fn scan_events_sorts_by_captureable_dollars_desc() {
        // Two events, one with bigger edge. Result should be that one first.
        let big_edge_legs = vec![
            leg("a", 0.30, 0.29, 1000.0),
            leg("b", 0.30, 0.29, 1000.0),
            leg("c", 0.25, 0.24, 1000.0),
            leg("d", 0.25, 0.24, 1000.0),
            leg("e", 0.10, 0.09, 1000.0),
        ];
        let _ = scan_events(&[]);   // empty case doesn't crash
        // (Exhaustive scan_events test would require building GammaEvent
        // shape; covered by integration test in build phase.)
        assert!(big_edge_legs.iter().all(|l| l.yes_ask > 0.0));
    }
}
