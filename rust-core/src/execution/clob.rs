// Sprint 2 / J4 — Polymarket CLOB REST submission client.
//
// Hot-path responsibilities:
//   1. Convert a SignedOrder (from presign) → JSON wire format
//   2. Sign the request with HMAC-SHA256 L2 auth headers
//   3. POST to clob.polymarket.com/order with retry on transient 5xx
//   4. Parse the JSON response with strict schema validation
//   5. DRY_RUN short-circuit so we can run end-to-end with no live trades
//
// Polymarket order JSON format (from py-clob-client signing.py):
//   {
//     "order": {
//       "salt":          "<decimal>",
//       "maker":         "0x<40-hex>",
//       "signer":        "0x<40-hex>",
//       "taker":         "0x<40-hex>",
//       "tokenId":       "<decimal>",
//       "makerAmount":   "<decimal>",   // USDC micros
//       "takerAmount":   "<decimal>",   // token micros
//       "expiration":    "<decimal>",
//       "nonce":         "<decimal>",
//       "feeRateBps":    "<decimal>",
//       "side":          "BUY" | "SELL",
//       "signatureType": 0 | 1 | 2,
//       "signature":     "0x<130-hex>"   // r||s||v
//     },
//     "owner":     "<api_key UUID>",
//     "orderType": "GTC" | "GTD" | "FOK" | "FAK"
//   }

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::execution::auth::{auth_headers_now, ApiCreds};
use crate::execution::orders::{
    address_to_hex, u256_to_dec, SignedOrder,
};

pub const POLYMARKET_CLOB_BASE_URL: &str = "https://clob.polymarket.com";
pub const ORDER_PATH:                &str = "/order";
pub const ORDERS_BULK_PATH:          &str = "/orders";
pub const DATA_ORDERS_PATH:          &str = "/data/orders";
pub const DATA_TRADES_PATH:          &str = "/data/trades";
pub const CANCEL_MARKET_PATH:        &str = "/cancel-market-orders";
pub const BOOK_PATH:                 &str = "/book";
pub const REWARDS_CURRENT_PATH:      &str = "/rewards/markets/current";

/// Max orders Polymarket accepts per bulk submit (postOrders endpoint).
pub const MAX_BULK_ORDERS: usize = 15;

/// Polymarket time-in-force values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderType {
    /// Good-till-cancelled (rests on the book until filled or cancelled).
    Gtc,
    /// Good-till-date (uses the order's expiration field).
    Gtd,
    /// Fill-or-kill (atomic full fill or rejection).
    Fok,
    /// Fill-and-kill (partial fill OK, remainder cancelled).
    Fak,
}

impl OrderType {
    pub fn as_str(&self) -> &'static str {
        match self {
            OrderType::Gtc => "GTC",
            OrderType::Gtd => "GTD",
            OrderType::Fok => "FOK",
            OrderType::Fak => "FAK",
        }
    }
}

// ── Wire format ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
struct OrderWire {
    /// JSON number, NOT a string — matches rs-clob-client. Pre-sign generates
    /// u64 salts placed in the low 8 bytes of the 32-byte hash input, so the
    /// uint256 value always fits a u64.
    salt:           u64,
    maker:          String,
    signer:         String,
    taker:          String,
    #[serde(rename = "tokenId")]
    token_id:       String,
    #[serde(rename = "makerAmount")]
    maker_amount:   String,
    #[serde(rename = "takerAmount")]
    taker_amount:   String,
    expiration:     String,
    nonce:          String,
    #[serde(rename = "feeRateBps")]
    fee_rate_bps:   String,
    side:           String,
    #[serde(rename = "signatureType")]
    signature_type: u8,
    signature:      String,
}

#[derive(Debug, Clone, Serialize)]
struct SubmitBody {
    order:      OrderWire,
    owner:      String,
    #[serde(rename = "orderType")]
    order_type: String,
    /// Only emitted when set — Polymarket's API treats absence as "not post-only".
    #[serde(rename = "postOnly", skip_serializing_if = "Option::is_none")]
    post_only:  Option<bool>,
}

/// Schema-validated decode of the Polymarket /order response.
/// Field names track py-clob-client's ApiOrderResponse.
#[derive(Debug, Clone, Deserialize)]
pub struct ClobApiResponse {
    pub success: bool,
    #[serde(rename = "errorMsg", default)]
    pub error_msg: String,
    #[serde(rename = "orderID", default)]
    pub order_id: String,
    #[serde(default)]
    pub status: String,
    #[serde(rename = "transactionsHashes", default)]
    pub transaction_hashes: Vec<String>,
}

/// Outcome the strategy layer cares about — already collapses HTTP + JSON
/// + DRY_RUN paths into a single shape.
#[derive(Debug, Clone)]
pub struct SubmitOutcome {
    /// True iff Polymarket accepted the order.
    pub success:    bool,
    /// Polymarket-assigned order id (empty on failure or DRY_RUN).
    pub order_id:   String,
    /// Polymarket status string (e.g. "matched", "live").
    pub status:     String,
    /// Error message from the API on failure (or our retry exhaustion message).
    pub error_msg:  String,
    /// HTTP status code we observed (or 0 in DRY_RUN).
    pub http_status: u16,
    /// True iff this was a dry-run that short-circuited the HTTP call.
    pub dry_run:    bool,
    /// Number of attempts made (1 on first-try success, up to retry budget).
    pub attempts:   u32,
}

impl SubmitOutcome {
    fn dry_run() -> Self {
        Self {
            success:     true,
            order_id:    String::new(),
            status:      "dry_run".to_string(),
            error_msg:   String::new(),
            http_status: 0,
            dry_run:     true,
            attempts:    0,
        }
    }
}

// ── Client config ────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ClobConfig {
    pub base_url:         String,
    pub address:          String,    // wallet address, 0x-prefixed
    pub creds:            ApiCreds,
    pub dry_run:          bool,
    pub order_type:       OrderType,
    /// Optional post-only flag (rests on the book; rejects if it would cross).
    /// None = let Polymarket use its default behaviour.
    pub post_only:        Option<bool>,
    pub max_retries:      u32,       // total attempts = max_retries + 1
    pub initial_backoff_ms: u64,
    pub request_timeout: Duration,
}

impl ClobConfig {
    /// Sensible production defaults: FAK time-in-force (partial fill OK,
    /// remainder cancelled — right for short-window directional trades),
    /// 3 retries, 100ms initial backoff, 5s req timeout.
    pub fn new(
        address: impl Into<String>,
        creds:   ApiCreds,
        dry_run: bool,
    ) -> Self {
        Self {
            base_url:           POLYMARKET_CLOB_BASE_URL.to_string(),
            address:            address.into(),
            creds,
            dry_run,
            order_type:         OrderType::Fak,
            post_only:          None,
            max_retries:        3,
            initial_backoff_ms: 100,
            request_timeout:    Duration::from_secs(5),
        }
    }
}

// ── Client ───────────────────────────────────────────────────────────────────

pub struct ClobClient {
    cfg:  ClobConfig,
    http: reqwest::Client,
}

impl ClobClient {
    pub fn new(cfg: ClobConfig) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(cfg.request_timeout)
            .build()
            .context("reqwest client build failed")?;
        Ok(Self { cfg, http })
    }

    pub fn config(&self) -> &ClobConfig { &self.cfg }

    /// Submit a pre-signed order using the configured default OrderType
    /// (FAK for short-window directional trades). Strategy 2 callers
    /// should use `submit_order_as` to override with GTC for maker fills.
    pub async fn submit_order(&self, signed: &SignedOrder) -> Result<SubmitOutcome> {
        self.submit_order_as(signed, self.cfg.order_type, self.cfg.post_only).await
    }

    /// Submit a pre-signed order with an EXPLICIT time-in-force.
    /// Strategy 2 (event sum-of-YES arb) uses GTC so legs rest as
    /// makers and earn the spread instead of paying taker fees.
    pub async fn submit_order_as(
        &self,
        signed:     &SignedOrder,
        order_type: OrderType,
        post_only:  Option<bool>,
    ) -> Result<SubmitOutcome> {
        let body = build_submit_body(
            signed,
            &self.cfg.creds.api_key,
            order_type,
            post_only,
        )?;
        let body_str = serde_json::to_string(&body).context("serialize order body")?;

        if self.cfg.dry_run {
            tracing::info!(
                salt = %body.order.salt,
                token_id = %body.order.token_id,
                side = %body.order.side,
                maker_amount = %body.order.maker_amount,
                taker_amount = %body.order.taker_amount,
                "DRY_RUN: would submit order to {}",
                self.cfg.base_url,
            );
            return Ok(SubmitOutcome::dry_run());
        }

        self.submit_with_retry(&body_str).await
    }

    async fn submit_with_retry(&self, body_str: &str) -> Result<SubmitOutcome> {
        let url = format!("{}{}", self.cfg.base_url, ORDER_PATH);
        let total_attempts = self.cfg.max_retries.saturating_add(1);
        let mut backoff_ms = self.cfg.initial_backoff_ms;
        let mut last_err: Option<String> = None;
        let mut last_http: u16 = 0;

        for attempt in 1..=total_attempts {
            let headers = auth_headers_now(
                &self.cfg.address,
                &self.cfg.creds,
                "POST",
                ORDER_PATH,
                body_str,
            ).context("auth_headers_now failed")?;

            let mut req = self.http
                .post(&url)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body_str.to_owned());
            for (k, v) in &headers {
                req = req.header(k.as_str(), v.as_str());
            }

            match req.send().await {
                Ok(resp) => {
                    last_http = resp.status().as_u16();
                    if resp.status().is_success() {
                        let text = resp.text().await
                            .context("read response body")?;
                        let parsed: ClobApiResponse = serde_json::from_str(&text)
                            .with_context(|| format!("parse CLOB response: {}", truncate(&text, 200)))?;
                        return Ok(SubmitOutcome {
                            success:     parsed.success,
                            order_id:    parsed.order_id,
                            status:      parsed.status,
                            error_msg:   parsed.error_msg,
                            http_status: last_http,
                            dry_run:     false,
                            attempts:    attempt,
                        });
                    }
                    if (500..600).contains(&last_http) {
                        let body_preview = resp.text().await.unwrap_or_default();
                        last_err = Some(format!("5xx {}: {}", last_http, truncate(&body_preview, 200)));
                        tracing::warn!(attempt, status = last_http, "transient CLOB error, will retry");
                    } else {
                        // 4xx — our fault; do not retry.
                        let body_preview = resp.text().await.unwrap_or_default();
                        return Ok(SubmitOutcome {
                            success:     false,
                            order_id:    String::new(),
                            status:      String::new(),
                            error_msg:   format!("HTTP {}: {}", last_http, truncate(&body_preview, 200)),
                            http_status: last_http,
                            dry_run:     false,
                            attempts:    attempt,
                        });
                    }
                }
                Err(e) => {
                    last_err = Some(format!("network: {}", e));
                    tracing::warn!(attempt, error = %e, "CLOB request failed, will retry");
                }
            }

            if attempt < total_attempts {
                // Apply ±20% jitter to avoid synchronized retries from
                // multiple in-flight callers (thundering herd). Cheap
                // pseudo-random source: nanoseconds-mod-1000 — good
                // enough for de-correlating retries; we don't need
                // cryptographic randomness here.
                let nanos = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.subsec_nanos() as u64)
                    .unwrap_or(0);
                let jitter_unit = (nanos % 1000) as f64 / 1000.0; // 0.0..1.0
                let jitter = 0.8 + 0.4 * jitter_unit;             // 0.8..1.2
                let wait_ms = ((backoff_ms as f64) * jitter) as u64;
                tokio::time::sleep(Duration::from_millis(wait_ms)).await;
                // Cap at MAX_BACKOFF_MS so a misconfigured max_retries
                // doesn't blow up to many seconds of sleep.
                const MAX_BACKOFF_MS: u64 = 30_000;
                backoff_ms = backoff_ms.saturating_mul(2).min(MAX_BACKOFF_MS);
            }
        }

        // All attempts exhausted
        Ok(SubmitOutcome {
            success:     false,
            order_id:    String::new(),
            status:      String::new(),
            error_msg:   last_err.unwrap_or_else(|| "retry budget exhausted".to_string()),
            http_status: last_http,
            dry_run:     false,
            attempts:    total_attempts,
        })
    }

    // ── Strategy 2: cancel a resting limit order ────────────────────────

    /// Cancel an open (resting) limit order by Polymarket order_id.
    /// Returns Ok(true) on success, Ok(false) if the order was already
    /// filled / cancelled / not-found (server returns 4xx). DRY_RUN
    /// returns Ok(true) without network.
    pub async fn cancel_order(&self, order_id: &str) -> Result<bool> {
        if self.cfg.dry_run {
            tracing::info!(order_id, "DRY_RUN: would cancel");
            return Ok(true);
        }
        if order_id.is_empty() {
            anyhow::bail!("cancel: empty order_id");
        }
        // Polymarket: POST /order/cancel with body {"orderID": "<id>"}
        let path = "/order/cancel";
        let body = serde_json::json!({"orderID": order_id}).to_string();
        let url = format!("{}{}", self.cfg.base_url, path);
        let headers = auth_headers_now(
            &self.cfg.address,
            &self.cfg.creds,
            "POST",
            path,
            &body,
        ).context("auth_headers_now (cancel)")?;
        let mut req = self.http
            .post(&url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body);
        for (k, v) in &headers {
            req = req.header(k.as_str(), v.as_str());
        }
        let resp = req.send().await.context("cancel send")?;
        let status = resp.status();
        if status.is_success() { return Ok(true); }
        // 4xx — could be "already cancelled" or "already filled"; treat as
        // false rather than error so callers can move on.
        if status.is_client_error() {
            let body = resp.text().await.unwrap_or_default();
            tracing::debug!(order_id, status = status.as_u16(), body = %truncate(&body, 200),
                "cancel got 4xx (likely already gone)");
            return Ok(false);
        }
        anyhow::bail!("cancel HTTP {}", status);
    }

    // ── EDGE-A: bulk cancel by market_id ─────────────────────────────────

    /// Cancel ALL of our open orders on a single market. Used by the LP
    /// quoter when the inventory cap trips or the market exits the
    /// rewards programme. DRY_RUN short-circuits to Ok(0).
    pub async fn cancel_orders_by_market(&self, market_id: &str) -> Result<usize> {
        if self.cfg.dry_run {
            tracing::info!(market_id, "DRY_RUN: would cancel-by-market");
            return Ok(0);
        }
        if market_id.is_empty() {
            anyhow::bail!("cancel_orders_by_market: empty market_id");
        }
        let body = serde_json::json!({"market": market_id}).to_string();
        let url = format!("{}{}", self.cfg.base_url, CANCEL_MARKET_PATH);
        let headers = auth_headers_now(
            &self.cfg.address,
            &self.cfg.creds,
            "POST",
            CANCEL_MARKET_PATH,
            &body,
        ).context("auth_headers_now (cancel-market)")?;
        let mut req = self.http
            .post(&url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body);
        for (k, v) in &headers {
            req = req.header(k.as_str(), v.as_str());
        }
        let resp = req.send().await.context("cancel-market send")?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if status.is_success() {
            // Polymarket returns {"canceled":[ids], "not_canceled":{...}}.
            // Count what we got back if parseable; fall back to 0.
            #[derive(Deserialize)]
            struct Resp { #[serde(default)] canceled: Vec<String> }
            let n = serde_json::from_str::<Resp>(&text)
                .map(|r| r.canceled.len())
                .unwrap_or(0);
            return Ok(n);
        }
        if status.is_client_error() {
            tracing::debug!(market_id, status = status.as_u16(),
                body = %truncate(&text, 200), "cancel-market 4xx");
            return Ok(0);
        }
        anyhow::bail!("cancel-market HTTP {}: {}", status, truncate(&text, 200));
    }

    // ── EDGE-A: bulk order submit (postOrders, up to 15 per call) ────────

    /// Submit up to MAX_BULK_ORDERS pre-signed orders in one POST. Each
    /// entry can have its own OrderType / post_only — useful for posting
    /// both bid and ask of every quoted market in a single call.
    /// DRY_RUN logs each and returns a vector of dry-run outcomes.
    pub async fn submit_orders_bulk(
        &self,
        entries: &[(SignedOrder, OrderType, Option<bool>)],
    ) -> Result<Vec<SubmitOutcome>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        if entries.len() > MAX_BULK_ORDERS {
            anyhow::bail!("submit_orders_bulk: {} > MAX_BULK_ORDERS ({})",
                          entries.len(), MAX_BULK_ORDERS);
        }
        if self.cfg.dry_run {
            for (signed, ot, _po) in entries {
                tracing::info!(
                    salt = %u256_to_dec(&signed.order.salt),
                    token_id = %u256_to_dec(&signed.order.token_id),
                    side = ?signed.order.side,
                    order_type = ot.as_str(),
                    "DRY_RUN: would bulk-submit",
                );
            }
            return Ok(entries.iter().map(|_| SubmitOutcome::dry_run()).collect());
        }

        // Build the array body. Polymarket's /orders endpoint accepts
        // an array of the same shape as /order's body.
        let bodies: Result<Vec<SubmitBody>> = entries
            .iter()
            .map(|(signed, ot, po)| build_submit_body(
                signed, &self.cfg.creds.api_key, *ot, *po))
            .collect();
        let bodies = bodies?;
        let body_str = serde_json::to_string(&bodies).context("serialize bulk")?;

        let url = format!("{}{}", self.cfg.base_url, ORDERS_BULK_PATH);
        let headers = auth_headers_now(
            &self.cfg.address,
            &self.cfg.creds,
            "POST",
            ORDERS_BULK_PATH,
            &body_str,
        ).context("auth_headers_now (bulk)")?;
        let mut req = self.http
            .post(&url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body_str);
        for (k, v) in &headers {
            req = req.header(k.as_str(), v.as_str());
        }
        let resp = req.send().await.context("bulk submit send")?;
        let http_status = resp.status().as_u16();
        let text = resp.text().await.unwrap_or_default();
        if !(200..300).contains(&http_status) {
            // Treat as all-failed rather than panicking.
            tracing::warn!(http_status, body = %truncate(&text, 300),
                "bulk submit non-2xx");
            return Ok(entries.iter().map(|_| SubmitOutcome {
                success:     false,
                order_id:    String::new(),
                status:      String::new(),
                error_msg:   format!("HTTP {}: {}", http_status, truncate(&text, 200)),
                http_status,
                dry_run:     false,
                attempts:    1,
            }).collect());
        }
        // Polymarket returns either an array of ClobApiResponse OR an object
        // wrapping one. We accept both shapes.
        let parsed: Vec<ClobApiResponse> = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(_) => {
                let one: ClobApiResponse = serde_json::from_str(&text)
                    .with_context(|| format!("parse bulk response: {}", truncate(&text, 200)))?;
                vec![one]
            }
        };
        Ok(parsed.into_iter().map(|p| SubmitOutcome {
            success:     p.success,
            order_id:    p.order_id,
            status:      p.status,
            error_msg:   p.error_msg,
            http_status,
            dry_run:     false,
            attempts:    1,
        }).collect())
    }

    // ── EDGE-A: public read endpoints (no auth) ──────────────────────────

    /// Fetch the public order book for one token_id. Returns top-of-book
    /// via best_bid / best_ask helpers; full ladder via bids / asks.
    pub async fn fetch_book(&self, token_id: &str) -> Result<BookSnapshot> {
        let url = format!("{}{}?token_id={}", self.cfg.base_url, BOOK_PATH, token_id);
        let resp = self.http.get(&url).send().await.context("book GET")?;
        let status = resp.status();
        let text = resp.text().await.context("book body read")?;
        if !status.is_success() {
            anyhow::bail!("book HTTP {}: {}", status, truncate(&text, 200));
        }
        let raw: BookWire = serde_json::from_str(&text)
            .with_context(|| format!("parse book: {}", truncate(&text, 200)))?;
        Ok(raw.into_snapshot(token_id.to_string()))
    }

    /// Fetch active reward configs sorted by daily rate. Public, no auth.
    /// Used by the LP rewards strategy to discover which markets to quote.
    pub async fn fetch_active_rewards(&self, max_markets: usize) -> Result<Vec<RewardMarket>> {
        let mut out: Vec<RewardMarket> = Vec::new();
        let mut next_cursor: Option<String> = None;
        loop {
            let qs = match &next_cursor {
                Some(c) => format!("?limit=500&next_cursor={}", c),
                None    => "?limit=500".to_string(),
            };
            let url = format!("{}{}{}", self.cfg.base_url, REWARDS_CURRENT_PATH, qs);
            let resp = self.http.get(&url).send().await.context("rewards GET")?;
            let status = resp.status();
            let text = resp.text().await.context("rewards body read")?;
            if !status.is_success() {
                anyhow::bail!("rewards HTTP {}: {}", status, truncate(&text, 200));
            }
            let page: RewardsPage = serde_json::from_str(&text)
                .with_context(|| format!("parse rewards: {}", truncate(&text, 200)))?;
            for row in page.data {
                let rate_per_day = row.rewards_config.first()
                    .map(|c| c.rate_per_day)
                    .unwrap_or(0.0);
                if rate_per_day <= 0.0 { continue; }
                out.push(RewardMarket {
                    condition_id:       row.condition_id,
                    rewards_max_spread: row.rewards_max_spread,
                    rewards_min_size:   row.rewards_min_size,
                    rate_per_day,
                    sponsored_daily_rate: row.sponsored_daily_rate.unwrap_or(0.0),
                });
                if out.len() >= max_markets { break; }
            }
            if out.len() >= max_markets { break; }
            match page.next_cursor {
                Some(c) if c != "LTE=" => next_cursor = Some(c),
                _ => break,
            }
        }
        out.sort_by(|a, b| {
            (b.rate_per_day + b.sponsored_daily_rate)
                .partial_cmp(&(a.rate_per_day + a.sponsored_daily_rate))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        out.truncate(max_markets);
        Ok(out)
    }

    // ── G13: Read-side endpoints for startup reconciliation ─────────────

    /// Fetch the open orders Polymarket has on file for `owner`.
    /// In DRY_RUN returns an empty vec (we don't have orders on Polymarket
    /// in dry-run mode anyway).
    pub async fn list_open_orders(&self, owner: &str) -> Result<Vec<PolyOpenOrder>> {
        if self.cfg.dry_run { return Ok(Vec::new()); }
        let url = format!("{}{}?owner={}", self.cfg.base_url, DATA_ORDERS_PATH, owner);
        let body: Vec<PolyOpenOrder> = self
            .http_get_authed(DATA_ORDERS_PATH, &format!("?owner={}", owner), &url)
            .await?;
        Ok(body)
    }

    /// Fetch trades (fills) for `maker` since `after_ms`. Returns most-recent
    /// first per Polymarket's ordering. In DRY_RUN returns an empty vec.
    pub async fn list_trades(&self, maker: &str, after_ms: u64) -> Result<Vec<PolyTrade>> {
        if self.cfg.dry_run { return Ok(Vec::new()); }
        let after_secs = after_ms / 1_000;
        let qs  = format!("?maker={}&after={}", maker, after_secs);
        let url = format!("{}{}{}", self.cfg.base_url, DATA_TRADES_PATH, qs);
        let body: Vec<PolyTrade> = self
            .http_get_authed(DATA_TRADES_PATH, &qs, &url)
            .await?;
        Ok(body)
    }

    /// Authed GET helper. Signs the request with the L2 HMAC headers
    /// (matching what submit_order does on POST). Used by the read-side
    /// reconciliation endpoints. No retry — these are read calls; if
    /// they fail, callers degrade gracefully (skip reconciliation, don't
    /// crash the bot).
    async fn http_get_authed<T: serde::de::DeserializeOwned>(
        &self,
        path:     &str,
        qs:       &str,
        full_url: &str,
    ) -> Result<T> {
        // The signature is computed over (timestamp + "GET" + path + body).
        // For GETs the body is empty. Polymarket signs the path WITHOUT
        // the query string — confirmed against py-clob-client.
        let _ = qs;   // qs is part of the URL but not the signed message
        let headers = auth_headers_now(
            &self.cfg.address,
            &self.cfg.creds,
            "GET",
            path,
            "",
        ).context("auth_headers_now (GET)")?;

        let mut req = self.http.get(full_url);
        for (k, v) in &headers {
            req = req.header(k.as_str(), v.as_str());
        }
        let resp = req.send().await.context("GET send")?;
        let status = resp.status();
        let text = resp.text().await.context("GET body read")?;
        if !status.is_success() {
            anyhow::bail!("HTTP {}: {}", status, truncate(&text, 200));
        }
        serde_json::from_str(&text)
            .with_context(|| format!("parse {} response: {}", path, truncate(&text, 200)))
    }
}

// ── G13: Read-side response schemas ─────────────────────────────────────────

/// A single order Polymarket considers "open" for our wallet. We only
/// keep the fields we need for reconciliation; serde::default makes us
/// tolerant of new fields Polymarket adds over time.
#[derive(Debug, Clone, Deserialize)]
pub struct PolyOpenOrder {
    /// Polymarket order id (matches what we stored as `order_id` after a
    /// successful submit_order).
    #[serde(rename = "id", alias = "order_id")]
    pub id:       String,
    #[serde(default)]
    pub status:   String,        // "live", "matched", "cancelled", ...
    #[serde(default)]
    pub asset_id: String,
    #[serde(default)]
    pub side:     String,
    /// Original requested price (decimal string).
    #[serde(default)]
    pub price:    String,
    /// Total size requested.
    #[serde(default)]
    pub size:     String,
    /// Already-filled portion.
    #[serde(rename = "size_matched", alias = "matched_size", default)]
    pub size_matched: String,
}

/// A trade (fill) Polymarket has on record for our wallet.
#[derive(Debug, Clone, Deserialize)]
pub struct PolyTrade {
    #[serde(default)]
    pub id:             String,
    #[serde(rename = "taker_order_id", default)]
    pub taker_order_id: String,
    #[serde(rename = "maker_order_id", default)]
    pub maker_order_id: String,
    /// Fill price as a decimal string.
    #[serde(default)]
    pub price:          String,
    /// Fill size as a decimal string.
    #[serde(default)]
    pub size:           String,
    #[serde(default)]
    pub side:           String,
    #[serde(rename = "match_time", alias = "timestamp", default)]
    pub timestamp:      String,
}

impl PolyTrade {
    /// Best-effort numeric parse of the price string. Returns None on bad
    /// input rather than failing — reconciliation must degrade gracefully.
    pub fn price_f64(&self) -> Option<f64> { self.price.parse().ok() }
    pub fn size_f64(&self)  -> Option<f64> { self.size.parse().ok()  }
}

// ── EDGE-A: Order book + rewards schemas ────────────────────────────────────

/// Public order book snapshot for one token. Levels are sorted: bids
/// best-first (highest price), asks best-first (lowest price).
#[derive(Debug, Clone)]
pub struct BookSnapshot {
    pub token_id: String,
    pub bids:     Vec<(f64, f64)>,   // (price, size)
    pub asks:     Vec<(f64, f64)>,
}

impl BookSnapshot {
    pub fn best_bid(&self) -> Option<f64> { self.bids.first().map(|(p, _)| *p) }
    pub fn best_ask(&self) -> Option<f64> { self.asks.first().map(|(p, _)| *p) }
    pub fn mid(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some(b), Some(a)) if a >= b => Some((a + b) / 2.0),
            _ => None,
        }
    }
    pub fn spread(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some(b), Some(a)) if a >= b => Some(a - b),
            _ => None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct BookWire {
    #[serde(default)] bids: Vec<BookLevelWire>,
    #[serde(default)] asks: Vec<BookLevelWire>,
}

#[derive(Debug, Deserialize)]
struct BookLevelWire {
    #[serde(default)] price: String,
    #[serde(default)] size:  String,
}

impl BookWire {
    fn into_snapshot(self, token_id: String) -> BookSnapshot {
        fn parse_levels(raw: Vec<BookLevelWire>) -> Vec<(f64, f64)> {
            raw.into_iter()
                .filter_map(|l| {
                    let p = l.price.parse::<f64>().ok()?;
                    let s = l.size.parse::<f64>().ok()?;
                    if p <= 0.0 || s <= 0.0 { return None; }
                    Some((p, s))
                })
                .collect()
        }
        let mut bids = parse_levels(self.bids);
        let mut asks = parse_levels(self.asks);
        bids.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        asks.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        BookSnapshot { token_id, bids, asks }
    }
}

/// One market's reward configuration as returned by /rewards/markets/current.
#[derive(Debug, Clone)]
pub struct RewardMarket {
    pub condition_id:         String,
    /// Max spread in cents (e.g. 3.5 = 3.5 cents).
    pub rewards_max_spread:   f64,
    /// Minimum order size in USD.
    pub rewards_min_size:     f64,
    /// Native daily reward pool USDC.
    pub rate_per_day:         f64,
    pub sponsored_daily_rate: f64,
}

#[derive(Debug, Deserialize)]
struct RewardsPage {
    #[serde(default)] data:        Vec<RewardRowWire>,
    #[serde(default)] next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RewardRowWire {
    condition_id:         String,
    #[serde(default)] rewards_max_spread: f64,
    #[serde(default)] rewards_min_size:   f64,
    #[serde(default)] rewards_config:     Vec<RewardCfgWire>,
    #[serde(default)] sponsored_daily_rate: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct RewardCfgWire {
    #[serde(default)] rate_per_day: f64,
}

// ── Wire conversion ──────────────────────────────────────────────────────────

fn signed_to_wire(signed: &SignedOrder) -> Result<OrderWire> {
    // Salt is generated in presign as a u64 padded into a 32-byte word with
    // the high 24 bytes zero. Verify that invariant and extract the u64.
    let salt32 = &signed.order.salt;
    if salt32[..24].iter().any(|&b| b != 0) {
        anyhow::bail!("salt does not fit in u64 — high 24 bytes nonzero");
    }
    let mut salt8 = [0u8; 8];
    salt8.copy_from_slice(&salt32[24..]);
    let salt_u64 = u64::from_be_bytes(salt8);

    Ok(OrderWire {
        salt:           salt_u64,
        maker:          address_to_hex(&signed.order.maker),
        signer:         address_to_hex(&signed.order.signer),
        taker:          address_to_hex(&signed.order.taker),
        token_id:       u256_to_dec(&signed.order.token_id),
        maker_amount:   u256_to_dec(&signed.order.maker_amount),
        taker_amount:   u256_to_dec(&signed.order.taker_amount),
        expiration:     u256_to_dec(&signed.order.expiration),
        nonce:          u256_to_dec(&signed.order.nonce),
        fee_rate_bps:   u256_to_dec(&signed.order.fee_rate_bps),
        side:           signed.order.side.as_str().to_string(),
        signature_type: signed.order.signature_type as u8,
        signature:      format!("0x{}", hex::encode(signed.signature)),
    })
}

fn build_submit_body(
    signed:     &SignedOrder,
    owner:      &str,
    order_type: OrderType,
    post_only:  Option<bool>,
) -> Result<SubmitBody> {
    if owner.is_empty() {
        bail!("owner (api_key) must not be empty");
    }
    Ok(SubmitBody {
        order:      signed_to_wire(signed)?,
        owner:      owner.to_string(),
        order_type: order_type.as_str().to_string(),
        post_only,
    })
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n { s.to_string() } else { format!("{}...", &s[..n]) }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::orders::{
        hex_to_address, sign_order, u256_from_u64, Domain, Order, Side, SignatureType,
    };

    const TEST_PK:   &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
    const TEST_ADDR: &str = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266";

    fn sample_signed() -> SignedOrder {
        let addr = hex_to_address(TEST_ADDR).unwrap();
        let order = Order {
            // salt MUST fit in u64 — wire format demands it.
            salt:           u256_from_u64(0x0123_4567_89AB_CDEF),
            maker:          addr,
            signer:         addr,
            taker:          [0u8; 20],
            token_id:       u256_from_u64(123_456_789),
            maker_amount:   u256_from_u64(5_000_000),
            taker_amount:   u256_from_u64(10_000_000),
            expiration:     u256_from_u64(0),
            nonce:          u256_from_u64(0),
            fee_rate_bps:   u256_from_u64(0),
            side:           Side::Buy,
            signature_type: SignatureType::Eoa,
        };
        sign_order(order, &Domain::polymarket_polygon(), TEST_PK).unwrap()
    }

    fn sample_creds() -> ApiCreds {
        use base64::{engine::general_purpose::URL_SAFE, Engine as _};
        ApiCreds {
            api_key:        "00000000-1111-2222-3333-444444444444".to_string(),
            api_secret:     URL_SAFE.encode(b"test-secret-bytes-here-32-chars!"),
            api_passphrase: "passphrase".to_string(),
        }
    }

    // ── OrderType + wire ──────────────────────────────────────────────────

    #[test]
    fn order_type_strings() {
        assert_eq!(OrderType::Gtc.as_str(), "GTC");
        assert_eq!(OrderType::Gtd.as_str(), "GTD");
        assert_eq!(OrderType::Fok.as_str(), "FOK");
        assert_eq!(OrderType::Fak.as_str(), "FAK");
    }

    #[test]
    fn signed_to_wire_serialises_amounts_as_decimal_strings() {
        let signed = sample_signed();
        let wire = signed_to_wire(&signed).unwrap();
        assert_eq!(wire.maker_amount, "5000000");
        assert_eq!(wire.taker_amount, "10000000");
        assert_eq!(wire.token_id,     "123456789");
        assert_eq!(wire.side,         "BUY");
        assert_eq!(wire.signature_type, 0);
    }

    #[test]
    fn signed_to_wire_salt_is_u64_number() {
        let signed = sample_signed();
        let wire = signed_to_wire(&signed).unwrap();
        // Round-trips through the wire layer
        assert_eq!(wire.salt, 0x0123_4567_89AB_CDEF_u64);
        // And serialises as a JSON number, not a string.
        let json = serde_json::to_value(&wire).unwrap();
        assert!(json.get("salt").unwrap().is_number(), "salt must be a JSON number, not string");
    }

    #[test]
    fn signed_to_wire_rejects_oversize_salt() {
        // Salt with high bytes set would not fit in a u64.
        let mut signed = sample_signed();
        signed.order.salt[0] = 0xff;
        assert!(signed_to_wire(&signed).is_err());
    }

    #[test]
    fn signed_to_wire_addresses_have_0x_prefix_and_40_hex() {
        let wire = signed_to_wire(&sample_signed()).unwrap();
        assert!(wire.maker.starts_with("0x") && wire.maker.len() == 42);
        assert!(wire.signer.starts_with("0x") && wire.signer.len() == 42);
        assert!(wire.taker.starts_with("0x") && wire.taker.len() == 42);
    }

    #[test]
    fn signed_to_wire_signature_is_0x_prefixed_130_hex() {
        let wire = signed_to_wire(&sample_signed()).unwrap();
        assert!(wire.signature.starts_with("0x"));
        // 65 bytes * 2 hex + 2 ("0x") = 132
        assert_eq!(wire.signature.len(), 132);
    }

    #[test]
    fn taker_zero_address_serialises_as_all_zeros() {
        let wire = signed_to_wire(&sample_signed()).unwrap();
        assert_eq!(wire.taker, "0x0000000000000000000000000000000000000000");
    }

    // ── build_submit_body ─────────────────────────────────────────────────

    #[test]
    fn build_submit_body_includes_owner_and_order_type() {
        let signed = sample_signed();
        let body = build_submit_body(&signed, "uuid-here", OrderType::Fok, None).unwrap();
        assert_eq!(body.owner, "uuid-here");
        assert_eq!(body.order_type, "FOK");
        assert!(body.post_only.is_none());
    }

    #[test]
    fn build_submit_body_rejects_empty_owner() {
        assert!(build_submit_body(&sample_signed(), "", OrderType::Gtc, None).is_err());
    }

    #[test]
    fn submit_body_serialises_to_expected_json_keys() {
        let signed = sample_signed();
        let body = build_submit_body(&signed, "u", OrderType::Gtc, None).unwrap();
        let json = serde_json::to_value(&body).unwrap();
        let order = json.get("order").unwrap();
        for key in ["salt", "maker", "signer", "taker", "tokenId",
                    "makerAmount", "takerAmount", "expiration", "nonce",
                    "feeRateBps", "side", "signatureType", "signature"] {
            assert!(order.get(key).is_some(), "missing key: {}", key);
        }
        assert!(json.get("owner").is_some());
        assert!(json.get("orderType").is_some());
        // post_only is absent when None
        assert!(json.get("postOnly").is_none());
    }

    #[test]
    fn submit_body_post_only_appears_when_set() {
        let signed = sample_signed();
        let body = build_submit_body(&signed, "u", OrderType::Gtc, Some(true)).unwrap();
        let json = serde_json::to_value(&body).unwrap();
        assert_eq!(json.get("postOnly").unwrap().as_bool(), Some(true));
    }

    #[test]
    fn signed_to_wire_round_trip_preserves_recoverable_signer() {
        // End-to-end: sign → wire encode → JSON → parse JSON → reconstruct
        // a SignedOrder from the JSON values → recover_signer should match
        // the address derived from TEST_PK. This catches any silent
        // bytewise corruption between sign_order and the wire format
        // (salt encoding, hex padding, signature byte order, etc).
        use crate::execution::orders::{
            private_key_to_address, recover_signer, u256_from_dec,
            Domain, Order, SignedOrder,
        };
        let signed = sample_signed();
        let body = build_submit_body(&signed, "owner-uuid", OrderType::Gtc, None).unwrap();
        let json = serde_json::to_value(&body).unwrap();

        // Pull the order subobject and reconstruct a SignedOrder from
        // its JSON values exactly the way Polymarket's matcher would.
        let o = json.get("order").expect("order subobject");
        let salt_u64 = o.get("salt").unwrap().as_u64().expect("salt is JSON number");
        // u256 fields stored as decimal strings in the wire body
        let token_id = u256_from_dec(o.get("tokenId").unwrap().as_str().unwrap()).unwrap();
        let maker_amount = u256_from_dec(o.get("makerAmount").unwrap().as_str().unwrap()).unwrap();
        let taker_amount = u256_from_dec(o.get("takerAmount").unwrap().as_str().unwrap()).unwrap();
        let expiration = u256_from_dec(o.get("expiration").unwrap().as_str().unwrap()).unwrap();
        let nonce = u256_from_dec(o.get("nonce").unwrap().as_str().unwrap()).unwrap();
        let fee_rate_bps = u256_from_dec(o.get("feeRateBps").unwrap().as_str().unwrap()).unwrap();
        // Addresses come back as "0x..." strings
        let maker = hex_to_address(o.get("maker").unwrap().as_str().unwrap()).unwrap();
        let signer = hex_to_address(o.get("signer").unwrap().as_str().unwrap()).unwrap();
        let taker = hex_to_address(o.get("taker").unwrap().as_str().unwrap()).unwrap();
        // Signature is "0x" + 130 hex chars (65 bytes: r || s || v)
        let sig_str = o.get("signature").unwrap().as_str().unwrap();
        let sig_bytes = hex::decode(sig_str.trim_start_matches("0x")).unwrap();
        assert_eq!(sig_bytes.len(), 65);
        let mut signature = [0u8; 65];
        signature.copy_from_slice(&sig_bytes);
        // Reconstruct salt as 32 bytes (high 24 zero, low 8 = u64 BE)
        let mut salt = [0u8; 32];
        salt[24..].copy_from_slice(&salt_u64.to_be_bytes());
        // Decode side enum
        let side = match o.get("side").unwrap().as_str().unwrap() {
            "BUY"  => crate::execution::orders::Side::Buy,
            "SELL" => crate::execution::orders::Side::Sell,
            other  => panic!("unknown side {other}"),
        };
        let signature_type = match o.get("signatureType").unwrap().as_u64().unwrap() {
            0 => crate::execution::orders::SignatureType::Eoa,
            1 => crate::execution::orders::SignatureType::PolyProxy,
            2 => crate::execution::orders::SignatureType::PolyGnosis,
            other => panic!("unknown signatureType {other}"),
        };

        let reconstructed = SignedOrder {
            order: Order {
                salt, maker, signer, taker,
                token_id, maker_amount, taker_amount,
                expiration, nonce, fee_rate_bps,
                side, signature_type,
            },
            signature,
        };

        let domain = Domain::polymarket_polygon();
        let recovered = recover_signer(&domain, &reconstructed)
            .expect("recover_signer should succeed");
        let expected = private_key_to_address(TEST_PK).expect("derive addr from TEST_PK");
        assert_eq!(recovered, expected,
            "recovered signer from JSON-roundtripped order must equal address derived from TEST_PK");
    }

    // ── ClobApiResponse parsing ───────────────────────────────────────────

    #[test]
    fn response_parses_success_payload() {
        let raw = r#"{
            "success": true,
            "errorMsg": "",
            "orderID": "0xabc",
            "status": "matched",
            "transactionsHashes": ["0xdef"]
        }"#;
        let parsed: ClobApiResponse = serde_json::from_str(raw).unwrap();
        assert!(parsed.success);
        assert_eq!(parsed.order_id, "0xabc");
        assert_eq!(parsed.status, "matched");
        assert_eq!(parsed.transaction_hashes, vec!["0xdef"]);
    }

    #[test]
    fn response_parses_failure_payload() {
        let raw = r#"{
            "success": false,
            "errorMsg": "insufficient balance",
            "orderID": "",
            "status": ""
        }"#;
        let parsed: ClobApiResponse = serde_json::from_str(raw).unwrap();
        assert!(!parsed.success);
        assert_eq!(parsed.error_msg, "insufficient balance");
        assert!(parsed.transaction_hashes.is_empty());
    }

    #[test]
    fn response_tolerates_missing_optional_fields() {
        // Real Polymarket responses don't always include every field
        let raw = r#"{ "success": true }"#;
        let parsed: ClobApiResponse = serde_json::from_str(raw).unwrap();
        assert!(parsed.success);
        assert_eq!(parsed.order_id, "");
        assert_eq!(parsed.status,   "");
    }

    #[test]
    fn response_rejects_missing_required_field() {
        let raw = r#"{ "errorMsg": "no success field" }"#;
        let res: Result<ClobApiResponse, _> = serde_json::from_str(raw);
        assert!(res.is_err());
    }

    // ── ClobConfig + ClobClient construction ──────────────────────────────

    #[test]
    fn clob_config_defaults() {
        let cfg = ClobConfig::new(TEST_ADDR, sample_creds(), true);
        assert_eq!(cfg.base_url, POLYMARKET_CLOB_BASE_URL);
        assert!(cfg.dry_run);
        // FAK is the right default for short-window directional trades —
        // partial fills OK, remainder cancelled (no resting on the book).
        assert_eq!(cfg.order_type, OrderType::Fak);
        assert!(cfg.post_only.is_none());
        assert_eq!(cfg.max_retries, 3);
        assert_eq!(cfg.initial_backoff_ms, 100);
    }

    #[test]
    fn clob_client_builds() {
        let cfg = ClobConfig::new(TEST_ADDR, sample_creds(), true);
        assert!(ClobClient::new(cfg).is_ok());
    }

    // ── DRY_RUN path ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn submit_order_dry_run_returns_success_without_http() {
        let cfg = ClobConfig::new(TEST_ADDR, sample_creds(), /*dry_run=*/true);
        let client = ClobClient::new(cfg).unwrap();
        let signed = sample_signed();
        let outcome = client.submit_order(&signed).await.unwrap();
        assert!(outcome.dry_run);
        assert!(outcome.success);
        assert_eq!(outcome.http_status, 0);
        assert_eq!(outcome.attempts, 0);
        assert_eq!(outcome.status, "dry_run");
    }

    // ── Retry / 4xx behaviour against a local mock ────────────────────────

    /// Tiny single-shot HTTP server that lets us assert retry behaviour
    /// without pulling in wiremock as a dev-dependency.
    async fn run_mock<F>(handler: F) -> (String, tokio::task::JoinHandle<()>)
    where
        F: Fn(usize) -> (u16, &'static str) + Send + Sync + 'static,
    {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr     = listener.local_addr().unwrap();
        let handler  = std::sync::Arc::new(handler);
        let h = tokio::spawn(async move {
            let mut hits = 0usize;
            loop {
                match listener.accept().await {
                    Ok((mut sock, _)) => {
                        let mut buf = [0u8; 4096];
                        let _ = sock.read(&mut buf).await.unwrap_or(0);
                        let (status, body) = (handler)(hits);
                        hits += 1;
                        let resp = format!(
                            "HTTP/1.1 {} OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
                            status, body.len(), body,
                        );
                        let _ = sock.write_all(resp.as_bytes()).await;
                        let _ = sock.shutdown().await;
                    }
                    Err(_) => break,
                }
            }
        });
        (format!("http://{}", addr), h)
    }

    #[tokio::test]
    async fn submit_returns_failure_on_4xx_without_retry() {
        let (base, _h) = run_mock(|_| (400, r#"{"success":false,"errorMsg":"bad sig"}"#)).await;
        let mut cfg = ClobConfig::new(TEST_ADDR, sample_creds(), false);
        cfg.base_url = base;
        cfg.max_retries = 3;
        cfg.initial_backoff_ms = 1;
        let client = ClobClient::new(cfg).unwrap();
        let outcome = client.submit_order(&sample_signed()).await.unwrap();
        assert!(!outcome.success);
        assert_eq!(outcome.http_status, 400);
        // 4xx: no retry, exactly 1 attempt
        assert_eq!(outcome.attempts, 1);
    }

    #[tokio::test]
    async fn submit_retries_on_5xx_then_succeeds() {
        // First two calls return 503, third returns 200 success.
        let (base, _h) = run_mock(|hits| {
            if hits < 2 {
                (503, r#"{"success":false,"errorMsg":"transient"}"#)
            } else {
                (200, r#"{"success":true,"orderID":"0xok","status":"live"}"#)
            }
        }).await;
        let mut cfg = ClobConfig::new(TEST_ADDR, sample_creds(), false);
        cfg.base_url = base;
        cfg.max_retries = 5;
        cfg.initial_backoff_ms = 1;
        let client = ClobClient::new(cfg).unwrap();
        let outcome = client.submit_order(&sample_signed()).await.unwrap();
        assert!(outcome.success);
        assert_eq!(outcome.order_id, "0xok");
        assert_eq!(outcome.attempts, 3);
    }

    #[tokio::test]
    async fn submit_exhausts_retries_on_persistent_5xx() {
        let (base, _h) = run_mock(|_| (502, r#"{"success":false,"errorMsg":"upstream"}"#)).await;
        let mut cfg = ClobConfig::new(TEST_ADDR, sample_creds(), false);
        cfg.base_url = base;
        cfg.max_retries = 2;
        cfg.initial_backoff_ms = 1;
        let client = ClobClient::new(cfg).unwrap();
        let outcome = client.submit_order(&sample_signed()).await.unwrap();
        assert!(!outcome.success);
        assert_eq!(outcome.attempts, 3);   // 1 + 2 retries
        assert!(outcome.error_msg.contains("502"));
    }

    // ── Strategy 2: GTC submit + cancel ──────────────────────────────────

    #[tokio::test]
    async fn submit_order_as_uses_explicit_order_type() {
        // Verifies submit_order_as overrides the cfg default. Mock returns
        // the request body so we can inspect the orderType field.
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let received: std::sync::Arc<tokio::sync::Mutex<Option<String>>> =
            std::sync::Arc::new(tokio::sync::Mutex::new(None));
        let r2 = received.clone();
        tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = vec![0u8; 16384];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]).to_string();
                *r2.lock().await = Some(req);
                let body = r#"{"success":true,"orderID":"0xok","status":"live"}"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
                    body.len(), body,
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            }
        });
        let mut cfg = ClobConfig::new(TEST_ADDR, sample_creds(), false);
        cfg.base_url = format!("http://{}", addr);
        cfg.max_retries = 0;
        cfg.order_type = OrderType::Fak;   // default = FAK
        let client = ClobClient::new(cfg).unwrap();
        // Override to GTC
        let _ = client.submit_order_as(
            &sample_signed(), OrderType::Gtc, None,
        ).await.unwrap();
        let req_text = received.lock().await.clone().unwrap_or_default();
        assert!(req_text.contains("\"orderType\":\"GTC\""),
            "expected GTC orderType in request, got: {}",
            truncate(&req_text, 600));
    }

    #[tokio::test]
    async fn cancel_order_dry_run_returns_true_without_http() {
        let cfg = ClobConfig::new(TEST_ADDR, sample_creds(), true);
        let client = ClobClient::new(cfg).unwrap();
        assert!(client.cancel_order("0xORDER").await.unwrap());
    }

    #[tokio::test]
    async fn cancel_order_returns_true_on_2xx() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = vec![0u8; 4096];
                let _ = sock.read(&mut buf).await;
                let body = r#"{"cancelled":true}"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
                    body.len(), body,
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            }
        });
        let mut cfg = ClobConfig::new(TEST_ADDR, sample_creds(), false);
        cfg.base_url = format!("http://{}", addr);
        let client = ClobClient::new(cfg).unwrap();
        assert!(client.cancel_order("0xORDER").await.unwrap());
    }

    #[tokio::test]
    async fn cancel_order_returns_false_on_4xx() {
        // Cancel of an already-filled order returns 4xx.
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = vec![0u8; 4096];
                let _ = sock.read(&mut buf).await;
                let body = r#"{"error":"order not found"}"#;
                let resp = format!(
                    "HTTP/1.1 404 NOT FOUND\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
                    body.len(), body,
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            }
        });
        let mut cfg = ClobConfig::new(TEST_ADDR, sample_creds(), false);
        cfg.base_url = format!("http://{}", addr);
        let client = ClobClient::new(cfg).unwrap();
        let r = client.cancel_order("0xORDER").await.unwrap();
        assert!(!r);
    }

    #[tokio::test]
    async fn cancel_order_rejects_empty_id() {
        let cfg = ClobConfig::new(TEST_ADDR, sample_creds(), false);
        let client = ClobClient::new(cfg).unwrap();
        assert!(client.cancel_order("").await.is_err());
    }

    // ── EDGE-A tests: cancel-by-market, bulk submit, book, rewards ──────────

    #[tokio::test]
    async fn cancel_by_market_dry_run_returns_zero() {
        let cfg = ClobConfig::new(TEST_ADDR, sample_creds(), true);
        let client = ClobClient::new(cfg).unwrap();
        let n = client.cancel_orders_by_market("0xCONDITION").await.unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn cancel_by_market_rejects_empty() {
        let cfg = ClobConfig::new(TEST_ADDR, sample_creds(), false);
        let client = ClobClient::new(cfg).unwrap();
        assert!(client.cancel_orders_by_market("").await.is_err());
    }

    #[tokio::test]
    async fn bulk_submit_dry_run_returns_one_outcome_per_entry() {
        let cfg = ClobConfig::new(TEST_ADDR, sample_creds(), true);
        let client = ClobClient::new(cfg).unwrap();
        let entries = vec![
            (sample_signed(), OrderType::Gtc, Some(true)),
            (sample_signed(), OrderType::Gtc, Some(true)),
            (sample_signed(), OrderType::Gtc, Some(true)),
        ];
        let outcomes = client.submit_orders_bulk(&entries).await.unwrap();
        assert_eq!(outcomes.len(), 3);
        for o in outcomes {
            assert!(o.dry_run);
        }
    }

    #[tokio::test]
    async fn bulk_submit_rejects_oversize_batch() {
        let cfg = ClobConfig::new(TEST_ADDR, sample_creds(), true);
        let client = ClobClient::new(cfg).unwrap();
        let too_many: Vec<_> = (0..MAX_BULK_ORDERS + 1)
            .map(|_| (sample_signed(), OrderType::Gtc, None))
            .collect();
        assert!(client.submit_orders_bulk(&too_many).await.is_err());
    }

    #[tokio::test]
    async fn bulk_submit_empty_input_returns_empty() {
        let cfg = ClobConfig::new(TEST_ADDR, sample_creds(), true);
        let client = ClobClient::new(cfg).unwrap();
        let out = client.submit_orders_bulk(&[]).await.unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn book_wire_parses_levels_and_sorts() {
        // Bids should be best-first (highest), asks should be best-first (lowest)
        let raw = r#"{
            "bids": [{"price": "0.49", "size": "100"}, {"price": "0.50", "size": "200"}],
            "asks": [{"price": "0.55", "size": "50"}, {"price": "0.52", "size": "150"}]
        }"#;
        let wire: BookWire = serde_json::from_str(raw).unwrap();
        let snap = wire.into_snapshot("tok123".to_string());
        assert_eq!(snap.best_bid(), Some(0.50));
        assert_eq!(snap.best_ask(), Some(0.52));
        assert!(snap.mid().is_some());
        let mid = snap.mid().unwrap();
        assert!((mid - 0.51).abs() < 1e-9);
        assert!((snap.spread().unwrap() - 0.02).abs() < 1e-9);
    }

    #[test]
    fn book_wire_skips_zero_or_negative_levels() {
        let raw = r#"{
            "bids": [{"price": "0", "size": "100"}, {"price": "0.50", "size": "0"}, {"price": "0.49", "size": "100"}],
            "asks": []
        }"#;
        let wire: BookWire = serde_json::from_str(raw).unwrap();
        let snap = wire.into_snapshot("tok".to_string());
        assert_eq!(snap.bids.len(), 1);
        assert_eq!(snap.bids[0].0, 0.49);
        assert!(snap.best_ask().is_none());
        assert!(snap.mid().is_none());
    }

    #[test]
    fn book_snapshot_handles_crossed_book() {
        // If bid > ask (crossed), mid + spread return None to signal junk data
        let snap = BookSnapshot {
            token_id: "x".to_string(),
            bids: vec![(0.60, 100.0)],
            asks: vec![(0.55, 100.0)],
        };
        assert!(snap.mid().is_none());
        assert!(snap.spread().is_none());
    }

    #[test]
    fn rewards_page_parses_with_minimal_fields() {
        let raw = r#"{
            "data": [
                {
                    "condition_id": "0xABC",
                    "rewards_max_spread": 3.5,
                    "rewards_min_size": 200.0,
                    "rewards_config": [{"rate_per_day": 4000.0}]
                }
            ],
            "next_cursor": "LTE="
        }"#;
        let page: RewardsPage = serde_json::from_str(raw).unwrap();
        assert_eq!(page.data.len(), 1);
        assert_eq!(page.data[0].condition_id, "0xABC");
        assert_eq!(page.data[0].rewards_config[0].rate_per_day, 4000.0);
    }

    #[test]
    fn rewards_page_skips_zero_rate_rows() {
        // Simulate the filter applied inside fetch_active_rewards
        let raw = r#"{
            "data": [
                {"condition_id": "0xA", "rewards_max_spread": 3.5, "rewards_min_size": 200.0,
                 "rewards_config": [{"rate_per_day": 100.0}]},
                {"condition_id": "0xB", "rewards_max_spread": 3.5, "rewards_min_size": 200.0,
                 "rewards_config": []}
            ]
        }"#;
        let page: RewardsPage = serde_json::from_str(raw).unwrap();
        let mut keep = 0;
        for row in page.data {
            let rate = row.rewards_config.first().map(|c| c.rate_per_day).unwrap_or(0.0);
            if rate > 0.0 { keep += 1; }
        }
        assert_eq!(keep, 1);
    }
}
