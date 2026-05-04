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

    /// Submit a pre-signed order. In DRY_RUN this returns a synthetic success
    /// without ever touching the network.
    pub async fn submit_order(&self, signed: &SignedOrder) -> Result<SubmitOutcome> {
        let body = build_submit_body(
            signed,
            &self.cfg.creds.api_key,
            self.cfg.order_type,
            self.cfg.post_only,
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
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = backoff_ms.saturating_mul(2);
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
}
