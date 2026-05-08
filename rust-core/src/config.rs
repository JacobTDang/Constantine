use anyhow::{Context, Result};

use crate::execution::orders::SignatureType;

#[derive(Debug, Clone)]
pub struct Config {
    // Polymarket credentials
    pub polymarket_private_key: String,
    pub polymarket_api_key: String,
    pub polymarket_api_secret: String,
    pub polymarket_api_passphrase: String,
    /// Signing model — "EOA" / "POLY_PROXY" / "POLY_GNOSIS_SAFE".
    /// EOA: maker = signer = derived from private_key.
    /// POLY_PROXY / POLY_GNOSIS_SAFE: maker = funder_address, signer = derived from private_key.
    pub polymarket_signature_type: SignatureType,
    /// Optional override for the order's `maker` field. Required when
    /// signature_type is POLY_PROXY or POLY_GNOSIS_SAFE (must be the proxy /
    /// safe address). For EOA, leave None and we derive from the private key.
    pub polymarket_funder_address: Option<String>,

    // RPC
    pub alchemy_polygon_key: String,

    // Optional NLP
    pub anthropic_api_key: Option<String>,

    // Trading behaviour
    pub dry_run: bool,
    /// Master switch — when false, the bot logs signals but never submits.
    /// Default: false (observe mode). Sprint 6 wires the execution loop;
    /// Sprint 9 is the testnet drill that flips this to true.
    pub execution_enabled: bool,
    pub max_bet_dollars: f64,
    pub bankroll: f64,
    pub min_edge: f64,
    pub kelly_fraction: f64,
    pub daily_loss_limit_dollars: f64,
    pub weekly_loss_limit_dollars: f64,
    pub max_open_exposure_dollars: f64,
    pub min_liquidity: f64,
    pub oracle_arb_threshold: f64,
    pub scan_interval_ms: u64,

    // EDGE-A: liquidity-rewards quoter. Off by default; flip on after
    // running Phase 0 backtest and confirming markets pass.
    pub lp_quoter_enabled: bool,
    pub lp_quote_size_usd: f64,
    pub lp_max_markets:    usize,
    pub lp_inventory_cap_usd: f64,
    pub lp_refresh_secs:   u64,

    // EDGE-C: Pinnacle devig sportsbook overlay
    pub sportsbook_devig_enabled: bool,
    pub sportsbook_devig_path:    String,
    pub sportsbook_devig_reload_secs: u64,

    // EDGE-D: whale-follow
    pub whale_follow_enabled: bool,
    pub whale_trades_path:    String,
    pub whale_reload_secs:    u64,

    // EDGE-E: hibernating market scanner (observe-only)
    pub hibernating_enabled:  bool,
    pub hibernating_scan_secs: u64,

    // Logging
    pub log_level: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Self::parse(|key| std::env::var(key).ok())
    }

    // Separated from from_env() so tests can inject values without touching
    // global env state.
    fn parse(get: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let req = |key: &'static str| -> Result<String> {
            get(key).with_context(|| format!("missing required env var: {key}"))
        };

        let f64_var = |key: &'static str, default: f64| -> Result<f64> {
            match get(key) {
                Some(v) => v
                    .parse::<f64>()
                    .with_context(|| format!("{key} must be a number, got {v:?}")),
                None => Ok(default),
            }
        };

        let u64_var = |key: &'static str, default: u64| -> Result<u64> {
            match get(key) {
                Some(v) => v
                    .parse::<u64>()
                    .with_context(|| format!("{key} must be an integer, got {v:?}")),
                None => Ok(default),
            }
        };

        let bool_var = |key: &'static str, default: bool| -> Result<bool> {
            match get(key).as_deref() {
                Some("true") => Ok(true),
                Some("false") => Ok(false),
                Some(v) => anyhow::bail!("{key} must be 'true' or 'false', got {v:?}"),
                None => Ok(default),
            }
        };

        let sig_type = match get("POLYMARKET_SIGNATURE_TYPE") {
            Some(s) => SignatureType::parse(&s)?,
            None    => SignatureType::Eoa,
        };
        let funder = get("POLYMARKET_FUNDER_ADDRESS").filter(|s| !s.is_empty());

        // POLY_PROXY / POLY_GNOSIS_SAFE require an explicit funder address.
        if matches!(sig_type, SignatureType::PolyProxy | SignatureType::PolyGnosis)
            && funder.is_none()
        {
            anyhow::bail!(
                "POLYMARKET_SIGNATURE_TYPE={:?} requires POLYMARKET_FUNDER_ADDRESS to be set",
                sig_type
            );
        }

        Ok(Self {
            polymarket_private_key:    req("POLYMARKET_PRIVATE_KEY")?,
            polymarket_api_key:        req("POLYMARKET_API_KEY")?,
            polymarket_api_secret:     req("POLYMARKET_API_SECRET")?,
            polymarket_api_passphrase: req("POLYMARKET_API_PASSPHRASE")?,
            polymarket_signature_type: sig_type,
            polymarket_funder_address: funder,
            execution_enabled:         bool_var("EXECUTION_ENABLED", false)?,
            alchemy_polygon_key:      req("ALCHEMY_POLYGON_KEY")?,
            anthropic_api_key:        get("ANTHROPIC_API_KEY"),
            dry_run:                  bool_var("DRY_RUN", true)?,
            max_bet_dollars:          f64_var("MAX_BET_DOLLARS", 30.0)?,
            bankroll:                 f64_var("BANKROLL", 1500.0)?,
            min_edge:                 f64_var("MIN_EDGE", 0.05)?,
            kelly_fraction:           f64_var("KELLY_FRACTION", 0.25)?,
            daily_loss_limit_dollars: f64_var("DAILY_LOSS_LIMIT_DOLLARS", 75.0)?,
            weekly_loss_limit_dollars:f64_var("WEEKLY_LOSS_LIMIT_DOLLARS", 150.0)?,
            max_open_exposure_dollars:f64_var("MAX_OPEN_EXPOSURE_DOLLARS", 300.0)?,
            min_liquidity:            f64_var("MIN_LIQUIDITY", 500.0)?,
            oracle_arb_threshold:     f64_var("ORACLE_ARB_THRESHOLD", 0.04)?,
            scan_interval_ms:         u64_var("SCAN_INTERVAL_MS", 500)?,
            lp_quoter_enabled:        bool_var("LP_QUOTER_ENABLED", false)?,
            lp_quote_size_usd:        f64_var("LP_QUOTE_SIZE_USD", 200.0)?,
            lp_max_markets:           u64_var("LP_MAX_MARKETS", 5)? as usize,
            lp_inventory_cap_usd:     f64_var("LP_INVENTORY_CAP_USD", 400.0)?,
            lp_refresh_secs:          u64_var("LP_REFRESH_SECS", 5)?,
            sportsbook_devig_enabled: bool_var("SPORTSBOOK_DEVIG_ENABLED", false)?,
            sportsbook_devig_path:    get("SPORTSBOOK_DEVIG_PATH")
                .unwrap_or_else(|| "data/sportsbook_devig.json".to_string()),
            sportsbook_devig_reload_secs: u64_var("SPORTSBOOK_DEVIG_RELOAD_SECS", 30)?,
            whale_follow_enabled:     bool_var("WHALE_FOLLOW_ENABLED", false)?,
            whale_trades_path:        get("WHALE_TRADES_PATH")
                .unwrap_or_else(|| "data/whale_trades.jsonl".to_string()),
            whale_reload_secs:        u64_var("WHALE_RELOAD_SECS", 30)?,
            hibernating_enabled:      bool_var("HIBERNATING_ENABLED", false)?,
            hibernating_scan_secs:    u64_var("HIBERNATING_SCAN_SECS", 1800)?,
            log_level:                get("LOG_LEVEL").unwrap_or_else(|| "info".to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn fixture() -> HashMap<String, String> {
        [
            ("POLYMARKET_PRIVATE_KEY",    "0xdeadbeef"),
            ("POLYMARKET_API_KEY",        "test-api-key"),
            ("POLYMARKET_API_SECRET",     "test-api-secret"),
            ("POLYMARKET_API_PASSPHRASE", "test-passphrase"),
            ("ALCHEMY_POLYGON_KEY",       "test-alchemy-key"),
            ("ANTHROPIC_API_KEY",         "test-anthropic-key"),
            ("DRY_RUN",                   "true"),
            ("MAX_BET_DOLLARS",           "30"),
            ("BANKROLL",                  "1500"),
            ("MIN_EDGE",                  "0.05"),
            ("KELLY_FRACTION",            "0.25"),
            ("DAILY_LOSS_LIMIT_DOLLARS",  "75"),
            ("WEEKLY_LOSS_LIMIT_DOLLARS", "150"),
            ("MAX_OPEN_EXPOSURE_DOLLARS", "300"),
            ("MIN_LIQUIDITY",             "500"),
            ("ORACLE_ARB_THRESHOLD",      "0.04"),
            ("SCAN_INTERVAL_MS",          "500"),
            ("LOG_LEVEL",                 "info"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    }

    fn parse(env: &HashMap<String, String>) -> Result<Config> {
        Config::parse(|k| env.get(k).cloned())
    }

    #[test]
    fn parses_all_fields() {
        let cfg = parse(&fixture()).unwrap();
        assert_eq!(cfg.polymarket_private_key,    "0xdeadbeef");
        assert_eq!(cfg.polymarket_api_key,        "test-api-key");
        assert_eq!(cfg.polymarket_api_secret,     "test-api-secret");
        assert_eq!(cfg.polymarket_api_passphrase, "test-passphrase");
        // Default signature type is EOA, no funder needed
        assert_eq!(cfg.polymarket_signature_type, SignatureType::Eoa);
        assert!(cfg.polymarket_funder_address.is_none());
        // EXECUTION_ENABLED defaults to false (observe-mode safety)
        assert!(!cfg.execution_enabled);
        assert_eq!(cfg.alchemy_polygon_key,       "test-alchemy-key");
        assert_eq!(cfg.anthropic_api_key.as_deref(), Some("test-anthropic-key"));
        assert!(cfg.dry_run);
        assert_eq!(cfg.max_bet_dollars,           30.0);
        assert_eq!(cfg.bankroll,                  1500.0);
        assert_eq!(cfg.min_edge,                  0.05);
        assert_eq!(cfg.kelly_fraction,            0.25);
        assert_eq!(cfg.daily_loss_limit_dollars,  75.0);
        assert_eq!(cfg.weekly_loss_limit_dollars, 150.0);
        assert_eq!(cfg.max_open_exposure_dollars, 300.0);
        assert_eq!(cfg.min_liquidity,             500.0);
        assert_eq!(cfg.oracle_arb_threshold,      0.04);
        assert_eq!(cfg.scan_interval_ms,          500);
        assert_eq!(cfg.log_level,                 "info");
    }

    #[test]
    fn anthropic_key_is_optional() {
        let mut env = fixture();
        env.remove("ANTHROPIC_API_KEY");
        assert!(parse(&env).unwrap().anthropic_api_key.is_none());
    }

    #[test]
    fn defaults_apply_when_optional_vars_absent() {
        let mut env = fixture();
        for k in ["DRY_RUN", "MAX_BET_DOLLARS", "SCAN_INTERVAL_MS", "LOG_LEVEL"] {
            env.remove(k);
        }
        let cfg = parse(&env).unwrap();
        assert!(cfg.dry_run);
        assert_eq!(cfg.max_bet_dollars, 30.0);
        assert_eq!(cfg.scan_interval_ms, 500);
        assert_eq!(cfg.log_level, "info");
    }

    #[test]
    fn missing_required_field_errors_with_key_name() {
        let mut env = fixture();
        env.remove("POLYMARKET_PRIVATE_KEY");
        let err = parse(&env).unwrap_err().to_string();
        assert!(err.contains("POLYMARKET_PRIVATE_KEY"), "error was: {err}");
    }

    #[test]
    fn invalid_bool_is_rejected() {
        let mut env = fixture();
        env.insert("DRY_RUN".to_string(), "yes".to_string());
        assert!(parse(&env).is_err());
    }

    #[test]
    fn invalid_number_is_rejected() {
        let mut env = fixture();
        env.insert("MAX_BET_DOLLARS".to_string(), "thirty".to_string());
        assert!(parse(&env).is_err());
    }

    #[test]
    fn dry_run_false_parses() {
        let mut env = fixture();
        env.insert("DRY_RUN".to_string(), "false".to_string());
        assert!(!parse(&env).unwrap().dry_run);
    }

    #[test]
    fn poly_proxy_sigtype_parses_with_funder() {
        let mut env = fixture();
        env.insert("POLYMARKET_SIGNATURE_TYPE".into(), "POLY_PROXY".into());
        env.insert("POLYMARKET_FUNDER_ADDRESS".into(), "0xabc".into());
        let cfg = parse(&env).unwrap();
        assert_eq!(cfg.polymarket_signature_type, SignatureType::PolyProxy);
        assert_eq!(cfg.polymarket_funder_address.as_deref(), Some("0xabc"));
    }

    #[test]
    fn poly_proxy_sigtype_without_funder_is_rejected() {
        let mut env = fixture();
        env.insert("POLYMARKET_SIGNATURE_TYPE".into(), "POLY_PROXY".into());
        // No POLYMARKET_FUNDER_ADDRESS — should fail
        let err = parse(&env).unwrap_err().to_string();
        assert!(err.contains("POLYMARKET_FUNDER_ADDRESS"), "error was: {err}");
    }

    #[test]
    fn invalid_sigtype_is_rejected() {
        let mut env = fixture();
        env.insert("POLYMARKET_SIGNATURE_TYPE".into(), "WAT".into());
        assert!(parse(&env).is_err());
    }

    #[test]
    fn execution_enabled_parses_from_env() {
        let mut env = fixture();
        env.insert("EXECUTION_ENABLED".into(), "true".into());
        assert!(parse(&env).unwrap().execution_enabled);
        env.insert("EXECUTION_ENABLED".into(), "false".into());
        assert!(!parse(&env).unwrap().execution_enabled);
    }

    #[test]
    fn empty_funder_address_treated_as_unset() {
        let mut env = fixture();
        env.insert("POLYMARKET_FUNDER_ADDRESS".into(), "".into());
        let cfg = parse(&env).unwrap();
        assert!(cfg.polymarket_funder_address.is_none());
    }
}
