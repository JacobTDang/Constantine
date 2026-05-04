use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct Config {
    // Polymarket credentials
    pub polymarket_private_key: String,
    pub polymarket_api_key: String,
    pub polymarket_api_secret: String,
    pub polymarket_api_passphrase: String,

    // RPC
    pub alchemy_polygon_key: String,

    // Optional NLP
    pub anthropic_api_key: Option<String>,

    // Trading behaviour
    pub dry_run: bool,
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

        Ok(Self {
            polymarket_private_key:   req("POLYMARKET_PRIVATE_KEY")?,
            polymarket_api_key:       req("POLYMARKET_API_KEY")?,
            polymarket_api_secret:    req("POLYMARKET_API_SECRET")?,
            polymarket_api_passphrase:req("POLYMARKET_API_PASSPHRASE")?,
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
}
