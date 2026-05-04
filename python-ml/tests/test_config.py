import pytest
from pathlib import Path
from utils.config import Config

FIXTURE = Path(__file__).parent / "fixtures" / ".env.test"


def fixture_env() -> dict:
    return {
        "POLYMARKET_PRIVATE_KEY":    "0xdeadbeef",
        "POLYMARKET_API_KEY":        "test-api-key",
        "POLYMARKET_API_SECRET":     "test-api-secret",
        "POLYMARKET_API_PASSPHRASE": "test-passphrase",
        "ALCHEMY_POLYGON_KEY":       "test-alchemy-key",
        "ANTHROPIC_API_KEY":         "test-anthropic-key",
        "DRY_RUN":                   "true",
        "MAX_BET_DOLLARS":           "30",
        "BANKROLL":                  "1500",
        "MIN_EDGE":                  "0.05",
        "KELLY_FRACTION":            "0.25",
        "DAILY_LOSS_LIMIT_DOLLARS":  "75",
        "WEEKLY_LOSS_LIMIT_DOLLARS": "150",
        "MAX_OPEN_EXPOSURE_DOLLARS": "300",
        "MIN_LIQUIDITY":             "500",
        "ORACLE_ARB_THRESHOLD":      "0.04",
        "SCAN_INTERVAL_MS":          "500",
        "LOG_LEVEL":                 "info",
    }


def test_parses_all_fields():
    cfg = Config._parse(fixture_env())
    assert cfg.polymarket_private_key    == "0xdeadbeef"
    assert cfg.polymarket_api_key        == "test-api-key"
    assert cfg.polymarket_api_secret     == "test-api-secret"
    assert cfg.polymarket_api_passphrase == "test-passphrase"
    assert cfg.alchemy_polygon_key       == "test-alchemy-key"
    assert cfg.anthropic_api_key         == "test-anthropic-key"
    assert cfg.dry_run                   is True
    assert cfg.max_bet_dollars           == 30.0
    assert cfg.bankroll                  == 1500.0
    assert cfg.min_edge                  == 0.05
    assert cfg.kelly_fraction            == 0.25
    assert cfg.daily_loss_limit_dollars  == 75.0
    assert cfg.weekly_loss_limit_dollars == 150.0
    assert cfg.max_open_exposure_dollars == 300.0
    assert cfg.min_liquidity             == 500.0
    assert cfg.oracle_arb_threshold      == 0.04
    assert cfg.scan_interval_ms          == 500
    assert cfg.log_level                 == "info"


def test_anthropic_key_is_optional():
    env = fixture_env()
    del env["ANTHROPIC_API_KEY"]
    cfg = Config._parse(env)
    assert cfg.anthropic_api_key is None


def test_defaults_apply_when_optional_vars_absent():
    env = fixture_env()
    for k in ("DRY_RUN", "MAX_BET_DOLLARS", "SCAN_INTERVAL_MS", "LOG_LEVEL"):
        del env[k]
    cfg = Config._parse(env)
    assert cfg.dry_run          is True
    assert cfg.max_bet_dollars  == 30.0
    assert cfg.scan_interval_ms == 500
    assert cfg.log_level        == "info"


def test_missing_required_field_raises_with_key_name():
    env = fixture_env()
    del env["POLYMARKET_PRIVATE_KEY"]
    with pytest.raises(ValueError, match="POLYMARKET_PRIVATE_KEY"):
        Config._parse(env)


def test_invalid_bool_raises():
    env = fixture_env()
    env["DRY_RUN"] = "yes"
    with pytest.raises(ValueError, match="DRY_RUN"):
        Config._parse(env)


def test_invalid_float_raises():
    env = fixture_env()
    env["MAX_BET_DOLLARS"] = "thirty"
    with pytest.raises(ValueError, match="MAX_BET_DOLLARS"):
        Config._parse(env)


def test_invalid_int_raises():
    env = fixture_env()
    env["SCAN_INTERVAL_MS"] = "half-a-second"
    with pytest.raises(ValueError, match="SCAN_INTERVAL_MS"):
        Config._parse(env)


def test_dry_run_false_parses():
    env = fixture_env()
    env["DRY_RUN"] = "false"
    assert Config._parse(env).dry_run is False


def test_from_env_loads_fixture_file():
    cfg = Config.from_env(env_file=FIXTURE)
    assert cfg.polymarket_private_key == "0xdeadbeef"
    assert cfg.dry_run is True
    assert cfg.scan_interval_ms == 500
