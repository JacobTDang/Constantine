from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


@dataclass(frozen=True)
class Config:
    # Polymarket credentials
    polymarket_private_key: str
    polymarket_api_key: str
    polymarket_api_secret: str
    polymarket_api_passphrase: str

    # RPC
    alchemy_polygon_key: str

    # Optional NLP
    anthropic_api_key: Optional[str]

    # Trading behaviour
    dry_run: bool
    max_bet_dollars: float
    bankroll: float
    min_edge: float
    kelly_fraction: float
    daily_loss_limit_dollars: float
    weekly_loss_limit_dollars: float
    max_open_exposure_dollars: float
    min_liquidity: float
    oracle_arb_threshold: float
    scan_interval_ms: int

    # Logging
    log_level: str

    @classmethod
    def from_env(cls, env_file: Optional[Path] = None) -> "Config":
        if env_file is not None:
            load_dotenv(env_file, override=True)
        else:
            load_dotenv()
        return cls._parse(os.environ)

    @classmethod
    def _parse(cls, env: dict[str, str]) -> "Config":
        def req(key: str) -> str:
            val = env.get(key)
            if val is None:
                raise ValueError(f"Missing required env var: {key}")
            return val

        def get_float(key: str, default: float) -> float:
            val = env.get(key)
            if val is None:
                return default
            try:
                return float(val)
            except ValueError:
                raise ValueError(f"{key} must be a number, got {val!r}")

        def get_int(key: str, default: int) -> int:
            val = env.get(key)
            if val is None:
                return default
            try:
                return int(val)
            except ValueError:
                raise ValueError(f"{key} must be an integer, got {val!r}")

        def get_bool(key: str, default: bool) -> bool:
            val = env.get(key)
            if val is None:
                return default
            if val.lower() == "true":
                return True
            if val.lower() == "false":
                return False
            raise ValueError(f"{key} must be 'true' or 'false', got {val!r}")

        return cls(
            polymarket_private_key=req("POLYMARKET_PRIVATE_KEY"),
            polymarket_api_key=req("POLYMARKET_API_KEY"),
            polymarket_api_secret=req("POLYMARKET_API_SECRET"),
            polymarket_api_passphrase=req("POLYMARKET_API_PASSPHRASE"),
            alchemy_polygon_key=req("ALCHEMY_POLYGON_KEY"),
            anthropic_api_key=env.get("ANTHROPIC_API_KEY"),
            dry_run=get_bool("DRY_RUN", True),
            max_bet_dollars=get_float("MAX_BET_DOLLARS", 30.0),
            bankroll=get_float("BANKROLL", 1500.0),
            min_edge=get_float("MIN_EDGE", 0.05),
            kelly_fraction=get_float("KELLY_FRACTION", 0.25),
            daily_loss_limit_dollars=get_float("DAILY_LOSS_LIMIT_DOLLARS", 75.0),
            weekly_loss_limit_dollars=get_float("WEEKLY_LOSS_LIMIT_DOLLARS", 150.0),
            max_open_exposure_dollars=get_float("MAX_OPEN_EXPOSURE_DOLLARS", 300.0),
            min_liquidity=get_float("MIN_LIQUIDITY", 500.0),
            oracle_arb_threshold=get_float("ORACLE_ARB_THRESHOLD", 0.04),
            scan_interval_ms=get_int("SCAN_INTERVAL_MS", 500),
            log_level=env.get("LOG_LEVEL", "info"),
        )
