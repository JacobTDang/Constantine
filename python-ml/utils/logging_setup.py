from __future__ import annotations

import logging
import sys
from typing import Optional

from rich.console import Console
from rich.logging import RichHandler


def setup_logging(level: str = "info", *, log_file: Optional[str] = None) -> None:
    """Configure the root logger for the entire Python ML process.

    Uses RichHandler for coloured, timestamped console output.
    Optionally mirrors output to a plain-text log file (for monitoring).
    Silences noisy third-party loggers to keep output readable.
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    handlers: list[logging.Handler] = [
        RichHandler(
            console=Console(stderr=False),
            show_time=True,
            show_level=True,
            show_path=True,
            markup=False,
            rich_tracebacks=True,
            tracebacks_show_locals=False,
        )
    ]

    if log_file:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        handlers.append(file_handler)

    logging.basicConfig(
        level=numeric_level,
        format="%(message)s",
        datefmt="[%H:%M:%S]",
        handlers=handlers,
        force=True,  # replace any handlers set by imported libraries
    )

    # Silence noisy third-party loggers
    for noisy in ("websockets", "aiohttp", "asyncio", "urllib3", "httpx"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Return a named logger; call setup_logging() first."""
    return logging.getLogger(name)
