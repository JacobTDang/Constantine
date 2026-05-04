import logging
import pytest
from utils.logging_setup import setup_logging, get_logger


def test_setup_does_not_raise():
    setup_logging("info")


def test_double_setup_does_not_raise():
    setup_logging("info")
    setup_logging("debug")


def test_log_level_applied():
    setup_logging("warning")
    logger = get_logger("test.level")
    assert logger.getEffectiveLevel() <= logging.WARNING


def test_get_logger_returns_named_logger():
    setup_logging("info")
    logger = get_logger("polymarket.test")
    assert logger.name == "polymarket.test"


def test_emitting_all_levels_does_not_raise(capsys):
    setup_logging("debug")
    log = get_logger("test.emit")
    log.debug("debug message")
    log.info("info message value=%d", 42)
    log.warning("warning message")
    log.error("error message")


def test_noisy_loggers_suppressed():
    setup_logging("debug")
    for name in ("websockets", "aiohttp", "asyncio", "urllib3", "httpx"):
        assert logging.getLogger(name).level == logging.WARNING


def test_log_to_file(tmp_path):
    log_file = tmp_path / "test.log"
    setup_logging("info", log_file=str(log_file))
    get_logger("test.file").info("written to file")
    content = log_file.read_text(encoding="utf-8")
    assert "written to file" in content
