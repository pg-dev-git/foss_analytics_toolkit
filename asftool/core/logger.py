"""Centralized structured JSON logging for ASFTool.

Writes concurrently to stderr and persistent log file `~/.asftool/asftool.log`.
Uses structlog with JSON renderer for production-grade observability.
"""

import logging
import sys
from pathlib import Path

import structlog

from asftool.core.config import get_settings


def setup_logging(log_file: Path | None = None, level: str | None = None) -> None:
    """Configure structlog with JSON output to stderr and a file.

    Args:
        log_file: Optional path to log file. Defaults to settings.log_file.
        level: Optional log level. Defaults to settings.log_level.
    """
    settings = get_settings()
    log_path = log_file or settings.log_file
    log_level = (level or settings.log_level).upper()

    # Ensure log directory exists
    log_path.parent.mkdir(parents=True, exist_ok=True)

    # Standard library logging configuration
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stderr,
        level=getattr(logging, log_level, logging.INFO),
    )

    # File handler — append mode
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(getattr(logging, log_level, logging.INFO))
    file_handler.setFormatter(logging.Formatter("%(message)s"))
    logging.getLogger().addHandler(file_handler)

    # structlog configuration
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, log_level, logging.INFO)
        ),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Get a configured structlog logger.

    Args:
        name: Optional logger name. Defaults to caller's module.

    Returns:
        Configured structlog logger.
    """
    return structlog.get_logger(name)  # type: ignore[no-any-return]
