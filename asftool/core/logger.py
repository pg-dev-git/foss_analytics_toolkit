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
    # Logging must not depend on the rest of the configuration being
    # valid: an invalid ENCRYPTION_KEY would otherwise stop the logger
    # from being set up, which is the one thing that can still work.
    try:
        settings = get_settings()
        default_log_file = settings.log_file
        default_level = settings.log_level
    except Exception:
        default_log_file = Path.home() / ".asftool" / "asftool.log"
        default_level = "INFO"

    log_path = log_file or default_log_file
    log_level = (level or default_level).upper()

    # Ensure log directory exists
    log_path.parent.mkdir(parents=True, exist_ok=True)

    # Standard library logging configuration
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stderr,
        level=getattr(logging, log_level, logging.INFO),
    )
    # basicConfig is a no-op when the root logger already has handlers
    # (an embedding application, or a test runner), so set the level
    # explicitly: otherwise the file handler is attached but never fires.
    root = logging.getLogger()
    root.setLevel(getattr(logging, log_level, logging.INFO))

    # File handler — append mode
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(getattr(logging, log_level, logging.INFO))
    file_handler.setFormatter(logging.Formatter("%(message)s"))
    root.addHandler(file_handler)

    # structlog configuration.
    #
    # The logger factory is the stdlib one, not PrintLoggerFactory: the
    # latter writes straight to the stream and bypasses the stdlib
    # handlers, so the file handler above never saw a record and
    # ~/.asftool/asftool.log stayed empty. Routing through the stdlib
    # logger sends each record to both the stderr handler and the file
    # handler.
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
        logger_factory=structlog.stdlib.LoggerFactory(),
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
