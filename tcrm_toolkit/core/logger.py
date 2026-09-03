"""Centralized logging configuration for TCRM Toolkit."""

import logging
from pathlib import Path
import structlog


def setup_logging(log_file: Path | None = None) -> None:
    """Configure structured logging to stderr and a persistent log file."""
    log_file = log_file or (Path.home() / ".tcrm" / "tcrm.log")
    try:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
    except Exception:
        file_handler = None

    handlers = [logging.StreamHandler()]
    if file_handler:
        handlers.append(file_handler)

    logging.basicConfig(
        level=logging.INFO,
        handlers=handlers,
        force=True,
    )

    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
