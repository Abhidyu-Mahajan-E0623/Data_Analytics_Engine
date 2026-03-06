"""Structured logging configuration."""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

import orjson

from src.utils.io import OUTPUT_LOGS_DIR, ensure_project_dirs
from src.utils.time import utc_iso

LOGGER_NAME = "schema_maker"


class JsonLogFormatter(logging.Formatter):
    """Compact JSON formatter."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": utc_iso(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key in ("run_id", "domain", "hypothesis_id"):
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return orjson.dumps(payload).decode("utf-8")


def configure_logging(run_id: str | None = None, level: int = logging.INFO) -> logging.Logger:
    """Configure a run-scoped logger writing to Output/logs."""
    ensure_project_dirs()
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level)
    logger.handlers.clear()
    logger.propagate = False

    log_name = f"{run_id}.log" if run_id else "session.log"
    log_path = OUTPUT_LOGS_DIR / log_name

    file_handler = RotatingFileHandler(
        filename=Path(log_path),
        mode="a",
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(JsonLogFormatter())
    file_handler.setLevel(level)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)
    stream_handler.setFormatter(
        logging.Formatter("[%(asctime)s] %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
    )

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger
