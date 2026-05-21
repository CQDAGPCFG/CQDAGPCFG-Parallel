from __future__ import annotations

import json
import logging
import os
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any


DEFAULT_LOGGER_NAME = "cqdagpcfg"
DEFAULT_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def configure_framework_logging(
    *,
    level: str | int | None = None,
    log_format: str | None = None,
) -> None:
    """Install a conservative default logger for framework-managed services."""

    resolved_level = _resolve_level(level or os.environ.get("CQPCFG_LOG_LEVEL", "INFO"))
    logger = logging.getLogger(DEFAULT_LOGGER_NAME)
    logger.setLevel(resolved_level)

    root = logging.getLogger()
    if root.handlers:
        return

    handler = logging.StreamHandler()
    resolved_format = log_format or os.environ.get("CQPCFG_LOG_FORMAT", "text")
    if resolved_format.lower() == "json":
        handler.setFormatter(_JsonLogFormatter())
    else:
        handler.setFormatter(logging.Formatter(DEFAULT_LOG_FORMAT))
    root.addHandler(handler)
    root.setLevel(resolved_level)


def log_event(
    logger: logging.Logger,
    level: int,
    event: str,
    **fields: Any,
) -> None:
    if not logger.isEnabledFor(level):
        return
    logger.log(level, _format_event(event, fields))


def _format_event(event: str, fields: dict[str, Any]) -> str:
    parts = [f"event={event}"]
    for key in sorted(fields):
        value = fields[key]
        if value is None:
            continue
        parts.append(f"{key}={_stringify(value)}")
    return " ".join(parts)


def _stringify(value: Any) -> str:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)):
        return str(value)
    if is_dataclass(value) and not isinstance(value, type):
        return json.dumps(asdict(value), sort_keys=True, default=str)
    if isinstance(value, (tuple, list, dict)):
        return json.dumps(value, sort_keys=True, default=str)
    return str(value)


def _resolve_level(level: str | int) -> int:
    if isinstance(level, int):
        return level
    resolved = logging.getLevelName(level.upper())
    if isinstance(resolved, int):
        return resolved
    raise ValueError(f"invalid CQPCFG log level: {level}")


class _JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(
            {
                "time": self.formatTime(record),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
            },
            ensure_ascii=False,
            sort_keys=True,
        )


__all__ = [
    "configure_framework_logging",
    "log_event",
]
