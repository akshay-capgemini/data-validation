"""Structured logging with correlation IDs.

Every log line carries correlation_id = run_id:validation_id:execution_id
(bound via contextvars so it survives thread pools when set inside the task).
"""

from __future__ import annotations

import contextvars
import json
import logging
import sys
from contextlib import contextmanager

from dvf.config.settings import LoggingSettings
from dvf.utils.timeutil import iso, utcnow

_correlation: contextvars.ContextVar[str] = contextvars.ContextVar("dvf_correlation", default="-")
_ROOT = "dvf"
_configured = False


def correlation_id() -> str:
    return _correlation.get()


def set_correlation(run_id: str | None = None, validation_id: str | None = None,
                    execution_id: str | None = None) -> None:
    parts = [p for p in (run_id, validation_id, execution_id) if p]
    _correlation.set(":".join(parts) if parts else "-")


@contextmanager
def correlation_scope(run_id: str | None = None, validation_id: str | None = None,
                      execution_id: str | None = None):
    parts = [p for p in (run_id, validation_id, execution_id) if p]
    token = _correlation.set(":".join(parts) if parts else "-")
    try:
        yield
    finally:
        _correlation.reset(token)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": iso(utcnow()),
            "level": record.levelname,
            "logger": record.name,
            "correlation_id": correlation_id(),
            "msg": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0]:
            payload["exception"] = self.formatException(record.exc_info)
        extra = getattr(record, "dvf", None)
        if isinstance(extra, dict):
            payload.update(extra)
        return json.dumps(payload, default=str)


class PlainFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base = (f"{iso(utcnow())} {record.levelname:<7} [{correlation_id()}] "
                f"{record.name}: {record.getMessage()}")
        if record.exc_info and record.exc_info[0]:
            base += "\n" + self.formatException(record.exc_info)
        return base


def configure_logging(settings: LoggingSettings | None = None, force: bool = False) -> None:
    global _configured
    if _configured and not force:
        return
    settings = settings or LoggingSettings()
    root = logging.getLogger(_ROOT)
    root.setLevel(getattr(logging, settings.level.upper(), logging.INFO))
    root.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter() if settings.json_format else PlainFormatter())
    root.addHandler(handler)
    root.propagate = False
    _configured = True


def get_logger(name: str) -> logging.Logger:
    configure_logging()
    return logging.getLogger(f"{_ROOT}.{name}" if not name.startswith(_ROOT) else name)
