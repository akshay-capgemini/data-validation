"""Time helpers — all framework timestamps are timezone-aware UTC."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(ts: datetime | None) -> str | None:
    return ts.isoformat(timespec="microseconds") if ts else None


def duration_sec(start: datetime, end: datetime) -> float:
    return round((end - start).total_seconds(), 3)


def new_run_id(now: datetime | None = None) -> str:
    """R{yyyyMMddHHmmss}_{uuid8} — sortable, unique."""
    now = now or utcnow()
    return f"R{now.strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"
