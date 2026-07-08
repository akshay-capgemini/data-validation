"""Retry helper — retries only transient failures, with exponential backoff."""

from __future__ import annotations

import time
from typing import Callable, TypeVar

from dvf.core.exceptions import is_transient

T = TypeVar("T")


def retry_transient(fn: Callable[[], T], *, retry_max: int, backoff_s: float,
                    on_retry: Callable[[int, BaseException], None] | None = None,
                    sleep: Callable[[float], None] = time.sleep) -> tuple[T, int]:
    """Call fn; on transient failure retry up to retry_max times.

    Returns (result, retries_used). Non-transient errors propagate immediately.
    """
    attempt = 0
    while True:
        try:
            return fn(), attempt
        except BaseException as exc:  # noqa: BLE001 — classification decides
            if attempt >= retry_max or not is_transient(exc):
                raise
            attempt += 1
            if on_retry:
                on_retry(attempt, exc)
            sleep(backoff_s * (2 ** (attempt - 1)))
