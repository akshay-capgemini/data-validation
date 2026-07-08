"""AuditWriter — single background writer thread with micro-batching.

All audit/result Delta writes from N validation worker threads funnel through
one queue and one writer thread: concurrent Delta MERGEs conflict, a single
writer never does, and tasks stay non-blocking. Ops are processed strictly in
queue order; consecutive appends to the same table are batched into one write.
"""

from __future__ import annotations

import queue
import threading
from dataclasses import dataclass, field
from typing import Any, Mapping

from dvf.audit.logger import get_logger
from dvf.metadata.backend import StorageBackend

_log = get_logger("audit.writer")


@dataclass
class _Op:
    kind: str                      # append | merge | flush
    schema: str = ""
    table: str = ""
    row: Mapping[str, Any] | None = None
    event: threading.Event | None = None


class AuditWriter:
    def __init__(self, backend: StorageBackend, *,
                 batch_rows: int = 500, flush_interval_s: float = 5.0):
        self.backend = backend
        self.batch_rows = max(1, batch_rows)
        self.flush_interval_s = max(0.1, flush_interval_s)
        self.errors: list[Exception] = []
        self._q: queue.Queue[_Op | None] = queue.Queue()
        self._thread: threading.Thread | None = None
        self._started = False

    # ------------------------------------------------------------------ #
    def start(self) -> None:
        if self._started:
            return
        self._thread = threading.Thread(target=self._worker, name="dvf-audit-writer",
                                        daemon=True)
        self._thread.start()
        self._started = True

    def append(self, schema: str, table: str, row: Mapping[str, Any] | Any) -> None:
        self._q.put(_Op("append", schema, table, self._to_row(row)))

    def merge(self, schema: str, table: str, row: Mapping[str, Any] | Any) -> None:
        self._q.put(_Op("merge", schema, table, self._to_row(row)))

    def flush(self, timeout_s: float = 300.0) -> None:
        """Block until everything queued so far is persisted."""
        if not self._started:
            return
        event = threading.Event()
        self._q.put(_Op("flush", event=event))
        if not event.wait(timeout=timeout_s):
            raise TimeoutError("AuditWriter flush timed out")

    def stop(self, timeout_s: float = 300.0) -> None:
        if not self._started:
            return
        self.flush(timeout_s=timeout_s)
        self._q.put(None)
        assert self._thread is not None
        self._thread.join(timeout=timeout_s)
        self._started = False

    # ------------------------------------------------------------------ #
    @staticmethod
    def _to_row(row: Mapping[str, Any] | Any) -> Mapping[str, Any]:
        if hasattr(row, "model_dump"):
            return row.model_dump()
        return dict(row)

    def _worker(self) -> None:
        pending: list[_Op] = []
        while True:
            try:
                op = self._q.get(timeout=self.flush_interval_s)
            except queue.Empty:
                self._drain(pending)
                pending = []
                continue
            if op is None:                       # shutdown sentinel
                self._drain(pending)
                return
            if op.kind == "flush":
                self._drain(pending)
                pending = []
                op.event.set()
                continue
            pending.append(op)
            if len(pending) >= self.batch_rows:
                self._drain(pending)
                pending = []

    def _drain(self, ops: list[_Op]) -> None:
        """Persist ops in order; batch consecutive appends to the same table."""
        i = 0
        while i < len(ops):
            op = ops[i]
            try:
                if op.kind == "append":
                    j = i
                    rows = []
                    while (j < len(ops) and ops[j].kind == "append"
                           and ops[j].schema == op.schema and ops[j].table == op.table):
                        rows.append(dict(ops[j].row))
                        j += 1
                    self.backend.append_rows(op.schema, op.table, rows)
                    i = j
                    continue
                if op.kind == "merge":
                    self.backend.merge_rows(op.schema, op.table, [dict(op.row)])
            except Exception as exc:  # noqa: BLE001 — keep the run alive; surface via .errors
                self.errors.append(exc)
                _log.error("audit write failed for %s.%s: %s", op.schema, op.table, exc,
                           exc_info=True)
            i += 1
