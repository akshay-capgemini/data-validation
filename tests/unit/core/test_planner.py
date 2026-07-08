"""ExecutionPlanner — level grouping, ordering, barrier semantics, parallelism."""

from __future__ import annotations

import threading
import time

from dvf.core.planner import ExecutionPlanner, run_level
from dvf.metadata.models import ValidationConfig


def _cfg(vid: str, priority: int = 100, order: int = 100,
         parallel: bool = True) -> ValidationConfig:
    return ValidationConfig(validation_id=vid, source_connection="S",
                            target_connection="T", priority=priority,
                            execution_order=order, parallel_flag=parallel)


def test_plan_levels_and_ordering():
    configs = [_cfg("C", priority=2, order=1), _cfg("B", priority=1, order=20),
               _cfg("A", priority=1, order=10), _cfg("D", priority=3)]
    levels = ExecutionPlanner.plan(configs)
    assert [lvl.priority for lvl in levels] == [1, 2, 3]
    assert [c.validation_id for c in levels[0].items] == ["A", "B"]
    assert [c.validation_id for c in levels[1].items] == ["C"]


def test_run_level_parallelism_and_barrier():
    """A,B must run concurrently (proven via a 2-party barrier); C has
    parallel_flag=False and must run strictly alone; D resumes after C."""
    items = [_cfg("A"), _cfg("B"), _cfg("C", parallel=False), _cfg("D")]
    sync = threading.Barrier(2, timeout=10)
    lock = threading.Lock()
    active = 0
    observed: dict[str, int] = {}
    order: list[str] = []

    def runner(cfg):
        nonlocal active
        with lock:
            active += 1
            observed[cfg.validation_id] = active
            order.append(cfg.validation_id)
        if cfg.validation_id in ("A", "B"):
            sync.wait()                    # both must be in-flight together
        time.sleep(0.02)
        with lock:
            active -= 1
        return cfg.validation_id

    results = run_level(items, runner, max_workers=4)
    assert results == ["A", "B", "C", "D"]     # submission order preserved
    assert observed["C"] == 1                  # barrier ran with nothing in flight
    assert order.index("C") > order.index("A") and order.index("C") > order.index("B")
    assert order.index("D") > order.index("C")


def test_run_level_single_worker_still_completes():
    items = [_cfg("A"), _cfg("B", parallel=False), _cfg("C")]
    results = run_level(items, lambda c: c.validation_id, max_workers=1)
    assert results == ["A", "B", "C"]
