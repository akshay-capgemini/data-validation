"""ExecutionPlanner — priority levels + ordered submission with barrier support.

Scheduling model (kept deliberately one-dimensional):
* distinct `priority` values ascending = sequential DAG levels
* within a level, items sorted by `execution_order` are submitted to a thread
  pool of `max_parallel` workers
* an item with parallel_flag=False is a BARRIER: the level drains, the item
  runs alone, then submission resumes
(`execution_group` is a selection dimension, not a scheduling one.)
"""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Callable, Sequence, TypeVar

from dvf.metadata.models import ValidationConfig

T = TypeVar("T")


@dataclass(frozen=True)
class ExecutionLevel:
    priority: int
    items: tuple[ValidationConfig, ...]


class ExecutionPlanner:
    @staticmethod
    def plan(configs: Sequence[ValidationConfig]) -> list[ExecutionLevel]:
        by_priority: dict[int, list[ValidationConfig]] = {}
        for c in configs:
            by_priority.setdefault(c.priority, []).append(c)
        levels = []
        for priority in sorted(by_priority):
            items = sorted(by_priority[priority],
                           key=lambda c: (c.execution_order, c.validation_id))
            levels.append(ExecutionLevel(priority=priority, items=tuple(items)))
        return levels


def run_level(items: Sequence[ValidationConfig],
              runner: Callable[[ValidationConfig], T],
              max_workers: int) -> list[T]:
    """Execute one level honoring order + barriers; results in submission order."""
    results: list[T] = []
    if not items:
        return results
    max_workers = max(1, max_workers)
    with ThreadPoolExecutor(max_workers=max_workers,
                            thread_name_prefix="dvf-worker") as pool:
        futures: list[Future] = []

        def drain() -> None:
            for f in futures:
                results.append(f.result())
            futures.clear()

        for item in items:
            if item.parallel_flag:
                futures.append(pool.submit(runner, item))
            else:
                drain()                       # barrier: wait for in-flight work
                results.append(runner(item))  # run alone on the caller thread
        drain()
    return results
