"""RerunManager — selective re-execution of failures from a prior run."""

from __future__ import annotations

from dvf.audit.logger import get_logger
from dvf.audit.models import ValidationStatus
from dvf.core.engine import RunSummary, ValidationEngine
from dvf.metadata.models import RunRequest

_log = get_logger("core.rerun")

_FAILURE_STATUSES = {ValidationStatus.FAILED, ValidationStatus.ERROR,
                     ValidationStatus.TIMEOUT}


class RerunManager:
    def __init__(self, engine: ValidationEngine):
        self.engine = engine

    # ------------------------------------------------------------------ #
    def failed_validation_ids(self, run_id: str) -> list[str]:
        """Validations whose LATEST attempt in run_id ended in failure."""
        rows = self.engine.backend.read_rows("audit", "validation_run_audit",
                                             where_eq={"run_id": run_id})
        latest: dict[str, dict] = {}
        for row in rows:
            vid = row["validation_id"]
            best = latest.get(vid)
            if best is None or (row.get("attempt") or 0) > (best.get("attempt") or 0):
                latest[vid] = row
        return sorted(vid for vid, row in latest.items()
                      if row.get("status") in _FAILURE_STATUSES)

    def rerun_failed(self, run_id: str, triggered_by: str | None = None,
                     run_name: str | None = None) -> RunSummary | None:
        """New run (trigger_type=RERUN, parent_run_id=run_id) covering only the
        failures of run_id. Returns None when there is nothing to rerun."""
        failed = self.failed_validation_ids(run_id)
        if not failed:
            _log.info("rerun_failed(%s): nothing to rerun", run_id)
            return None
        _log.info("rerun_failed(%s): re-executing %d validation(s): %s",
                  run_id, len(failed), ", ".join(failed))
        return self.engine.run(RunRequest(
            run_name=run_name or f"rerun of {run_id}",
            trigger_type="RERUN",
            triggered_by=triggered_by,
            validation_ids=failed,
            parent_run_id=run_id,
        ))

    # ------------------------------------------------------------------ #
    def resume(self, run_id: str, triggered_by: str | None = None) -> RunSummary:
        """Continue an interrupted run: everything not yet PASSED re-executes."""
        return self.engine.run(RunRequest(
            run_name=f"resume of {run_id}",
            trigger_type="RERUN",
            triggered_by=triggered_by,
            resume_run_id=run_id,
        ))
