"""ValidationEngine — the run orchestrator.

run(request): create run audit -> load & expand metadata -> execute validations
-> checkpoint everything through the AuditWriter -> finalize rollup.
Phase 1 executes sequentially; the planner/thread-pool lands in phase 2 behind
the same _execute() seam.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import dvf
import dvf.validators  # noqa: F401 — registers all validator classes
from dvf.audit.logger import configure_logging, correlation_scope, get_logger
from dvf.audit.models import RunAudit, RunStatus, ValidationRunAudit, ValidationStatus
from dvf.audit.writer import AuditWriter
from dvf.config.settings import FrameworkSettings
from dvf.connectors.factory import ConnectorFactory
from dvf.core.context import RunContext
from dvf.core.exceptions import MetadataError
from dvf.core.planner import ExecutionPlanner, run_level
from dvf.core.task import ValidationTask
from dvf.metadata.backend import StorageBackend
from dvf.metadata.models import RunRequest, ValidationConfig
from dvf.metadata.repository import MetadataRepository
from dvf.sqlgen.generator import SQLGenerator
from dvf.utils.secrets import SecretResolver
from dvf.utils.timeutil import duration_sec, new_id, new_run_id, utcnow
from dvf.validators.profiles import ProfileExpander

_log = get_logger("core.engine")


@dataclass
class RunSummary:
    run_id: str
    status: str
    total: int = 0
    passed: int = 0
    failed: int = 0
    warnings: int = 0
    errors: int = 0
    skipped: int = 0
    duration_sec: float = 0.0
    validations: list[ValidationRunAudit] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.status == RunStatus.SUCCEEDED


class ValidationEngine:
    def __init__(self, settings: FrameworkSettings, backend: StorageBackend,
                 spark=None, dbutils=None, secrets: SecretResolver | None = None):
        configure_logging(settings.logging)
        self.settings = settings
        self.backend = backend
        self.spark = spark
        self.repo = MetadataRepository(backend)
        self.writer = AuditWriter(backend,
                                  batch_rows=settings.audit.batch_rows,
                                  flush_interval_s=settings.audit.flush_interval_s)
        self.factory = ConnectorFactory(secrets or SecretResolver(dbutils=dbutils),
                                        spark=spark)
        self.sqlgen = SQLGenerator(settings)
        self.expander = ProfileExpander(self.repo)

    # ------------------------------------------------------------------ #
    def run(self, request: RunRequest | None = None) -> RunSummary:
        request = request or RunRequest()
        run_id = new_run_id()
        start = utcnow()
        self.writer.start()

        run_audit = RunAudit(
            run_id=run_id,
            run_name=request.run_name,
            trigger_type=request.trigger_type,
            parent_run_id=request.parent_run_id or request.resume_run_id,
            triggered_by=request.triggered_by,
            group_filter=",".join(request.execution_groups) or None,
            validation_id_filter=",".join(request.validation_ids) or None,
            profile_override=request.profile_override,
            status=RunStatus.RUNNING,
            start_ts=start,
            config_snapshot=self.settings.snapshot_json(),
            framework_version=dvf.__version__,
        )
        self.writer.merge("audit", "run_audit", run_audit)

        with correlation_scope(run_id):
            try:
                configs = self.repo.load_validation_configs(
                    groups=request.execution_groups or None,
                    validation_ids=request.validation_ids or None)
                _log.info("run %s: %d validation(s) selected", run_id, len(configs))

                ctx = RunContext(
                    run_id=run_id, settings=self.settings, backend=self.backend,
                    repo=self.repo, writer=self.writer, factory=self.factory,
                    sqlgen=self.sqlgen, spark=self.spark,
                    profile_override=request.profile_override,
                    skip_validation_ids=self._resume_skips(request))

                results = self._execute(ctx, configs)
            finally:
                pass  # run_audit finalized below even on unexpected failure

            end = utcnow()
            summary = self._summarize(run_id, results, start, end)
            run_audit.status = summary.status
            run_audit.end_ts = end
            run_audit.duration_sec = summary.duration_sec
            run_audit.total_validations = summary.total
            run_audit.passed = summary.passed
            run_audit.failed = summary.failed
            run_audit.warnings = summary.warnings
            run_audit.errors = summary.errors
            run_audit.skipped = summary.skipped
            self.writer.merge("audit", "run_audit", run_audit)
            self.writer.stop()
            _log.info("run %s finished: %s (%d passed / %d failed / %d errors)",
                      run_id, summary.status, summary.passed, summary.failed,
                      summary.errors)
        return summary

    # ------------------------------------------------------------------ #
    def _execute(self, ctx: RunContext,
                 configs: list[ValidationConfig]) -> list[ValidationRunAudit]:
        """Priority levels run sequentially; items within a level run on the
        thread pool, with parallel_flag=False items acting as barriers."""
        results: list[ValidationRunAudit] = []
        levels = ExecutionPlanner.plan(configs)
        max_workers = max(1, self.settings.engine.max_parallel)
        for level in levels:
            _log.info("executing priority level %d (%d validation(s))",
                      level.priority, len(level.items))
            results.extend(run_level(level.items,
                                     lambda c: self._run_one(ctx, c),
                                     max_workers))
        return results

    def _run_one(self, ctx: RunContext, config: ValidationConfig) -> ValidationRunAudit:
        if config.validation_id in ctx.skip_validation_ids:
            audit = self._skipped_audit(ctx, config, "resume: already PASSED in prior run")
            self.writer.merge("audit", "validation_run_audit", audit)
            return audit
        try:
            rules = self.expander.expand(config, ctx.profile_override)
        except MetadataError as exc:
            audit = self._skipped_audit(ctx, config, str(exc),
                                        status=ValidationStatus.ERROR)
            self.writer.merge("audit", "validation_run_audit", audit)
            _log.error("validation %s not runnable: %s", config.validation_id, exc)
            return audit
        return ValidationTask(ctx, config, rules).execute()

    def _skipped_audit(self, ctx: RunContext, config: ValidationConfig,
                       reason: str, status: str = ValidationStatus.SKIPPED) -> ValidationRunAudit:
        now = utcnow()
        return ValidationRunAudit(
            execution_id=new_id("E"), run_id=ctx.run_id,
            validation_id=config.validation_id, status=status,
            start_ts=now, end_ts=now, duration_sec=0.0,
            error_message=reason,
            correlation_id=f"{ctx.run_id}:{config.validation_id}")

    # ------------------------------------------------------------------ #
    def _resume_skips(self, request: RunRequest) -> set[str]:
        if not request.resume_run_id:
            return set()
        rows = self.backend.read_rows(
            "audit", "validation_run_audit",
            where_eq={"run_id": request.resume_run_id,
                      "status": ValidationStatus.PASSED})
        skips = {r["validation_id"] for r in rows}
        _log.info("resume from %s: skipping %d already-passed validation(s)",
                  request.resume_run_id, len(skips))
        return skips

    @staticmethod
    def _summarize(run_id: str, results: list[ValidationRunAudit],
                   start, end) -> RunSummary:
        s = RunSummary(run_id=run_id, status=RunStatus.SUCCEEDED,
                       duration_sec=duration_sec(start, end), validations=results)
        s.total = len(results)
        for r in results:
            if r.status == ValidationStatus.PASSED:
                s.passed += 1
            elif r.status == ValidationStatus.WARNING:
                s.warnings += 1
            elif r.status == ValidationStatus.SKIPPED:
                s.skipped += 1
            elif r.status in (ValidationStatus.ERROR, ValidationStatus.TIMEOUT):
                s.errors += 1
            else:
                s.failed += 1
        if s.failed or s.errors:
            s.status = RunStatus.PARTIAL if (s.passed or s.warnings) else RunStatus.FAILED
        return s
