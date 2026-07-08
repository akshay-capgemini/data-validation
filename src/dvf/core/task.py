"""ValidationTask — runs one validation's expanded rules against live connectors.

Checkpoint grain = one validation attempt: a RUNNING row is merged before rules
start and finalized (same execution_id) when they finish, so resume/rerun can
reason about exactly what completed.
"""

from __future__ import annotations

import time
import traceback

from dvf.audit.logger import correlation_scope, get_logger
from dvf.audit.models import (
    RuleResult, RuleStatus, ValidationRunAudit, ValidationStatus,
)
from dvf.core.context import RunContext
from dvf.core.exceptions import DVFError, QueryTimeoutError, is_transient
from dvf.metadata.models import RuleInstance, ValidationConfig
from dvf.utils.timeutil import duration_sec, new_id, utcnow
from dvf.validators.base import ValidationContext
from dvf.validators.registry import ValidatorRegistry

_log = get_logger("core.task")

_MSG_CAP = 4000
_STACK_CAP = 8000


class ValidationTask:
    def __init__(self, run_ctx: RunContext, config: ValidationConfig,
                 rules: list[RuleInstance], attempt: int = 1):
        self.run_ctx = run_ctx
        self.config = config
        self.rules = rules
        self.attempt = attempt

    # ------------------------------------------------------------------ #
    def execute(self) -> ValidationRunAudit:
        """Run with transient-only retry: each attempt is its own audit row
        (new execution_id); data mismatches (FAILED) never retry."""
        retry_max = self.run_ctx.settings.engine.retry_max
        backoff = self.run_ctx.settings.engine.retry_backoff_s
        while True:
            audit, transient_exc = self._execute_once()
            retryable = (transient_exc is not None
                         and audit.status in (ValidationStatus.ERROR,
                                              ValidationStatus.TIMEOUT))
            if retryable and self.attempt <= retry_max:
                delay = backoff * (2 ** (self.attempt - 1))
                _log.warning("validation %s attempt %d hit transient error (%s) — "
                             "retrying in %.0fs", self.config.validation_id,
                             self.attempt, transient_exc, delay)
                time.sleep(delay)
                self.attempt += 1
                continue
            return audit

    def _execute_once(self) -> tuple[ValidationRunAudit, BaseException | None]:
        execution_id = new_id("E")
        start = utcnow()
        transient_exc: BaseException | None = None
        audit = ValidationRunAudit(
            execution_id=execution_id,
            run_id=self.run_ctx.run_id,
            validation_id=self.config.validation_id,
            attempt=self.attempt,
            status=ValidationStatus.RUNNING,
            start_ts=start,
            rules_total=0,
            correlation_id=f"{self.run_ctx.run_id}:{self.config.validation_id}:{execution_id}",
        )
        self.run_ctx.writer.merge("audit", "validation_run_audit", audit)

        with correlation_scope(self.run_ctx.run_id, self.config.validation_id, execution_id):
            source = target = None
            shared: dict = {}
            results: list[RuleResult] = []
            timed_out = False
            try:
                _log.info("validation started (profile expansion: %d rules)", len(self.rules))
                source = self.run_ctx.factory.create(
                    self.run_ctx.repo.get_connection(self.config.source_connection))
                target = self.run_ctx.factory.create(
                    self.run_ctx.repo.get_connection(self.config.target_connection))
                source.connect()
                target.connect()
                self._apply_modes(source, target, shared)

                for rule in self.rules:
                    rule_results, rule_exc = self._run_rule(
                        rule, source, target, shared, execution_id)
                    if rule_exc is not None:
                        timed_out = timed_out or isinstance(rule_exc, QueryTimeoutError)
                        if transient_exc is None and is_transient(rule_exc):
                            transient_exc = rule_exc
                    for r in rule_results:
                        self.run_ctx.writer.append("results", "result_detail", r)
                    results.extend(rule_results)

                audit.status = self._aggregate_status(results, timed_out)
                audit.error_message = self._first_error(results)
                if shared.pop("_watermark_pending", False) and \
                        audit.status in (ValidationStatus.PASSED,
                                         ValidationStatus.WARNING):
                    self._advance_watermark(source)
            except Exception as exc:  # noqa: BLE001 — every failure must reach the audit trail
                audit.status = (ValidationStatus.TIMEOUT
                                if isinstance(exc, QueryTimeoutError)
                                else ValidationStatus.ERROR)
                audit.error_message = str(exc)[:_MSG_CAP]
                audit.error_stack = traceback.format_exc()[:_STACK_CAP]
                if is_transient(exc):
                    transient_exc = exc
                _log.error("validation errored: %s", exc, exc_info=True)
            finally:
                for connector in (source, target):
                    if connector is not None:
                        try:
                            connector.disconnect()
                        except Exception:  # noqa: BLE001 — disconnect is best-effort
                            pass

            end = utcnow()
            audit.end_ts = end
            audit.duration_sec = duration_sec(start, end)
            audit.rules_total = len(results)
            audit.rules_passed = sum(1 for r in results if r.status == RuleStatus.PASS)
            audit.rules_failed = sum(1 for r in results
                                     if r.status in (RuleStatus.FAIL, RuleStatus.ERROR))
            audit.source_row_count = shared.get(ValidationContext.KEY_SOURCE_COUNT)
            audit.target_row_count = shared.get(ValidationContext.KEY_TARGET_COUNT)
            comparison = shared.get(ValidationContext.KEY_COMPARISON) or {}
            audit.rows_compared = comparison.get("rows_compared")
            audit.rows_matched = comparison.get("rows_matched")
            audit.rows_mismatched = comparison.get("rows_mismatched")
            audit.rows_missing = comparison.get("rows_missing")
            audit.rows_extra = comparison.get("rows_extra")
            audit.comparison_tier_used = comparison.get("tier")
            audit.retry_count = self.attempt - 1
            self.run_ctx.writer.merge("audit", "validation_run_audit", audit)
            _log.info("validation finished status=%s rules=%d/%d",
                      audit.status, audit.rules_passed, audit.rules_total)
        return audit, transient_exc

    # ------------------------------------------------------------------ #
    # Validation modes: INCREMENTAL (watermark predicates) & SAMPLE
    # ------------------------------------------------------------------ #
    def _apply_modes(self, source, target, shared: dict) -> None:
        cfg = self.config
        mode = (cfg.validation_mode or "FULL").upper()

        if mode == "INCREMENTAL" and cfg.watermark_column:
            wm = self.run_ctx.repo.get_watermark(cfg.validation_id)
            value = None
            if wm is not None:
                value = (wm.last_value_num if wm.last_value_num is not None
                         else wm.last_value_ts or wm.last_value_str)
            if value is not None:
                for side, connector in (("source", source), ("target", target)):
                    d = connector.dialect
                    predicate = (f"{d.quote(cfg.watermark_column)} > "
                                 f"{d.literal(value)}")
                    shared[f"{side}_extra_predicates"] = \
                        tuple(shared.get(f"{side}_extra_predicates", ())) + (predicate,)
                _log.info("incremental mode: %s > %r", cfg.watermark_column, value)
            shared["_watermark_pending"] = True

        if mode == "SAMPLE" and cfg.sampling_type and cfg.sampling_value:
            if cfg.sampling_type.upper() == "RANDOM":
                for side, connector, query in (
                        ("source", source, cfg.source_query),
                        ("target", target, cfg.target_query)):
                    d = connector.dialect
                    suffix = None if query else d.tablesample_clause(cfg.sampling_value)
                    if suffix:
                        shared[f"{side}_sample_suffix"] = suffix
                    else:
                        predicate = d.sample_predicate(cfg.sampling_value)
                        if predicate:
                            shared[f"{side}_extra_predicates"] = \
                                tuple(shared.get(f"{side}_extra_predicates", ())) + \
                                (predicate,)
                        else:
                            _log.warning("%s dialect '%s' supports no sampling — "
                                         "running unsampled", side, d.name)

    def _advance_watermark(self, source) -> None:
        cfg = self.config
        d = source.dialect
        ref = self.run_ctx.sqlgen.object_ref(
            d, schema=cfg.source_schema, obj=cfg.source_object,
            query=cfg.source_query, filter_sql=cfg.source_filter)
        sql = self.run_ctx.sqlgen.render(
            d, "aggregate.sql.j2", ref=ref,
            aggs=[{"func": "MAX", "expr": d.quote(cfg.watermark_column),
                   "alias": "wm"}])
        row = source.execute_query(
            sql, timeout_s=self.run_ctx.settings.engine.query_timeout_s)[0]
        value = row[1]
        if value is None:
            return
        from dvf.metadata.models import Watermark
        num = ts = None
        try:
            num = float(value)
        except (TypeError, ValueError):
            from datetime import datetime
            try:
                ts = datetime.fromisoformat(str(value).replace(" ", "T"))
            except ValueError:
                pass
        self.run_ctx.repo.save_watermark(Watermark(
            validation_id=cfg.validation_id,
            watermark_column=cfg.watermark_column,
            last_value_str=str(value), last_value_num=num, last_value_ts=ts,
            last_run_id=self.run_ctx.run_id))
        _log.info("watermark advanced to %r", value)

    # ------------------------------------------------------------------ #
    def _run_rule(self, rule: RuleInstance, source, target, shared: dict,
                  execution_id: str) -> tuple[list[RuleResult], BaseException | None]:
        ctx = ValidationContext(
            config=self.config, rule=rule, source=source, target=target,
            sqlgen=self.run_ctx.sqlgen, settings=self.run_ctx.settings,
            run_id=self.run_ctx.run_id, execution_id=execution_id,
            shared=shared, writer=self.run_ctx.writer, spark=self.run_ctx.spark)
        rule_start = utcnow()
        caught: BaseException | None = None
        try:
            validator = ValidatorRegistry.create(rule.rule_type, ctx)
            rule_results = validator.execute()
        except Exception as exc:  # noqa: BLE001 — a rule failure must not kill the validation
            caught = exc
            _log.error("rule %s errored: %s", rule.rule_type, exc, exc_info=True)
            detail: dict = {"exception": type(exc).__name__}
            if not isinstance(exc, DVFError):
                detail["stack"] = traceback.format_exc()[:_STACK_CAP]
            rule_results = [RuleResult(
                result_id=new_id("RES"), execution_id=execution_id,
                run_id=self.run_ctx.run_id, validation_id=self.config.validation_id,
                rule_type=rule.rule_type, rule_id=rule.rule_id,
                column_name=rule.column_name, status=RuleStatus.ERROR,
                difference=str(exc)[:_MSG_CAP], details=detail)]
        rule_end = utcnow()
        for r in rule_results:
            if r.start_ts is None:
                r.start_ts = rule_start
            if r.end_ts is None:
                r.end_ts = rule_end
            if r.duration_sec is None:
                r.duration_sec = duration_sec(rule_start, rule_end)
        return rule_results, caught

    # ------------------------------------------------------------------ #
    @staticmethod
    def _aggregate_status(results: list[RuleResult], timed_out: bool) -> str:
        statuses = {r.status for r in results}
        if timed_out:
            return ValidationStatus.TIMEOUT
        if RuleStatus.ERROR in statuses:
            return ValidationStatus.ERROR
        if RuleStatus.FAIL in statuses:
            return ValidationStatus.FAILED
        if RuleStatus.WARN in statuses:
            return ValidationStatus.WARNING
        return ValidationStatus.PASSED

    @staticmethod
    def _first_error(results: list[RuleResult]) -> str | None:
        for r in results:
            if r.status in (RuleStatus.ERROR, RuleStatus.FAIL):
                text = r.difference or r.status
                return f"{r.rule_type}: {text}"[:_MSG_CAP]
        return None
