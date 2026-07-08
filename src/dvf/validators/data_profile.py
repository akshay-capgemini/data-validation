"""DATA_PROFILE validator — profiles both sides, persists metrics to
results.profile_detail, and (optionally) compares headline stats."""

from __future__ import annotations

import json

from dvf.audit.models import ProfileRecord, RuleResult, RuleStatus
from dvf.metadata.ddl import RuleTypes
from dvf.profiling.profiler import DataProfiler, ProfileMetrics
from dvf.utils.timeutil import new_id
from dvf.validators.base import BaseValidator
from dvf.validators.registry import ValidatorRegistry


@ValidatorRegistry.register(RuleTypes.DATA_PROFILE)
class DataProfileValidator(BaseValidator):
    """Parameters: top_n (5), top_value_columns (5), distinct ('true'),
    compare ('true' -> FAIL when null/distinct counts diverge between sides)."""

    def execute(self) -> list[RuleResult]:
        pairs = self.shared_columns()
        if self.rule.column_name:
            pairs = [(s, t) for s, t in pairs
                     if s.name.lower() == self.rule.column_name.lower()]
        if not pairs:
            return [self.result(RuleStatus.SKIP,
                                details={"reason": "no shared columns"})]
        top_n = int(self.param("top_n", 5))
        top_cols = int(self.param("top_value_columns", 5))
        distinct = str(self.param("distinct", "true")).lower() == "true"
        compare = str(self.param("compare", "true")).lower() == "true"

        profiles = {}
        for side, connector, ref, cols in (
                ("SOURCE", self.source, self.source_ref(), [s for s, _ in pairs]),
                ("TARGET", self.target, self.target_ref(), [t for _, t in pairs])):
            profiler = DataProfiler(connector, self.sqlgen, self.settings)
            profiles[side] = profiler.profile(ref, cols, top_n=top_n,
                                              top_value_columns=top_cols,
                                              distinct=distinct)
            self._persist(side, profiles[side])

        drift: list[str] = []
        if compare:
            src_by = {c.column.lower(): c for c in profiles["SOURCE"].columns}
            for i, (s_col, t_col) in enumerate(pairs):
                s = src_by.get(s_col.name.lower())
                t = profiles["TARGET"].columns[i]
                if s is None:
                    continue
                for metric in (ProfileMetrics.NULL_COUNT,
                               ProfileMetrics.DISTINCT_COUNT):
                    sv, tv = s.metrics.get(metric), t.metrics.get(metric)
                    if sv is not None and tv is not None and sv != tv:
                        drift.append(f"{s_col.name}.{metric}: {sv} vs {tv}")

        status = RuleStatus.PASS if not (compare and drift) else self.fail_status()
        return [self.result(
            status,
            source_value=f"{profiles['SOURCE'].row_count} rows profiled",
            target_value=f"{profiles['TARGET'].row_count} rows profiled",
            difference="; ".join(drift[:20]) if drift else None,
            details={"columns_profiled": len(pairs), "drift": drift[:50],
                     "metrics_in_profile_detail": True},
            source_sql=profiles["SOURCE"].executed_sql[0],
            target_sql=profiles["TARGET"].executed_sql[0])]

    # ------------------------------------------------------------------ #
    def _persist(self, side: str, profile) -> None:
        self.ctx.emit_profile(ProfileRecord(
            profile_id=new_id("PF"), run_id=self.ctx.run_id,
            execution_id=self.ctx.execution_id,
            validation_id=self.config.validation_id, side=side,
            column_name=None, metric=ProfileMetrics.ROW_COUNT,
            metric_value=str(profile.row_count)))
        for col in profile.columns:
            for metric, value in col.metrics.items():
                self.ctx.emit_profile(ProfileRecord(
                    profile_id=new_id("PF"), run_id=self.ctx.run_id,
                    execution_id=self.ctx.execution_id,
                    validation_id=self.config.validation_id, side=side,
                    column_name=col.column, metric=metric,
                    metric_value=json.dumps(value, default=str)
                    if isinstance(value, (list, dict)) else
                    (str(value) if value is not None else None)))
