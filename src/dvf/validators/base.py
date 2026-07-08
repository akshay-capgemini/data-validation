"""BaseValidator — contract + shared helpers for every validation rule type.

A validator receives a ValidationContext (config row + expanded rule instance +
live source/target connectors + SQL generator) and returns RuleResult rows.
Validators NEVER write to storage directly except mismatch/profile emission,
which goes through the context's writer so ordering is preserved.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, ClassVar, Optional

from dvf.audit.models import MismatchRecord, ProfileRecord, RuleResult, RuleStatus
from dvf.config.settings import FrameworkSettings
from dvf.connectors.base import BaseConnector
from dvf.metadata.models import (
    ColumnDef, RuleInstance, Severity, ToleranceTypes, ValidationConfig,
)
from dvf.sqlgen.generator import ObjectRef, SQLGenerator
from dvf.utils.timeutil import new_id, utcnow

if False:  # TYPE_CHECKING without cycle
    from dvf.audit.writer import AuditWriter


@dataclass
class ValidationContext:
    config: ValidationConfig
    rule: RuleInstance
    source: BaseConnector
    target: BaseConnector
    sqlgen: SQLGenerator
    settings: FrameworkSettings
    run_id: str
    execution_id: str
    shared: dict = field(default_factory=dict)   # per-validation cache across rules
    writer: "AuditWriter | None" = None
    spark: Any = None

    # ---- shared-cache keys used across rule types ---------------------- #
    KEY_SOURCE_COUNT = "source_row_count"
    KEY_TARGET_COUNT = "target_row_count"
    KEY_SOURCE_SCHEMA = "source_schema_cols"
    KEY_TARGET_SCHEMA = "target_schema_cols"
    KEY_COMPARISON = "comparison_outcome"

    def emit_mismatch(self, record: MismatchRecord) -> None:
        if self.writer is not None:
            self.writer.append("results", "mismatch_detail", record)

    def emit_profile(self, record: ProfileRecord) -> None:
        if self.writer is not None:
            self.writer.append("results", "profile_detail", record)


class BaseValidator(ABC):
    rule_type: ClassVar[str] = ""

    def __init__(self, ctx: ValidationContext):
        self.ctx = ctx
        self.config = ctx.config
        self.rule = ctx.rule
        self.source = ctx.source
        self.target = ctx.target
        self.sqlgen = ctx.sqlgen
        self.settings = ctx.settings

    @abstractmethod
    def execute(self) -> list[RuleResult]: ...

    # ------------------------------------------------------------------ #
    # Result construction
    # ------------------------------------------------------------------ #
    def result(self, status: str, *, column_name: str | None = None,
               source_value: Any = None, target_value: Any = None,
               difference: Any = None, difference_pct: float | None = None,
               tolerance_applied: str | None = None, details: dict | None = None,
               source_sql: str | None = None, target_sql: str | None = None) -> RuleResult:
        return RuleResult(
            result_id=new_id("RES"),
            execution_id=self.ctx.execution_id,
            run_id=self.ctx.run_id,
            validation_id=self.config.validation_id,
            rule_type=self.rule.rule_type,
            rule_id=self.rule.rule_id,
            column_name=column_name if column_name is not None else self.rule.column_name,
            status=status,
            source_value=_s(source_value),
            target_value=_s(target_value),
            difference=_s(difference),
            difference_pct=difference_pct,
            tolerance_applied=tolerance_applied,
            details=details or {},
            source_sql=source_sql,
            target_sql=target_sql,
        )

    def fail_status(self) -> str:
        """FAIL for ERROR-severity rules, WARN for WARNING/INFO severity."""
        return RuleStatus.FAIL if self.rule.severity == Severity.ERROR else RuleStatus.WARN

    def param(self, key: str, default: Any = None) -> Any:
        return self.rule.parameters.get(key, default)

    # ------------------------------------------------------------------ #
    # Object references
    # ------------------------------------------------------------------ #
    def source_ref(self, extra_predicates: tuple[str, ...] = ()) -> ObjectRef:
        extras = tuple(self.ctx.shared.get("source_extra_predicates", ())) + extra_predicates
        schema, obj = self.source.resolve_object(self.config.source_schema,
                                                 self.config.source_object)
        return self.sqlgen.object_ref(
            self.source.dialect, schema=schema, obj=obj,
            query=self.config.source_query, filter_sql=self.config.source_filter,
            extra_predicates=extras,
            suffix=self.ctx.shared.get("source_sample_suffix"))

    def target_ref(self, extra_predicates: tuple[str, ...] = ()) -> ObjectRef:
        extras = tuple(self.ctx.shared.get("target_extra_predicates", ())) + extra_predicates
        schema, obj = self.target.resolve_object(self.config.target_schema,
                                                 self.config.target_object)
        return self.sqlgen.object_ref(
            self.target.dialect, schema=schema, obj=obj,
            query=self.config.target_query, filter_sql=self.config.target_filter,
            extra_predicates=extras,
            suffix=self.ctx.shared.get("target_sample_suffix"))

    # ------------------------------------------------------------------ #
    # Schema access (cached per validation)
    # ------------------------------------------------------------------ #
    def source_schema_cols(self) -> list[ColumnDef]:
        cached = self.ctx.shared.get(ValidationContext.KEY_SOURCE_SCHEMA)
        if cached is None:
            cached = self.source.get_schema(self.config.source_schema,
                                            self.config.source_object) \
                if self.config.source_object else []
            self.ctx.shared[ValidationContext.KEY_SOURCE_SCHEMA] = cached
        return cached

    def target_schema_cols(self) -> list[ColumnDef]:
        cached = self.ctx.shared.get(ValidationContext.KEY_TARGET_SCHEMA)
        if cached is None:
            cached = self.target.get_schema(self.config.target_schema,
                                            self.config.target_object) \
                if self.config.target_object else []
            self.ctx.shared[ValidationContext.KEY_TARGET_SCHEMA] = cached
        return cached

    def shared_columns(self) -> list[tuple[ColumnDef, ColumnDef]]:
        """(source_col, target_col) pairs matched by mapping/name, excludes applied."""
        excluded = {c.lower() for c in self.config.excluded_columns}
        target_by_name = {c.name.lower(): c for c in self.target_schema_cols()}
        pairs: list[tuple[ColumnDef, ColumnDef]] = []
        for s_col in self.source_schema_cols():
            if s_col.name.lower() in excluded:
                continue
            t_name = self.config.map_column(s_col.name)
            t_col = target_by_name.get(t_name.lower())
            if t_col is not None and t_col.name.lower() not in excluded:
                pairs.append((s_col, t_col))
        return pairs

    # ------------------------------------------------------------------ #
    # Tolerance
    # ------------------------------------------------------------------ #
    def compare_numeric(self, source: float | int | None,
                        target: float | int | None) -> tuple[bool, str | None, float | None]:
        """(within_tolerance, tolerance_description, difference_pct)."""
        if source is None or target is None:
            return (source is None and target is None), None, None
        diff = abs(float(target) - float(source))
        diff_pct = (float(target) - float(source)) * 100.0 / float(source) \
            if float(source) != 0 else None
        t_type = (self.config.tolerance_type or "").upper()
        t_val = self.config.tolerance_value
        if t_val is None or not t_type:
            return diff == 0, None, diff_pct
        if t_type == ToleranceTypes.ABSOLUTE:
            return diff <= t_val, f"ABSOLUTE {t_val}", diff_pct
        if t_type == ToleranceTypes.PERCENT:
            allowed = abs(float(source)) * t_val / 100.0
            return diff <= allowed, f"PERCENT {t_val}", diff_pct
        return diff == 0, None, diff_pct


def _s(value: Any) -> str | None:
    if value is None:
        return None
    return value if isinstance(value, str) else str(value)
