"""Structural validators — connectivity, existence, schema, column & record counts.

All checks push SQL down to the source/target systems; only scalars and schema
metadata travel back.
"""

from __future__ import annotations

from dvf.audit.models import RuleResult, RuleStatus
from dvf.core.exceptions import DVFError
from dvf.dialects.base import TypeCat
from dvf.metadata.ddl import RuleTypes
from dvf.validators.base import BaseValidator, ValidationContext
from dvf.validators.registry import ValidatorRegistry

_DETAIL_CAP = 50  # cap per-column lists embedded in details JSON


@ValidatorRegistry.register(RuleTypes.CONNECTION)
class ConnectionValidator(BaseValidator):
    """Both endpoints answer a trivial SELECT."""

    def execute(self) -> list[RuleResult]:
        details: dict = {}
        statuses: list[str] = []
        for side, connector in (("source", self.source), ("target", self.target)):
            try:
                ok = connector.ping()
                details[f"{side}_ping"] = "OK" if ok else "NO RESPONSE"
                statuses.append(RuleStatus.PASS if ok else RuleStatus.FAIL)
            except DVFError as exc:
                details[f"{side}_ping"] = f"ERROR: {exc}"
                statuses.append(RuleStatus.ERROR)
        status = (RuleStatus.ERROR if RuleStatus.ERROR in statuses
                  else self.fail_status() if RuleStatus.FAIL in statuses
                  else RuleStatus.PASS)
        return [self.result(status,
                            source_value=details.get("source_ping"),
                            target_value=details.get("target_ping"),
                            details=details)]


@ValidatorRegistry.register(RuleTypes.OBJECT_EXISTS)
class ObjectExistsValidator(BaseValidator):
    """Source/target objects exist (skipped per side when a query override is set)."""

    def execute(self) -> list[RuleResult]:
        details: dict = {}
        failed = False
        for side, connector, schema, obj, query in (
                ("source", self.source, self.config.source_schema,
                 self.config.source_object, self.config.source_query),
                ("target", self.target, self.config.target_schema,
                 self.config.target_object, self.config.target_query)):
            if query:
                details[side] = "SKIPPED (query override)"
                continue
            exists = connector.table_exists(schema, obj)
            details[side] = "EXISTS" if exists else "MISSING"
            failed = failed or not exists
        status = self.fail_status() if failed else RuleStatus.PASS
        return [self.result(status,
                            source_value=details.get("source"),
                            target_value=details.get("target"),
                            details=details)]


@ValidatorRegistry.register(RuleTypes.SCHEMA_COMPARE)
class SchemaCompareValidator(BaseValidator):
    """Column presence parity after mapping/excludes (types are DATA_TYPE's job)."""

    def execute(self) -> list[RuleResult]:
        if self.config.source_query or self.config.target_query:
            return [self.result(RuleStatus.SKIP,
                                details={"reason": "query override — no schema to introspect"})]
        source_cols = self.source_schema_cols()
        target_cols = self.target_schema_cols()
        excluded = {c.lower() for c in self.config.excluded_columns}

        src_names = [c.name for c in source_cols if c.name.lower() not in excluded]
        tgt_names = {c.name.lower() for c in target_cols if c.name.lower() not in excluded}
        mapped_targets = {self.config.map_column(n).lower() for n in src_names}

        missing_in_target = [n for n in src_names
                             if self.config.map_column(n).lower() not in tgt_names]
        extra_in_target = sorted(
            c.name for c in target_cols
            if c.name.lower() not in excluded and c.name.lower() not in mapped_targets)

        extra_is_failure = str(self.param("extra_is_failure", "false")).lower() == "true"
        if missing_in_target or (extra_in_target and extra_is_failure):
            status = self.fail_status()
        elif extra_in_target:
            status = RuleStatus.WARN
        else:
            status = RuleStatus.PASS
        return [self.result(
            status,
            source_value=f"{len(src_names)} columns",
            target_value=f"{len(tgt_names)} columns",
            difference=(f"missing_in_target={len(missing_in_target)}, "
                        f"extra_in_target={len(extra_in_target)}"),
            details={"missing_in_target": missing_in_target[:_DETAIL_CAP],
                     "extra_in_target": extra_in_target[:_DETAIL_CAP]})]


@ValidatorRegistry.register(RuleTypes.COLUMN_COUNT)
class ColumnCountValidator(BaseValidator):
    def execute(self) -> list[RuleResult]:
        if self.config.source_query or self.config.target_query:
            return [self.result(RuleStatus.SKIP,
                                details={"reason": "query override — no schema to introspect"})]
        excluded = {c.lower() for c in self.config.excluded_columns}
        src = len([c for c in self.source_schema_cols() if c.name.lower() not in excluded])
        tgt = len([c for c in self.target_schema_cols() if c.name.lower() not in excluded])
        status = RuleStatus.PASS if src == tgt else self.fail_status()
        return [self.result(status, source_value=src, target_value=tgt,
                            difference=tgt - src)]


# --------------------------------------------------------------------------- #
class _SchemaAttrValidator(BaseValidator):
    """Compare a schema attribute across shared columns; one result per issue."""

    def compare_pair(self, s_col, t_col) -> str | None:
        """Return an issue description, or None when the pair is fine."""
        raise NotImplementedError

    def execute(self) -> list[RuleResult]:
        if self.config.source_query or self.config.target_query:
            return [self.result(RuleStatus.SKIP,
                                details={"reason": "query override — no schema to introspect"})]
        pairs = self.shared_columns()
        if self.rule.column_name:
            pairs = [(s, t) for s, t in pairs
                     if s.name.lower() == self.rule.column_name.lower()]
        if not pairs:
            return [self.result(RuleStatus.SKIP, details={"reason": "no shared columns"})]
        issues: list[tuple] = []
        for s_col, t_col in pairs:
            issue = self.compare_pair(s_col, t_col)
            if issue:
                issues.append((s_col, t_col, issue))
        summary = self.result(
            self.fail_status() if issues else RuleStatus.PASS,
            source_value=f"{len(pairs)} columns checked",
            target_value=f"{len(issues)} issue(s)",
            details={"issues": [{"column": s.name, "issue": i}
                                for s, _, i in issues[:_DETAIL_CAP]]})
        per_col = [self.result(self.fail_status(), column_name=s.name,
                               source_value=self.describe(s),
                               target_value=self.describe(t), difference=issue)
                   for s, t, issue in issues[:_DETAIL_CAP]]
        return [summary] + per_col

    @staticmethod
    def describe(col) -> str:
        return col.native_type


@ValidatorRegistry.register(RuleTypes.DATA_TYPE)
class DataTypeValidator(_SchemaAttrValidator):
    """Type-category parity (STRING/INTEGER/DECIMAL/...); strict=true compares
    raw native types instead."""

    def compare_pair(self, s_col, t_col) -> str | None:
        if str(self.param("strict", "false")).lower() == "true":
            if s_col.native_type.upper() != t_col.native_type.upper():
                return f"native type {s_col.native_type} != {t_col.native_type}"
            return None
        s_cat = self.source.dialect.type_category(s_col.native_type)
        t_cat = self.target.dialect.type_category(t_col.native_type)
        if s_cat != t_cat:
            return f"category {s_cat.value} != {t_cat.value}"
        return None


@ValidatorRegistry.register(RuleTypes.LENGTH)
class LengthValidator(_SchemaAttrValidator):
    """String columns: target must not be narrower than source (exact=true for equality)."""

    def compare_pair(self, s_col, t_col) -> str | None:
        if TypeCat.STRING not in (self.source.dialect.type_category(s_col.native_type),):
            return None
        if s_col.length is None or t_col.length is None:
            return None
        exact = str(self.param("exact", "false")).lower() == "true"
        if exact and s_col.length != t_col.length:
            return f"length {s_col.length} != {t_col.length}"
        if not exact and t_col.length < s_col.length:
            return f"target length {t_col.length} < source {s_col.length} (truncation risk)"
        return None

    @staticmethod
    def describe(col) -> str:
        return f"{col.native_type} (len={col.length})"


@ValidatorRegistry.register(RuleTypes.PRECISION)
class PrecisionValidator(_SchemaAttrValidator):
    def compare_pair(self, s_col, t_col) -> str | None:
        if self.source.dialect.type_category(s_col.native_type) != TypeCat.DECIMAL:
            return None
        if s_col.precision is None or t_col.precision is None:
            return None
        exact = str(self.param("exact", "false")).lower() == "true"
        if exact and s_col.precision != t_col.precision:
            return f"precision {s_col.precision} != {t_col.precision}"
        if not exact and t_col.precision < s_col.precision:
            return f"target precision {t_col.precision} < source {s_col.precision}"
        return None

    @staticmethod
    def describe(col) -> str:
        return f"{col.native_type} (p={col.precision})"


@ValidatorRegistry.register(RuleTypes.SCALE)
class ScaleValidator(_SchemaAttrValidator):
    def compare_pair(self, s_col, t_col) -> str | None:
        if self.source.dialect.type_category(s_col.native_type) != TypeCat.DECIMAL:
            return None
        if s_col.scale is None or t_col.scale is None:
            return None
        exact = str(self.param("exact", "false")).lower() == "true"
        if exact and s_col.scale != t_col.scale:
            return f"scale {s_col.scale} != {t_col.scale}"
        if not exact and t_col.scale < s_col.scale:
            return f"target scale {t_col.scale} < source {s_col.scale} (rounding risk)"
        return None

    @staticmethod
    def describe(col) -> str:
        return f"{col.native_type} (s={col.scale})"


@ValidatorRegistry.register(RuleTypes.DEFAULT_VALUE_CHECK)
class DefaultValueValidator(_SchemaAttrValidator):
    """Column DEFAULT definitions match (normalized: parens/quotes/case stripped)."""

    @staticmethod
    def _normalize(value: str | None) -> str | None:
        if value is None:
            return None
        v = value.strip()
        while v.startswith("(") and v.endswith(")"):
            v = v[1:-1].strip()
        v = v.strip("'\"").strip()
        return v.upper() or None

    def compare_pair(self, s_col, t_col) -> str | None:
        s_def = self._normalize(s_col.default_value)
        t_def = self._normalize(t_col.default_value)
        if s_def != t_def:
            return f"default {s_def!r} != {t_def!r}"
        return None

    @staticmethod
    def describe(col) -> str:
        return f"default={col.default_value!r}"


@ValidatorRegistry.register(RuleTypes.CONSTRAINT_CHECK)
class ConstraintCheckValidator(BaseValidator):
    """Primary-key column sets match (after column mapping)."""

    def execute(self) -> list[RuleResult]:
        if self.config.source_query or self.config.target_query:
            return [self.result(RuleStatus.SKIP,
                                details={"reason": "query override"})]
        src_pk = [c.lower() for c in self.source.get_primary_keys(
            self.config.source_schema, self.config.source_object)]
        tgt_pk = [c.lower() for c in self.target.get_primary_keys(
            self.config.target_schema, self.config.target_object)]
        mapped_src = sorted(self.config.map_column(c).lower() for c in src_pk)
        ok = mapped_src == sorted(tgt_pk)
        return [self.result(RuleStatus.PASS if ok else self.fail_status(),
                            source_value=",".join(sorted(src_pk)) or "(none)",
                            target_value=",".join(sorted(tgt_pk)) or "(none)",
                            difference=None if ok else "primary key definition differs",
                            details={"source_pk": src_pk, "target_pk": tgt_pk})]


@ValidatorRegistry.register(RuleTypes.INDEX_CHECK)
class IndexCheckValidator(BaseValidator):
    """Every source index column-set is covered by some target index."""

    def execute(self) -> list[RuleResult]:
        if self.config.source_query or self.config.target_query:
            return [self.result(RuleStatus.SKIP,
                                details={"reason": "query override"})]
        src_idx = self.source.get_indexes(self.config.source_schema,
                                          self.config.source_object)
        tgt_idx = self.target.get_indexes(self.config.target_schema,
                                          self.config.target_object)
        tgt_sets = {tuple(sorted(c.lower() for c in i.columns)) for i in tgt_idx}
        missing = []
        for idx in src_idx:
            mapped = tuple(sorted(self.config.map_column(c).lower()
                                  for c in idx.columns))
            if mapped and mapped not in tgt_sets:
                missing.append({"index": idx.name, "columns": list(mapped)})
        ok = not missing
        return [self.result(RuleStatus.PASS if ok else self.fail_status(),
                            source_value=f"{len(src_idx)} index(es)",
                            target_value=f"{len(tgt_idx)} index(es)",
                            difference=None if ok else
                            f"{len(missing)} source index(es) unmatched in target",
                            details={"missing_in_target": missing[:_DETAIL_CAP]})]


@ValidatorRegistry.register(RuleTypes.RECORD_COUNT)
class RecordCountValidator(BaseValidator):
    """Pushdown COUNT(*) both sides; tolerance-aware; seeds shared counts for
    tier selection and audit rollup."""

    def execute(self) -> list[RuleResult]:
        timeout = self.settings.engine.query_timeout_s
        src_sql = self.sqlgen.render(self.source.dialect, "record_count.sql.j2",
                                     ref=self.source_ref())
        tgt_sql = self.sqlgen.render(self.target.dialect, "record_count.sql.j2",
                                     ref=self.target_ref())
        src_count = int(self.source.execute_scalar(src_sql, timeout_s=timeout) or 0)
        tgt_count = int(self.target.execute_scalar(tgt_sql, timeout_s=timeout) or 0)

        self.ctx.shared[ValidationContext.KEY_SOURCE_COUNT] = src_count
        self.ctx.shared[ValidationContext.KEY_TARGET_COUNT] = tgt_count

        ok, tolerance, diff_pct = self.compare_numeric(src_count, tgt_count)
        status = RuleStatus.PASS if ok else self.fail_status()
        return [self.result(status,
                            source_value=src_count, target_value=tgt_count,
                            difference=tgt_count - src_count, difference_pct=diff_pct,
                            tolerance_applied=tolerance,
                            source_sql=src_sql, target_sql=tgt_sql)]
