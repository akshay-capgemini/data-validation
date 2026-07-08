"""Reconciliation validators — record-level comparison rule types.

The tiered comparison executes ONCE per validation (ComparisonEngine caches its
outcome in ctx.shared); ROW_HASH_COMPARE / MISSING_RECORDS / EXTRA_RECORDS /
COLUMN_COMPARE each read that shared outcome, so a FULL profile costs one
comparison, not four.
"""

from __future__ import annotations

from dvf.audit.models import RuleResult, RuleStatus
from dvf.comparison.engine import ComparisonEngine
from dvf.metadata.ddl import RuleTypes
from dvf.validators.base import BaseValidator
from dvf.validators.registry import ValidatorRegistry


class _ReconciliationBase(BaseValidator):
    def _outcome(self) -> dict:
        return ComparisonEngine(self).outcome()

    def _threshold(self) -> float:
        return float(self.rule.threshold or 0)


@ValidatorRegistry.register(RuleTypes.ROW_HASH_COMPARE)
class RowHashCompareValidator(_ReconciliationBase):
    """Overall record-level equality: mismatched + missing + extra == 0."""

    def execute(self) -> list[RuleResult]:
        o = self._outcome()
        diffs = o["rows_mismatched"] + o["rows_missing"] + o["rows_extra"]
        ok = diffs <= self._threshold()
        details = {k: o[k] for k in
                   ("tier", "rows_compared", "rows_matched", "rows_mismatched",
                    "rows_missing", "rows_extra", "duplicate_source_keys",
                    "duplicate_target_keys", "counts_capped", "samples_captured",
                    "key_columns")}
        return [self.result(
            RuleStatus.PASS if ok else self.fail_status(),
            source_value=o["rows_compared"],
            target_value=o["rows_matched"],
            difference=None if ok else
            (f"{o['rows_mismatched']} mismatched, {o['rows_missing']} missing, "
             f"{o['rows_extra']} extra"
             + (" (counts capped)" if o.get("counts_capped") else "")),
            details=details,
            source_sql=o.get("source_sql"), target_sql=o.get("target_sql"))]


@ValidatorRegistry.register(RuleTypes.MISSING_RECORDS)
class MissingRecordsValidator(_ReconciliationBase):
    """Rows present in source but absent from target."""

    def execute(self) -> list[RuleResult]:
        o = self._outcome()
        ok = o["rows_missing"] <= self._threshold()
        return [self.result(
            RuleStatus.PASS if ok else self.fail_status(),
            source_value=o["rows_compared"], target_value=o["rows_missing"],
            difference=None if ok else
            f"{o['rows_missing']} record(s) missing in target",
            details={"tier": o["tier"], "samples_in_mismatch_detail":
                     min(o["rows_missing"], o["samples_captured"])})]


@ValidatorRegistry.register(RuleTypes.EXTRA_RECORDS)
class ExtraRecordsValidator(_ReconciliationBase):
    """Rows present in target that the source never had."""

    def execute(self) -> list[RuleResult]:
        o = self._outcome()
        ok = o["rows_extra"] <= self._threshold()
        return [self.result(
            RuleStatus.PASS if ok else self.fail_status(),
            source_value=o["rows_compared"], target_value=o["rows_extra"],
            difference=None if ok else
            f"{o['rows_extra']} unexpected record(s) in target",
            details={"tier": o["tier"]})]


@ValidatorRegistry.register(RuleTypes.CHECKSUM)
class ChecksumValidator(_ReconciliationBase):
    """Whole-table equality proof: COUNT + two independent signature sums over
    row hashes, computed entirely pushdown — three numbers per side move.
    Needs hash + slice support on both dialects (else use ROW_HASH_COMPARE)."""

    def execute(self) -> list[RuleResult]:
        from dvf.comparison.engine import ComparisonEngine
        from dvf.comparison.plan import build_plan
        engine = ComparisonEngine(self)
        for side, connector in (("source", self.source), ("target", self.target)):
            if not (engine.supports_hash(connector)
                    and connector.dialect.supports_hash_slices):
                return [self.result(RuleStatus.SKIP, details={
                    "reason": f"{side} dialect '{connector.dialect.name}' lacks "
                              "hash/slice support — use ROW_HASH_COMPARE"})]
        plan = build_plan(self)
        timeout = self.settings.engine.query_timeout_s
        values, sqls = {}, {}
        for side, connector in (("source", self.source), ("target", self.target)):
            inner = engine.render_select(side, plan, include_values=False,
                                         hash_mode="hash")
            sql = self.sqlgen.render(connector.dialect, "checksum.sql.j2",
                                     inner_sql=inner)
            row = connector.execute_query(sql, timeout_s=timeout)[0]
            values[side] = (int(row[0] or 0), int(row[1] or 0), int(row[2] or 0))
            sqls[side] = sql
        ok = values["source"] == values["target"]
        return [self.result(
            RuleStatus.PASS if ok else self.fail_status(),
            source_value=f"cnt={values['source'][0]}, sig={values['source'][1:]}",
            target_value=f"cnt={values['target'][0]}, sig={values['target'][1:]}",
            difference=None if ok else "table checksums differ",
            details={"source": list(values["source"]),
                     "target": list(values["target"])},
            source_sql=sqls["source"], target_sql=sqls["target"])]


@ValidatorRegistry.register(RuleTypes.PARTITION_COUNT)
class PartitionCountValidator(_ReconciliationBase):
    """Per-partition record counts compared across sides (GROUP BY partition
    column pushed down on each engine; only one row per partition moves)."""

    _DETAIL_CAP = 50

    def execute(self) -> list[RuleResult]:
        column = self.rule.column_name or self.config.partition_column
        if not column:
            return [self.result(RuleStatus.SKIP,
                                details={"reason": "no partition_column configured"})]
        timeout = self.settings.engine.query_timeout_s
        counts, sqls = {}, {}
        for side, connector, ref in (("source", self.source, self.source_ref()),
                                     ("target", self.target, self.target_ref())):
            d = connector.dialect
            col = column if side == "source" else self.config.map_column(column)
            part_expr = f"COALESCE({d.cast_to_string(d.quote(col))}, '<NULL>')"
            sql = self.sqlgen.render(d, "partition_count.sql.j2", ref=ref,
                                     part_expr=part_expr)
            rows = connector.execute_query(sql, timeout_s=timeout)
            counts[side] = {str(r[0]): int(r[1] or 0) for r in rows}
            sqls[side] = sql

        src, tgt = counts["source"], counts["target"]
        issues = []
        for key in sorted(set(src) | set(tgt)):
            s, t = src.get(key), tgt.get(key)
            if s is None:
                issues.append({"partition": key, "issue": "only in target",
                               "target": t})
            elif t is None:
                issues.append({"partition": key, "issue": "only in source",
                               "source": s})
            else:
                ok, _, _ = self.compare_numeric(s, t)
                if not ok:
                    issues.append({"partition": key, "source": s, "target": t})
        status = self.fail_status() if issues else RuleStatus.PASS
        results = [self.result(
            status, column_name=column,
            source_value=f"{len(src)} partition(s)",
            target_value=f"{len(tgt)} partition(s)",
            difference=f"{len(issues)} partition(s) differ" if issues else None,
            details={"issues": issues[:self._DETAIL_CAP]},
            source_sql=sqls["source"], target_sql=sqls["target"])]
        for issue in issues[:self._DETAIL_CAP]:
            results.append(self.result(
                self.fail_status(), column_name=column,
                source_value=issue.get("source"), target_value=issue.get("target"),
                difference=f"partition {issue['partition']}: "
                           f"{issue.get('issue', 'count mismatch')}"))
        return results


@ValidatorRegistry.register(RuleTypes.COLUMN_COMPARE)
class ColumnCompareValidator(_ReconciliationBase):
    """Which columns drive the mismatches (exact in SMALL tier, sample-derived
    in MEDIUM+ — see details.derivation)."""

    def execute(self) -> list[RuleResult]:
        o = self._outcome()
        counts: dict[str, int] = o.get("column_mismatch_counts") or {}
        derivation = "exact" if o["tier"] == "SMALL" else "sampled"
        summary_ok = o["rows_mismatched"] <= self._threshold()
        results = [self.result(
            RuleStatus.PASS if summary_ok else self.fail_status(),
            source_value=f"{len(o.get('value_columns') or [])} columns compared",
            target_value=f"{len(counts)} column(s) with differences",
            difference=None if summary_ok else
            f"{o['rows_mismatched']} row(s) differ",
            details={"tier": o["tier"], "derivation": derivation,
                     "column_mismatch_counts": counts})]
        for name, count in sorted(counts.items(), key=lambda kv: -kv[1]):
            results.append(self.result(
                self.fail_status(), column_name=name,
                difference=f"{count} differing value(s) ({derivation})",
                details={"derivation": derivation}))
        return results
