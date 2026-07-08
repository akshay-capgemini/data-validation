"""Data-quality validators — null/blank parity, duplicates, keys, domains,
patterns, date sanity, aggregate parity.

Two operating modes recur:
* parity mode (no column on the rule): compare a metric between source & target
  across shared columns — the migration-validation default
* threshold mode (rule has column_name / expression): assert a metric on ONE
  side (parameters['side'], default 'target') stays within rule.threshold
"""

from __future__ import annotations

from dvf.audit.models import RuleResult, RuleStatus
from dvf.dialects.base import TypeCat
from dvf.metadata.ddl import RuleTypes
from dvf.validators.base import BaseValidator, ValidationContext
from dvf.validators.registry import ValidatorRegistry

_DETAIL_CAP = 50


class _QualityBase(BaseValidator):
    """Shared plumbing for per-column count checks."""

    def _side(self) -> str:
        return str(self.param("side", "target")).lower()

    def _connector(self, side: str):
        return self.source if side == "source" else self.target

    def _ref(self, side: str):
        return self.source_ref() if side == "source" else self.target_ref()

    def _threshold(self) -> float:
        return float(self.rule.threshold or 0)

    def _timeout(self) -> int:
        return self.settings.engine.query_timeout_s

    def _columns_for(self, side: str) -> list:
        return self.source_schema_cols() if side == "source" else self.target_schema_cols()

    # ---- per-column single-pass counters -------------------------------- #
    def _column_counts(self, side: str, exprs: dict[str, str]) -> dict[str, int]:
        """One pushdown query: {alias: SUM(CASE WHEN <expr> ...)} per column."""
        connector = self._connector(side)
        columns = [{"expr": expr, "alias": f"c{i}"}
                   for i, expr in enumerate(exprs.values())]
        sql = self.sqlgen.render(connector.dialect, "flag_count.sql.j2",
                                 ref=self._ref(side), columns=columns)
        row = connector.execute_query(sql, timeout_s=self._timeout())[0]
        names = list(exprs.keys())
        return {names[i]: int(row[i + 1] or 0) for i in range(len(names))}, int(row[0] or 0), sql

    def _parity_results(self, metric: str, src: dict[str, int],
                        tgt_by_src_name: dict[str, int],
                        src_sql: str, tgt_sql: str) -> list[RuleResult]:
        """Compare per-column metric counts source vs target."""
        mismatched = []
        for name, s_count in src.items():
            t_count = tgt_by_src_name.get(name)
            if t_count is not None and t_count != s_count:
                mismatched.append((name, s_count, t_count))
        details = {
            "metric": metric,
            "columns_checked": len(src),
            "mismatched_columns": [
                {"column": n, "source": s, "target": t}
                for n, s, t in mismatched[:_DETAIL_CAP]],
            "source_counts": dict(list(src.items())[:_DETAIL_CAP]),
        }
        status = self.fail_status() if mismatched else RuleStatus.PASS
        summary = self.result(
            status,
            source_value=sum(src.values()),
            target_value=sum(tgt_by_src_name.get(n, 0) for n in src),
            difference=f"{len(mismatched)} column(s) differ" if mismatched else None,
            details=details, source_sql=src_sql, target_sql=tgt_sql)
        per_col = [self.result(self.fail_status(), column_name=n,
                               source_value=s, target_value=t,
                               difference=t - s,
                               details={"metric": metric})
                   for n, s, t in mismatched[:_DETAIL_CAP]]
        return [summary] + per_col


# --------------------------------------------------------------------------- #
@ValidatorRegistry.register(RuleTypes.NULL_CHECK)
class NullCheckValidator(_QualityBase):
    """Parity: per-column NULL counts match. Threshold: NULLs on one side/column."""

    def execute(self) -> list[RuleResult]:
        if self.rule.column_name:
            return self._threshold_mode()
        pairs = self.shared_columns()
        if not pairs:
            return [self.result(RuleStatus.SKIP, details={"reason": "no shared columns"})]
        d_src, d_tgt = self.source.dialect, self.target.dialect
        src_exprs = {s.name: f"{d_src.quote(s.name)} IS NULL" for s, _ in pairs}
        tgt_exprs = {s.name: f"{d_tgt.quote(t.name)} IS NULL" for s, t in pairs}
        src_counts, _, src_sql = self._column_counts("source", src_exprs)
        tgt_counts, _, tgt_sql = self._column_counts("target", tgt_exprs)
        return self._parity_results("null_count", src_counts, tgt_counts,
                                    src_sql, tgt_sql)

    def _threshold_mode(self) -> list[RuleResult]:
        side = self._side()
        connector = self._connector(side)
        col = connector.dialect.quote(self.rule.column_name)
        sql = self.sqlgen.render(connector.dialect, "predicate_count.sql.j2",
                                 ref=self._ref(side), predicate=f"{col} IS NULL")
        violations = int(connector.execute_scalar(sql, timeout_s=self._timeout()) or 0)
        ok = violations <= self._threshold()
        kwargs = {f"{side}_sql": sql, f"{side}_value": violations}
        return [self.result(RuleStatus.PASS if ok else self.fail_status(),
                            difference=f"{violations} NULL(s) on {side}"
                            if not ok else None,
                            details={"threshold": self._threshold(), "side": side},
                            **kwargs)]


@ValidatorRegistry.register(RuleTypes.BLANK_CHECK)
class BlankCheckValidator(_QualityBase):
    """Blank ('' / all-spaces, non-NULL) parity for string columns."""

    def execute(self) -> list[RuleResult]:
        pairs = [(s, t) for s, t in self.shared_columns()
                 if self.source.dialect.type_category(s.native_type) == TypeCat.STRING]
        if self.rule.column_name:
            pairs = [(s, t) for s, t in pairs
                     if s.name.lower() == self.rule.column_name.lower()]
        if not pairs:
            return [self.result(RuleStatus.SKIP,
                                details={"reason": "no shared string columns"})]
        d_src, d_tgt = self.source.dialect, self.target.dialect

        def blank(d, name):
            q = d.quote(name)
            return f"{q} IS NOT NULL AND TRIM({d.cast_to_string(q)}) = ''"

        src_exprs = {s.name: blank(d_src, s.name) for s, _ in pairs}
        tgt_exprs = {s.name: blank(d_tgt, t.name) for s, t in pairs}
        src_counts, _, src_sql = self._column_counts("source", src_exprs)
        tgt_counts, _, tgt_sql = self._column_counts("target", tgt_exprs)
        return self._parity_results("blank_count", src_counts, tgt_counts,
                                    src_sql, tgt_sql)


# --------------------------------------------------------------------------- #
class _KeyCheckBase(_QualityBase):
    """Duplicate/NULL checks over a key column set."""

    def _key_columns(self) -> list[str]:
        if self.rule.rule_expression:
            return [c.strip() for c in self.rule.rule_expression.split(",") if c.strip()]
        if self.rule.column_name:
            return [self.rule.column_name]
        return self.config.comparison_key_columns

    def _duplicates(self, side: str, keys: list[str]) -> tuple[int, int, str]:
        connector = self._connector(side)
        d = connector.dialect
        sql = self.sqlgen.render(d, "duplicate_count.sql.j2", ref=self._ref(side),
                                 keys=[d.quote(k) for k in keys])
        row = connector.execute_query(sql, timeout_s=self._timeout())[0]
        return int(row[0] or 0), int(row[1] or 0), sql   # duplicate_rows, duplicate_keys


@ValidatorRegistry.register(RuleTypes.DUPLICATE_CHECK)
class DuplicateCheckValidator(_KeyCheckBase):
    """No duplicate comparison-key rows on either side (threshold-aware)."""

    def execute(self) -> list[RuleResult]:
        keys = self._key_columns()
        if not keys:
            return [self.result(RuleStatus.SKIP,
                                details={"reason": "no comparison/primary key configured"})]
        src_dupes, src_keys, src_sql = self._duplicates("source", keys)
        tgt_dupes, tgt_keys, tgt_sql = self._duplicates("target", keys)
        threshold = self._threshold()
        ok = src_dupes <= threshold and tgt_dupes <= threshold
        return [self.result(
            RuleStatus.PASS if ok else self.fail_status(),
            source_value=src_dupes, target_value=tgt_dupes,
            difference=None if ok else
            f"duplicate keys: source={src_keys}, target={tgt_keys}",
            details={"keys": keys, "source_duplicate_keys": src_keys,
                     "target_duplicate_keys": tgt_keys, "threshold": threshold},
            source_sql=src_sql, target_sql=tgt_sql)]


@ValidatorRegistry.register(RuleTypes.PK_CHECK)
class PrimaryKeyCheckValidator(_KeyCheckBase):
    """Primary key integrity on both sides: unique AND not-null."""

    def execute(self) -> list[RuleResult]:
        keys = self.config.primary_key_columns
        if not keys:
            return [self.result(RuleStatus.SKIP,
                                details={"reason": "no primary_key configured"})]
        results = []
        for side in ("source", "target"):
            connector = self._connector(side)
            d = connector.dialect
            dupes, dupe_keys, dup_sql = self._duplicates(side, keys)
            null_pred = " OR ".join(f"{d.quote(k)} IS NULL" for k in keys)
            null_sql = self.sqlgen.render(d, "predicate_count.sql.j2",
                                          ref=self._ref(side), predicate=null_pred)
            nulls = int(connector.execute_scalar(null_sql,
                                                 timeout_s=self._timeout()) or 0)
            ok = dupes == 0 and nulls == 0
            kwargs = {f"{side}_sql": dup_sql, f"{side}_value":
                      f"dupes={dupes}, null_keys={nulls}"}
            results.append(self.result(
                RuleStatus.PASS if ok else self.fail_status(),
                column_name=",".join(keys),
                difference=None if ok else f"{side}: PK violated",
                details={"side": side, "duplicate_rows": dupes,
                         "duplicate_keys": dupe_keys, "null_key_rows": nulls},
                **kwargs))
        return results


@ValidatorRegistry.register(RuleTypes.UNIQUE_KEY_CHECK)
class UniqueKeyCheckValidator(_KeyCheckBase):
    """Configured column set is unique on the chosen side (default target)."""

    def execute(self) -> list[RuleResult]:
        keys = self._key_columns()
        if not keys:
            return [self.result(RuleStatus.SKIP,
                                details={"reason": "no key columns configured"})]
        side = self._side()
        dupes, dupe_keys, sql = self._duplicates(side, keys)
        ok = dupes <= self._threshold()
        kwargs = {f"{side}_sql": sql, f"{side}_value": dupes}
        return [self.result(RuleStatus.PASS if ok else self.fail_status(),
                            column_name=",".join(keys),
                            difference=None if ok else
                            f"{dupe_keys} duplicated key value(s) on {side}",
                            details={"side": side, "keys": keys,
                                     "duplicate_keys": dupe_keys},
                            **kwargs)]


@ValidatorRegistry.register(RuleTypes.FK_CHECK)
class ForeignKeyCheckValidator(_QualityBase):
    """Orphan check: FK values must exist in the referenced object (same connection).

    rule_expression = FK column csv; parameters: ref_schema, ref_object,
    ref_columns (csv, defaults to FK columns), side (default target)."""

    def execute(self) -> list[RuleResult]:
        fk_cols = [c.strip() for c in (self.rule.rule_expression or
                                       self.rule.column_name or "").split(",")
                   if c.strip()]
        ref_object = self.param("ref_object")
        if not fk_cols or not ref_object:
            return [self.result(RuleStatus.SKIP,
                                details={"reason": "needs FK columns + ref_object"})]
        side = self._side()
        connector = self._connector(side)
        d = connector.dialect
        ref_schema = self.param("ref_schema") or \
            (self.config.source_schema if side == "source" else self.config.target_schema)
        ref_cols = [c.strip() for c in
                    str(self.param("ref_columns", ",".join(fk_cols))).split(",")
                    if c.strip()]
        base = self._ref(side)
        ref_name = d.qualify(ref_schema, ref_object)
        on = " AND ".join(f"c.{d.quote(f)} = r.{d.quote(r)}"
                          for f, r in zip(fk_cols, ref_cols))
        not_null = " AND ".join(f"c.{d.quote(f)} IS NOT NULL" for f in fk_cols)
        where_extra = f" AND ({base.where})" if base.where else ""
        sql = (f"SELECT COUNT(*) AS orphan_count\n"
               f"FROM {base.from_clause} c\n"
               f"LEFT JOIN {ref_name} r ON {on}\n"
               f"WHERE r.{d.quote(ref_cols[0])} IS NULL AND {not_null}{where_extra}")
        orphans = int(connector.execute_scalar(sql, timeout_s=self._timeout()) or 0)
        ok = orphans <= self._threshold()
        kwargs = {f"{side}_sql": sql, f"{side}_value": orphans}
        return [self.result(RuleStatus.PASS if ok else self.fail_status(),
                            column_name=",".join(fk_cols),
                            difference=None if ok else
                            f"{orphans} orphan row(s) on {side}",
                            details={"side": side, "ref_object": ref_object,
                                     "ref_columns": ref_cols},
                            **kwargs)]


@ValidatorRegistry.register(RuleTypes.MANDATORY_CHECK)
class MandatoryCheckValidator(_QualityBase):
    """Mandatory columns contain no NULLs/blanks on the chosen side."""

    def execute(self) -> list[RuleResult]:
        cols = [c.strip() for c in (self.rule.rule_expression or
                                    self.rule.column_name or "").split(",") if c.strip()]
        if not cols:
            return [self.result(RuleStatus.SKIP,
                                details={"reason": "no mandatory columns configured"})]
        side = self._side()
        connector = self._connector(side)
        d = connector.dialect
        exprs = {c: (f"{d.quote(c)} IS NULL OR "
                     f"TRIM({d.cast_to_string(d.quote(c))}) = ''") for c in cols}
        counts, _, sql = self._column_counts(side, exprs)
        offenders = {c: n for c, n in counts.items() if n > self._threshold()}
        ok = not offenders
        kwargs = {f"{side}_sql": sql, f"{side}_value": sum(counts.values())}
        summary = self.result(RuleStatus.PASS if ok else self.fail_status(),
                              column_name=",".join(cols),
                              difference=None if ok else
                              f"{len(offenders)} mandatory column(s) violated on {side}",
                              details={"side": side, "violations": offenders},
                              **kwargs)
        per_col = [self.result(self.fail_status(), column_name=c,
                               difference=f"{n} NULL/blank value(s) on {side}",
                               details={"metric": "mandatory", "side": side})
                   for c, n in list(offenders.items())[:_DETAIL_CAP]]
        return [summary] + per_col


@ValidatorRegistry.register(RuleTypes.DOMAIN_CHECK)
class DomainCheckValidator(_QualityBase):
    """Column values restricted to a domain list (rule_expression = csv values)."""

    def execute(self) -> list[RuleResult]:
        column = self.rule.column_name
        domain = [v.strip() for v in (self.rule.rule_expression or "").split(",")
                  if v.strip() != ""]
        if not column or not domain:
            return [self.result(RuleStatus.SKIP,
                                details={"reason": "needs column_name + domain csv"})]
        side = self._side()
        connector = self._connector(side)
        d = connector.dialect
        q = d.quote(column)
        values = ", ".join(d.literal(v) for v in domain)
        predicate = f"{q} IS NOT NULL AND {d.cast_to_string(q)} NOT IN ({values})"
        sql = self.sqlgen.render(d, "predicate_count.sql.j2",
                                 ref=self._ref(side), predicate=predicate)
        violations = int(connector.execute_scalar(sql, timeout_s=self._timeout()) or 0)
        ok = violations <= self._threshold()
        kwargs = {f"{side}_sql": sql, f"{side}_value": violations}
        return [self.result(RuleStatus.PASS if ok else self.fail_status(),
                            difference=None if ok else
                            f"{violations} value(s) outside domain on {side}",
                            details={"side": side, "domain": domain[:_DETAIL_CAP]},
                            **kwargs)]


@ValidatorRegistry.register(RuleTypes.PATTERN_CHECK)
class PatternCheckValidator(_QualityBase):
    """Column values match a regex (rule_expression = pattern)."""

    def execute(self) -> list[RuleResult]:
        column, pattern = self.rule.column_name, self.rule.rule_expression
        if not column or not pattern:
            return [self.result(RuleStatus.SKIP,
                                details={"reason": "needs column_name + regex"})]
        side = self._side()
        connector = self._connector(side)
        d = connector.dialect
        q = d.quote(column)
        match = d.regexp_predicate(d.cast_to_string(q), pattern)
        predicate = f"{q} IS NOT NULL AND NOT ({match})"
        sql = self.sqlgen.render(d, "predicate_count.sql.j2",
                                 ref=self._ref(side), predicate=predicate)
        violations = int(connector.execute_scalar(sql, timeout_s=self._timeout()) or 0)
        ok = violations <= self._threshold()
        kwargs = {f"{side}_sql": sql, f"{side}_value": violations}
        return [self.result(RuleStatus.PASS if ok else self.fail_status(),
                            difference=None if ok else
                            f"{violations} value(s) not matching pattern on {side}",
                            details={"side": side, "pattern": pattern},
                            **kwargs)]


# --------------------------------------------------------------------------- #
class _MinMaxBase(_QualityBase):
    """MIN/MAX parity (or bounds) for date/timestamp columns."""

    category: TypeCat = TypeCat.DATE

    def execute(self) -> list[RuleResult]:
        pairs = [(s, t) for s, t in self.shared_columns()
                 if self.source.dialect.type_category(s.native_type) == self.category]
        if self.rule.column_name:
            pairs = [(s, t) for s, t in pairs
                     if s.name.lower() == self.rule.column_name.lower()]
        if not pairs:
            return [self.result(RuleStatus.SKIP,
                                details={"reason": f"no shared {self.category} columns"})]
        results = []
        min_bound, max_bound = self.param("min"), self.param("max")
        for s_col, t_col in pairs:
            src = self._min_max("source", s_col.name)
            tgt = self._min_max("target", t_col.name)
            issues = []
            if src != tgt:
                issues.append(f"min/max differ: source={src}, target={tgt}")
            for label, bound, idx, op in (("min", min_bound, 0, "<"),
                                          ("max", max_bound, 1, ">")):
                if bound is not None:
                    for side_name, mm in (("source", src), ("target", tgt)):
                        v = mm[idx]
                        if v is not None and ((op == "<" and str(v) < str(bound)) or
                                              (op == ">" and str(v) > str(bound))):
                            issues.append(f"{side_name} {label} {v} outside bound {bound}")
            results.append(self.result(
                self.fail_status() if issues else RuleStatus.PASS,
                column_name=s_col.name,
                source_value=f"{src[0]} .. {src[1]}",
                target_value=f"{tgt[0]} .. {tgt[1]}",
                difference="; ".join(issues) if issues else None,
                details={"bounds": {"min": min_bound, "max": max_bound}}))
        return results

    def _min_max(self, side: str, column: str) -> tuple:
        connector = self._connector(side)
        d = connector.dialect
        q = d.quote(column)
        sql = self.sqlgen.render(d, "aggregate.sql.j2", ref=self._ref(side),
                                 aggs=[{"func": "MIN", "expr": q, "alias": "mn"},
                                       {"func": "MAX", "expr": q, "alias": "mx"}])
        row = connector.execute_query(sql, timeout_s=self._timeout())[0]
        return (row[1], row[2])


@ValidatorRegistry.register(RuleTypes.DATE_CHECK)
class DateCheckValidator(_MinMaxBase):
    category = TypeCat.DATE


@ValidatorRegistry.register(RuleTypes.TIMESTAMP_CHECK)
class TimestampCheckValidator(_MinMaxBase):
    category = TypeCat.TIMESTAMP


# --------------------------------------------------------------------------- #
@ValidatorRegistry.register(RuleTypes.AGGREGATE)
class AggregateValidator(_QualityBase):
    """Numeric parity: SUM/MIN/MAX/AVG per shared numeric column, tolerance-aware.
    One pushdown aggregate query per side regardless of column count."""

    NUMERIC = {TypeCat.INTEGER, TypeCat.DECIMAL, TypeCat.FLOAT}

    def execute(self) -> list[RuleResult]:
        funcs = [f.strip().upper() for f in
                 str(self.param("functions", "SUM,MIN,MAX,AVG")).split(",") if f.strip()]
        pairs = [(s, t) for s, t in self.shared_columns()
                 if self.source.dialect.type_category(s.native_type) in self.NUMERIC]
        if self.rule.column_name:
            pairs = [(s, t) for s, t in pairs
                     if s.name.lower() == self.rule.column_name.lower()]
        if not pairs:
            return [self.result(RuleStatus.SKIP,
                                details={"reason": "no shared numeric columns"})]

        src_vals, src_sql = self._aggregates("source", [s for s, _ in pairs], funcs)
        tgt_vals, tgt_sql = self._aggregates("target", [t for _, t in pairs], funcs)

        results: list[RuleResult] = []
        mismatches = 0
        for i, (s_col, _t_col) in enumerate(pairs):
            for f in funcs:
                s_v, t_v = src_vals[(i, f)], tgt_vals[(i, f)]
                ok, tolerance, diff_pct = self.compare_numeric(s_v, t_v)
                if not ok:
                    mismatches += 1
                    results.append(self.result(
                        self.fail_status(), column_name=s_col.name,
                        source_value=s_v, target_value=t_v,
                        difference=f"{f} differs",
                        difference_pct=diff_pct, tolerance_applied=tolerance,
                        details={"function": f}))
        summary_status = self.fail_status() if mismatches else RuleStatus.PASS
        results.insert(0, self.result(
            summary_status,
            source_value=f"{len(pairs)} col x {len(funcs)} funcs",
            target_value=f"{mismatches} mismatch(es)",
            details={"functions": funcs,
                     "columns": [s.name for s, _ in pairs][:_DETAIL_CAP]},
            source_sql=src_sql, target_sql=tgt_sql))
        return results

    def _aggregates(self, side: str, cols: list, funcs: list[str]):
        connector = self._connector(side)
        d = connector.dialect
        aggs = []
        for i, col in enumerate(cols):
            for f in funcs:
                aggs.append({"func": f, "expr": d.quote(col.name),
                             "alias": f"a{i}_{f.lower()}"})
        sql = self.sqlgen.render(d, "aggregate.sql.j2", ref=self._ref(side), aggs=aggs)
        row = connector.execute_query(sql, timeout_s=self._timeout())[0]
        values: dict[tuple[int, str], float | None] = {}
        pos = 1                                     # row[0] = COUNT(*)
        for i in range(len(cols)):
            for f in funcs:
                raw = row[pos]
                values[(i, f)] = float(raw) if raw is not None else None
                pos += 1
        return values, sql
