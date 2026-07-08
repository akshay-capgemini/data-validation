"""DataProfiler — pushdown column statistics for one side of a validation.

One aggregate query covers row count + per-column null/distinct/min/max and the
numeric moments (SUM, SUM(x²)) from which AVG/STDDEV derive CLIENT-SIDE — so the
same SQL works on every engine (sqlite included, which lacks STDDEV). Top-N
value distributions add one small GROUP BY per profiled column (capped).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

from dvf.dialects import TypeCat
from dvf.metadata.models import ColumnDef


class ProfileMetrics:
    ROW_COUNT = "ROW_COUNT"
    NULL_COUNT = "NULL_COUNT"
    DISTINCT_COUNT = "DISTINCT_COUNT"
    BLANK_COUNT = "BLANK_COUNT"
    MIN = "MIN"
    MAX = "MAX"
    AVG = "AVG"
    STDDEV = "STDDEV"
    SUM = "SUM"
    MIN_LENGTH = "MIN_LENGTH"
    MAX_LENGTH = "MAX_LENGTH"
    TOP_VALUES = "TOP_VALUES"


@dataclass
class ColumnProfile:
    column: str
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass
class TableProfile:
    row_count: int = 0
    columns: list[ColumnProfile] = field(default_factory=list)
    executed_sql: list[str] = field(default_factory=list)


_NUMERIC = {TypeCat.INTEGER, TypeCat.DECIMAL, TypeCat.FLOAT}


class DataProfiler:
    def __init__(self, connector, sqlgen, settings):
        self.connector = connector
        self.d = connector.dialect
        self.sqlgen = sqlgen
        self.settings = settings

    # ------------------------------------------------------------------ #
    def profile(self, ref, columns: list[ColumnDef], *,
                top_n: int = 5, top_value_columns: int = 5,
                distinct: bool = True) -> TableProfile:
        result = TableProfile()
        if not columns:
            return result
        specs = self._build_specs(columns, distinct)
        sql = self.sqlgen.render(self.d, "aggregate.sql.j2", ref=ref,
                                 aggs=[{"func": s["func"], "expr": s["expr"],
                                        "alias": s["alias"]} for s in specs])
        result.executed_sql.append(sql)
        row = self.connector.execute_query(
            sql, timeout_s=self.settings.engine.query_timeout_s)[0]
        result.row_count = int(row[0] or 0)
        self._decode(columns, specs, row, result)

        for col in columns[:top_value_columns]:
            top = self._top_values(ref, col, top_n)
            profile = next(c for c in result.columns if c.column == col.name)
            profile.metrics[ProfileMetrics.TOP_VALUES] = top
        return result

    # ------------------------------------------------------------------ #
    def _build_specs(self, columns: list[ColumnDef], distinct: bool) -> list[dict]:
        specs: list[dict] = []
        for i, col in enumerate(columns):
            q = self.d.quote(col.name)
            cat = self.d.type_category(col.native_type)
            specs.append({"func": "COUNT", "expr": q, "alias": f"c{i}_nn",
                          "column": col.name, "metric": "_nonnull"})
            if distinct:
                specs.append({"func": "COUNT", "expr": f"DISTINCT {q}",
                              "alias": f"c{i}_dc", "column": col.name,
                              "metric": ProfileMetrics.DISTINCT_COUNT})
            specs.append({"func": "MIN", "expr": q, "alias": f"c{i}_mn",
                          "column": col.name, "metric": ProfileMetrics.MIN})
            specs.append({"func": "MAX", "expr": q, "alias": f"c{i}_mx",
                          "column": col.name, "metric": ProfileMetrics.MAX})
            if cat in _NUMERIC:
                dbl = f"CAST({q} AS DOUBLE PRECISION)" \
                    if self.d.name in ("postgres", "oracle") else f"CAST({q} AS DOUBLE)"
                if self.d.name == "sqlserver":
                    dbl = f"CAST({q} AS FLOAT)"
                specs.append({"func": "SUM", "expr": dbl, "alias": f"c{i}_sm",
                              "column": col.name, "metric": ProfileMetrics.SUM})
                specs.append({"func": "SUM", "expr": f"({dbl}) * ({dbl})",
                              "alias": f"c{i}_sq", "column": col.name,
                              "metric": "_sumsq"})
            elif cat == TypeCat.STRING:
                specs.append({"func": "MIN", "expr": f"LENGTH(RTRIM({q}))",
                              "alias": f"c{i}_ln", "column": col.name,
                              "metric": ProfileMetrics.MIN_LENGTH})
                specs.append({"func": "MAX", "expr": f"LENGTH(RTRIM({q}))",
                              "alias": f"c{i}_lx", "column": col.name,
                              "metric": ProfileMetrics.MAX_LENGTH})
        return specs

    def _decode(self, columns, specs, row, result: TableProfile) -> None:
        values = dict(zip((s["alias"] for s in specs), row[1:]))
        for col in columns:
            profile = ColumnProfile(column=col.name)
            raw = {s["metric"]: values.get(s["alias"])
                   for s in specs if s["column"] == col.name}
            nonnull = int(raw.pop("_nonnull", 0) or 0)
            profile.metrics[ProfileMetrics.NULL_COUNT] = result.row_count - nonnull
            for metric, value in raw.items():
                if metric == "_sumsq":
                    continue
                if value is not None or metric in (ProfileMetrics.MIN, ProfileMetrics.MAX):
                    profile.metrics[metric] = value
            total = raw.get(ProfileMetrics.SUM)
            sumsq = raw.get("_sumsq")
            if total is not None and nonnull > 0:
                mean = float(total) / nonnull
                profile.metrics[ProfileMetrics.AVG] = round(mean, 6)
                if sumsq is not None:
                    variance = max(float(sumsq) / nonnull - mean * mean, 0.0)
                    profile.metrics[ProfileMetrics.STDDEV] = round(math.sqrt(variance), 6)
            result.columns.append(profile)

    def _top_values(self, ref, col: ColumnDef, top_n: int) -> list[dict]:
        q = self.d.quote(col.name)
        part_expr = f"COALESCE({self.d.cast_to_string(q)}, '<NULL>')"
        inner = self.sqlgen.render(self.d, "partition_count.sql.j2", ref=ref,
                                   part_expr=part_expr)
        sql = self.d.limit_clause(f"{inner}\nORDER BY cnt DESC", top_n)
        rows = self.connector.execute_query(
            sql, timeout_s=self.settings.engine.query_timeout_s)
        return [{"value": str(r[0]), "count": int(r[1] or 0)} for r in rows]
