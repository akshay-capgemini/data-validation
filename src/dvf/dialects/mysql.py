"""MySQL dialect (8.0+). supports_except=False keeps same-connection SMALL-tier
comparison on the portable cross-fetch path (EXCEPT landed only in 8.0.31)."""

from __future__ import annotations

from typing import ClassVar, Sequence

from dvf.dialects.base import SQLDialect


class MySqlDialect(SQLDialect):
    name: ClassVar[str] = "mysql"
    supports_hash: ClassVar[bool] = True
    supports_hash_slices: ClassVar[bool] = True
    supports_except: ClassVar[bool] = False
    quote_open: ClassVar[str] = "`"
    quote_close: ClassVar[str] = "`"

    def limit_clause(self, sql: str, n: int) -> str:
        return f"{sql} LIMIT {n}"

    def sample_predicate(self, pct: float) -> str | None:
        return f"(RAND() * 100) < {pct}"

    def concat_ws(self, sep_literal: str, exprs: Sequence[str]) -> str:
        sep = self.literal(sep_literal)
        parts: list[str] = []
        for i, e in enumerate(exprs):
            if i:
                parts.append(sep)
            parts.append(e)
        return f"CONCAT({', '.join(parts)})"

    def md5_hex_lower(self, expr: str) -> str:
        return f"MD5({expr})"

    def regexp_predicate(self, expr: str, pattern: str) -> str:
        return f"{expr} REGEXP {self.literal(pattern)}"

    def hex_slice_to_bigint(self, hash_hex_expr: str, start: int, length: int) -> str:
        return f"CAST(CONV(SUBSTRING({hash_hex_expr}, {start}, {length}), 16, 10) AS UNSIGNED)"

    def cast_to_string(self, expr: str) -> str:
        return f"CAST({expr} AS CHAR)"

    def canon_integer(self, expr: str) -> str:
        return f"CAST(CAST({expr} AS SIGNED) AS CHAR)"

    def canon_decimal(self, expr: str, scale: int) -> str:
        # DECIMAL -> CHAR keeps trailing zeros at declared scale
        return f"CAST(CAST({expr} AS DECIMAL(38,{scale})) AS CHAR)"

    def canon_date(self, expr: str) -> str:
        return f"DATE_FORMAT({expr}, '%Y-%m-%d')"

    def canon_timestamp(self, expr: str, precision: int) -> str:
        if precision <= 0:
            return f"DATE_FORMAT({expr}, '%Y-%m-%d %H:%i:%s')"
        full = f"DATE_FORMAT({expr}, '%Y-%m-%d %H:%i:%s.%f')"   # %f = 6 digits
        if precision >= 6:
            return full
        return f"LEFT({full}, {19 + 1 + precision})"

    def canon_boolean(self, expr: str) -> str:
        return (f"CASE WHEN {expr} IS NULL THEN NULL "
                f"WHEN {expr} = 0 THEN '0' ELSE '1' END")

    def canon_binary(self, expr: str) -> str:
        return f"LOWER(HEX({expr}))"
