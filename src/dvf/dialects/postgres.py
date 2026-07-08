"""PostgreSQL dialect."""

from __future__ import annotations

from typing import ClassVar

from dvf.dialects.base import SQLDialect


class PostgresDialect(SQLDialect):
    name: ClassVar[str] = "postgres"
    supports_hash: ClassVar[bool] = True
    supports_except: ClassVar[bool] = True
    quote_open: ClassVar[str] = '"'
    quote_close: ClassVar[str] = '"'

    def limit_clause(self, sql: str, n: int) -> str:
        return f"{sql} LIMIT {n}"

    def tablesample_clause(self, pct: float) -> str | None:
        return f"TABLESAMPLE BERNOULLI ({pct})"

    def md5_hex_lower(self, expr: str) -> str:
        return f"MD5({expr})"

    def hex_slice_to_bigint(self, hash_hex_expr: str, start: int, length: int) -> str:
        return (f"(('x' || LPAD(SUBSTR({hash_hex_expr}, {start}, {length}), 16, '0'))"
                f"::bit(64)::bigint)")

    def cast_to_string(self, expr: str) -> str:
        return f"CAST({expr} AS VARCHAR)"

    def canon_integer(self, expr: str) -> str:
        return f"CAST(CAST({expr} AS BIGINT) AS VARCHAR)"

    def canon_decimal(self, expr: str, scale: int) -> str:
        # NUMERIC(38,s) -> VARCHAR keeps trailing zeros at declared scale
        return f"CAST(CAST({expr} AS NUMERIC(38,{scale})) AS VARCHAR)"

    def canon_date(self, expr: str) -> str:
        return f"TO_CHAR({expr}, 'YYYY-MM-DD')"

    def canon_timestamp(self, expr: str, precision: int) -> str:
        if precision <= 0:
            return f"TO_CHAR({expr}, 'YYYY-MM-DD HH24:MI:SS')"
        full = f"TO_CHAR({expr}, 'YYYY-MM-DD HH24:MI:SS.US')"
        if precision >= 6:
            return full
        return f"LEFT({full}, {19 + 1 + precision})"

    def canon_boolean(self, expr: str) -> str:
        return (f"CASE WHEN {expr} IS NULL THEN NULL "
                f"WHEN {expr} THEN '1' ELSE '0' END")

    def canon_binary(self, expr: str) -> str:
        return f"ENCODE({expr}, 'hex')"