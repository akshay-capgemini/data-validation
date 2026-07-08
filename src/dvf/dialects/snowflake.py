"""Snowflake dialect."""

from __future__ import annotations

from typing import ClassVar

from dvf.dialects.base import SQLDialect


class SnowflakeDialect(SQLDialect):
    name: ClassVar[str] = "snowflake"
    supports_hash: ClassVar[bool] = True
    supports_except: ClassVar[bool] = True
    except_keyword: ClassVar[str] = "MINUS"       # EXCEPT also valid; MINUS is canonical
    quote_open: ClassVar[str] = '"'
    quote_close: ClassVar[str] = '"'

    def limit_clause(self, sql: str, n: int) -> str:
        return f"{sql} LIMIT {n}"

    def tablesample_clause(self, pct: float) -> str | None:
        return f"SAMPLE ({pct})"

    def md5_hex_lower(self, expr: str) -> str:
        return f"MD5({expr})"                     # returns lowercase hex

    def regexp_predicate(self, expr: str, pattern: str) -> str:
        return f"REGEXP_LIKE({expr}, {self.literal(pattern)})"

    def hex_slice_to_bigint(self, hash_hex_expr: str, start: int, length: int) -> str:
        return (f"TO_NUMBER(SUBSTR({hash_hex_expr}, {start}, {length}), "
                f"'{'X' * length}')")

    def cast_to_string(self, expr: str) -> str:
        return f"TO_VARCHAR({expr})"

    def canon_integer(self, expr: str) -> str:
        return f"TO_VARCHAR(CAST({expr} AS NUMBER(38,0)))"

    def canon_decimal(self, expr: str, scale: int) -> str:
        # NUMBER(38,s) -> VARCHAR keeps trailing zeros at full scale
        return f"TO_VARCHAR(CAST({expr} AS NUMBER(38,{scale})))"

    def canon_date(self, expr: str) -> str:
        return f"TO_VARCHAR({expr}, 'YYYY-MM-DD')"

    def canon_timestamp(self, expr: str, precision: int) -> str:
        if precision <= 0:
            return f"TO_VARCHAR({expr}, 'YYYY-MM-DD HH24:MI:SS')"
        return f"TO_VARCHAR({expr}, 'YYYY-MM-DD HH24:MI:SS.FF{precision}')"

    def canon_boolean(self, expr: str) -> str:
        return (f"CASE WHEN {expr} IS NULL THEN NULL "
                f"WHEN {expr} THEN '1' ELSE '0' END")

    def canon_binary(self, expr: str) -> str:
        return f"LOWER(TO_VARCHAR({expr}, 'HEX'))"
