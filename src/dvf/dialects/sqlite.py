"""SQLite dialect — test/demo engine (NOT for production data validation).

SqliteConnector registers Python UDFs ``dvf_md5(text)`` and ``dvf_hex_to_int(text)``
at connect time, giving sqlite full hash/bucket capability so the entire engine —
including comparison tiers and no-hash fallbacks (via supports_hash toggling in
tests) — runs with zero external infrastructure.
"""

from __future__ import annotations

from typing import ClassVar

from dvf.dialects.base import SQLDialect


class SqliteDialect(SQLDialect):
    name: ClassVar[str] = "sqlite"
    supports_hash: ClassVar[bool] = True          # via registered Python UDF
    supports_except: ClassVar[bool] = True
    quote_open: ClassVar[str] = '"'
    quote_close: ClassVar[str] = '"'

    def limit_clause(self, sql: str, n: int) -> str:
        return f"{sql} LIMIT {n}"

    def sample_predicate(self, pct: float) -> str | None:
        return f"(ABS(RANDOM()) % 100) < {pct}"

    def md5_hex_lower(self, expr: str) -> str:
        return f"dvf_md5({expr})"                 # UDF returns lowercase hex

    def regexp_predicate(self, expr: str, pattern: str) -> str:
        return f"{expr} REGEXP {self.literal(pattern)}"   # UDF-backed operator

    def hex_slice_to_bigint(self, hash_hex_expr: str, start: int, length: int) -> str:
        return f"dvf_hex_to_int(SUBSTR({hash_hex_expr}, {start}, {length}))"

    def mod_expr(self, expr: str, modulus: int) -> str:
        return f"(({expr}) % {modulus})"          # sqlite has no MOD()

    def bucket_expr(self, key_hash_int_expr: str, modulus: int, depth: int) -> str:
        divided = key_hash_int_expr if depth == 0 else \
            f"CAST(({key_hash_int_expr}) / {modulus ** depth} AS INTEGER)"
        return self.mod_expr(divided, modulus)

    def cast_to_string(self, expr: str) -> str:
        return f"CAST({expr} AS TEXT)"

    def canon_integer(self, expr: str) -> str:
        return f"CAST(CAST({expr} AS INTEGER) AS TEXT)"

    def canon_decimal(self, expr: str, scale: int) -> str:
        return f"printf('%.{scale}f', {expr})"

    def canon_date(self, expr: str) -> str:
        return f"strftime('%Y-%m-%d', {expr})"

    def canon_timestamp(self, expr: str, precision: int) -> str:
        # %f -> SS.SSS (milliseconds). Pad to the requested precision.
        if precision <= 0:
            return f"strftime('%Y-%m-%d %H:%M:%S', {expr})"
        if precision <= 3:
            base = f"strftime('%Y-%m-%d %H:%M:%f', {expr})"
            return f"SUBSTR({base}, 1, {19 + 1 + precision})"
        pad = "0" * (precision - 3)
        return f"(strftime('%Y-%m-%d %H:%M:%f', {expr}) || '{pad}')"

    def canon_boolean(self, expr: str) -> str:
        return (f"CASE WHEN {expr} IS NULL THEN NULL "
                f"WHEN {expr} IN (0, '0', 'false', 'FALSE') THEN '0' ELSE '1' END")

    def canon_binary(self, expr: str) -> str:
        return f"LOWER(HEX({expr}))"
