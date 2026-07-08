"""SQL Server dialect (2016+; older versions lack HASHBYTES on VARCHAR(MAX) —
set extra_properties['supports_hash']='false' on the connection to force the
no-hash fallback tiers)."""

from __future__ import annotations

from typing import ClassVar, Sequence

from dvf.dialects.base import SQLDialect


class SqlServerDialect(SQLDialect):
    name: ClassVar[str] = "sqlserver"
    supports_hash: ClassVar[bool] = True
    supports_except: ClassVar[bool] = True
    quote_open: ClassVar[str] = "["
    quote_close: ClassVar[str] = "]"

    def limit_clause(self, sql: str, n: int) -> str:
        # TOP over a wrapped subquery — works without ORDER BY (FETCH does not)
        return f"SELECT TOP ({n}) * FROM (\n{sql}\n) dvf_lim"

    def tablesample_clause(self, pct: float) -> str | None:
        return f"TABLESAMPLE ({pct} PERCENT)"

    def concat_ws(self, sep_literal: str, exprs: Sequence[str]) -> str:
        # First arg cast to VARCHAR(MAX) promotes the whole CONCAT result past
        # the 8000-byte limit so HASHBYTES sees the full canonical string.
        sep = self.literal(sep_literal)
        parts: list[str] = []
        for i, e in enumerate(exprs):
            if i == 0:
                parts.append(f"CAST({e} AS VARCHAR(MAX))")
            else:
                parts.append(sep)
                parts.append(e)
        return f"CONCAT({', '.join(parts)})"

    def md5_hex_lower(self, expr: str) -> str:
        return f"LOWER(CONVERT(VARCHAR(32), HASHBYTES('MD5', {expr}), 2))"

    def hex_slice_to_bigint(self, hash_hex_expr: str, start: int, length: int) -> str:
        return (f"CONVERT(BIGINT, CONVERT(VARBINARY(8), "
                f"'0x' + SUBSTRING({hash_hex_expr}, {start}, {length}), 1))")

    def mod_expr(self, expr: str, modulus: int) -> str:
        return f"(({expr}) % {modulus})"          # no MOD() function

    def bucket_expr(self, key_hash_int_expr: str, modulus: int, depth: int) -> str:
        divided = key_hash_int_expr if depth == 0 else \
            f"FLOOR({key_hash_int_expr} / {modulus ** depth})"
        return self.mod_expr(f"CAST({divided} AS BIGINT)", modulus)

    def cast_to_string(self, expr: str) -> str:
        return f"CONVERT(VARCHAR(4000), {expr})"

    def canon_integer(self, expr: str) -> str:
        return f"CONVERT(VARCHAR(40), CAST({expr} AS BIGINT))"

    def canon_decimal(self, expr: str, scale: int) -> str:
        return f"CONVERT(VARCHAR(60), CAST({expr} AS DECIMAL(38,{scale})))"

    def canon_date(self, expr: str) -> str:
        return f"CONVERT(VARCHAR(10), {expr}, 23)"

    def canon_timestamp(self, expr: str, precision: int) -> str:
        if precision <= 0:
            return f"CONVERT(VARCHAR(19), CAST({expr} AS DATETIME2(0)), 120)"
        return (f"CONVERT(VARCHAR({19 + 1 + precision}), "
                f"CAST({expr} AS DATETIME2({precision})), 121)")

    def canon_boolean(self, expr: str) -> str:
        return (f"CASE WHEN {expr} IS NULL THEN NULL "
                f"WHEN {expr} = 1 THEN '1' ELSE '0' END")

    def canon_binary(self, expr: str) -> str:
        return f"LOWER(CONVERT(VARCHAR(MAX), {expr}, 2))"