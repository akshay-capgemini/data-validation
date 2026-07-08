"""Spark SQL dialect — serves DELTA connections and every FILE_* connector
(files are temp views queried with Spark SQL; that IS their pushdown engine)."""

from __future__ import annotations

from typing import ClassVar, Sequence

from dvf.dialects.base import SQLDialect


class SparkDialect(SQLDialect):
    name: ClassVar[str] = "spark"
    supports_hash: ClassVar[bool] = True
    supports_except: ClassVar[bool] = True
    quote_open: ClassVar[str] = "`"
    quote_close: ClassVar[str] = "`"

    def limit_clause(self, sql: str, n: int) -> str:
        return f"{sql} LIMIT {n}"

    def tablesample_clause(self, pct: float) -> str | None:
        return f"TABLESAMPLE ({pct} PERCENT)"

    def concat_ws(self, sep_literal: str, exprs: Sequence[str]) -> str:
        # NOTE: deliberately NOT concat_ws() — it skips NULLs instead of failing,
        # which would silently break canonical alignment. Inputs are COALESCEd,
        # so plain CONCAT with explicit separators is NULL-safe here.
        sep = self.literal(sep_literal)
        parts: list[str] = []
        for i, e in enumerate(exprs):
            if i:
                parts.append(sep)
            parts.append(e)
        return f"CONCAT({', '.join(parts)})"

    def md5_hex_lower(self, expr: str) -> str:
        return f"md5({expr})"                     # already lowercase hex

    def regexp_predicate(self, expr: str, pattern: str) -> str:
        return f"{expr} RLIKE {self.literal(pattern)}"

    def hex_slice_to_bigint(self, hash_hex_expr: str, start: int, length: int) -> str:
        return f"CAST(CONV(SUBSTRING({hash_hex_expr}, {start}, {length}), 16, 10) AS BIGINT)"

    def mod_expr(self, expr: str, modulus: int) -> str:
        return f"PMOD({expr}, {modulus})"

    def bucket_expr(self, key_hash_int_expr: str, modulus: int, depth: int) -> str:
        divided = key_hash_int_expr if depth == 0 else \
            f"CAST(FLOOR({key_hash_int_expr} / {modulus ** depth}) AS BIGINT)"
        return self.mod_expr(divided, modulus)

    def cast_to_string(self, expr: str) -> str:
        return f"CAST({expr} AS STRING)"

    def canon_integer(self, expr: str) -> str:
        return f"CAST(CAST({expr} AS BIGINT) AS STRING)"

    def canon_decimal(self, expr: str, scale: int) -> str:
        # DECIMAL(38, s) casts render with full scale (trailing zeros kept)
        return f"CAST(CAST({expr} AS DECIMAL(38,{scale})) AS STRING)"

    def canon_date(self, expr: str) -> str:
        return f"DATE_FORMAT({expr}, 'yyyy-MM-dd')"

    def canon_timestamp(self, expr: str, precision: int) -> str:
        if precision <= 0:
            return f"DATE_FORMAT({expr}, 'yyyy-MM-dd HH:mm:ss')"
        return f"DATE_FORMAT({expr}, 'yyyy-MM-dd HH:mm:ss.{'S' * precision}')"

    def canon_boolean(self, expr: str) -> str:
        return (f"CASE WHEN {expr} IS NULL THEN NULL "
                f"WHEN {expr} THEN '1' ELSE '0' END")

    def canon_binary(self, expr: str) -> str:
        return f"LOWER(HEX({expr}))"
