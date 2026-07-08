"""Oracle dialect (12c+ for STANDARD_HASH and FETCH FIRST)."""

from __future__ import annotations

from typing import ClassVar

from dvf.dialects.base import SQLDialect


class OracleDialect(SQLDialect):
    name: ClassVar[str] = "oracle"
    supports_hash: ClassVar[bool] = True
    supports_except: ClassVar[bool] = True
    except_keyword: ClassVar[str] = "MINUS"
    quote_open: ClassVar[str] = '"'
    quote_close: ClassVar[str] = '"'
    dummy_from: ClassVar[str] = " FROM DUAL"

    def tablesample_clause(self, pct: float) -> str | None:
        return f"SAMPLE({pct})"

    def md5_hex_lower(self, expr: str) -> str:
        return f"LOWER(RAWTOHEX(STANDARD_HASH({expr}, 'MD5')))"

    def hex_slice_to_bigint(self, hash_hex_expr: str, start: int, length: int) -> str:
        return f"TO_NUMBER(SUBSTR({hash_hex_expr}, {start}, {length}), '{'X' * length}')"

    def cast_to_string(self, expr: str) -> str:
        return f"TO_CHAR({expr})"

    def canon_integer(self, expr: str) -> str:
        return f"TO_CHAR(CAST({expr} AS NUMBER(38,0)))"

    def canon_decimal(self, expr: str, scale: int) -> str:
        # FM strips padding; the trailing 0-block keeps fixed scale ('1.500000').
        int_part = "9" * 37 + "0"
        fmt = f"FM{int_part}.{'0' * scale}" if scale > 0 else f"FM{int_part}"
        return f"TO_CHAR(CAST({expr} AS NUMBER(38,{scale})), '{fmt}')"

    def canon_date(self, expr: str) -> str:
        return f"TO_CHAR({expr}, 'YYYY-MM-DD')"

    def canon_timestamp(self, expr: str, precision: int) -> str:
        if precision <= 0:
            return f"TO_CHAR({expr}, 'YYYY-MM-DD HH24:MI:SS')"
        return f"TO_CHAR({expr}, 'YYYY-MM-DD HH24:MI:SS.FF{precision}')"

    def canon_boolean(self, expr: str) -> str:
        return (f"CASE WHEN {expr} IS NULL THEN NULL "
                f"WHEN {expr} = 0 THEN '0' ELSE '1' END")

    def canon_binary(self, expr: str) -> str:
        return f"LOWER(RAWTOHEX({expr}))"