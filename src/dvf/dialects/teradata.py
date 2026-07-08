"""Teradata dialect (16.20+ for ANSI FETCH FIRST; TO_CHAR needs 14.0+).

No portable MD5 (HASHROW is a proprietary 4-byte hash) → supports_hash=False:
MEDIUM tier fetches canonical strings and hashes client-side; LARGE uses
aggregate signatures.
"""

from __future__ import annotations

from typing import ClassVar

from dvf.dialects.base import SQLDialect


class TeradataDialect(SQLDialect):
    name: ClassVar[str] = "teradata"
    supports_hash: ClassVar[bool] = False
    supports_hash_slices: ClassVar[bool] = False
    supports_except: ClassVar[bool] = True
    except_keyword: ClassVar[str] = "MINUS"
    quote_open: ClassVar[str] = '"'
    quote_close: ClassVar[str] = '"'

    def regexp_predicate(self, expr: str, pattern: str) -> str:
        return f"REGEXP_SIMILAR({expr}, {self.literal(pattern)}, 'c') = 1"

    def cast_to_string(self, expr: str) -> str:
        return f"CAST({expr} AS VARCHAR(4000))"

    def canon_integer(self, expr: str) -> str:
        return f"TRIM(CAST(CAST({expr} AS BIGINT) AS VARCHAR(40)))"

    def canon_decimal(self, expr: str, scale: int) -> str:
        int_part = "9" * 37 + "0"
        fmt = f"FM{int_part}.{'0' * scale}" if scale > 0 else f"FM{int_part}"
        return f"TO_CHAR(CAST({expr} AS DECIMAL(38,{scale})), '{fmt}')"

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
        return f"LOWER(FROM_BYTES({expr}, 'base16'))"
