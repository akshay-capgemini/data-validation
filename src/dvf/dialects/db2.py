"""DB2 dialects — LUW (11.1+) and z/OS.

* LUW: native HASH_MD5 → row-hash comparison works fully pushdown. DB2 cannot
  parse hex slices to integers, so bucket signatures use aggregate signatures
  (supports_hash_slices=False).
* z/OS: no hash function assumed (supports_hash=False) → MEDIUM tier fetches
  canonical strings and hashes client-side; LARGE uses aggregate signatures.
  DB2 13 users can opt in via connection extra_properties['supports_hash']='true'.
"""

from __future__ import annotations

from typing import ClassVar

from dvf.dialects.base import SQLDialect


class Db2LuwDialect(SQLDialect):
    name: ClassVar[str] = "db2_luw"
    supports_hash: ClassVar[bool] = True
    supports_hash_slices: ClassVar[bool] = False
    supports_except: ClassVar[bool] = True
    quote_open: ClassVar[str] = '"'
    quote_close: ClassVar[str] = '"'
    dummy_from: ClassVar[str] = " FROM SYSIBM.SYSDUMMY1"

    def tablesample_clause(self, pct: float) -> str | None:
        return f"TABLESAMPLE BERNOULLI({pct})"

    def md5_hex_lower(self, expr: str) -> str:
        return f"LOWER(HEX(HASH_MD5({expr})))"

    def regexp_predicate(self, expr: str, pattern: str) -> str:
        return f"REGEXP_LIKE({expr}, {self.literal(pattern)})"

    def cast_to_string(self, expr: str) -> str:
        return f"VARCHAR({expr})"

    def canon_integer(self, expr: str) -> str:
        return f"VARCHAR(CAST({expr} AS BIGINT))"

    def canon_decimal(self, expr: str, scale: int) -> str:
        # DB2 max DECIMAL precision is 31
        return f"VARCHAR(CAST({expr} AS DECIMAL(31,{scale})))"

    def canon_date(self, expr: str) -> str:
        return f"VARCHAR_FORMAT(TIMESTAMP({expr}), 'YYYY-MM-DD')"

    def canon_timestamp(self, expr: str, precision: int) -> str:
        if precision <= 0:
            return f"VARCHAR_FORMAT({expr}, 'YYYY-MM-DD HH24:MI:SS')"
        return f"VARCHAR_FORMAT({expr}, 'YYYY-MM-DD HH24:MI:SS.FF{precision}')"

    def canon_binary(self, expr: str) -> str:
        return f"LOWER(HEX({expr}))"


class Db2ZosDialect(Db2LuwDialect):
    name: ClassVar[str] = "db2_zos"
    supports_hash: ClassVar[bool] = False     # opt-in per connection on DB2 13
    supports_hash_slices: ClassVar[bool] = False

    def tablesample_clause(self, pct: float) -> str | None:
        return None                            # TABLESAMPLE not available on z/OS
