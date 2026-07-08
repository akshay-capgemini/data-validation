"""SQLDialect — per-engine SQL primitives + the cross-engine canonicalization contract.

The contract: every dialect must render each column into the *byte-identical*
canonical string for equal logical values, so row hashes computed on different
engines are comparable:

* delimiter ``|~|`` between columns, NULL -> ``<NULL>`` (COALESCE per column)
* strings: RTRIM always (CHAR padding); UPPER when case_insensitive
* integers: plain digits, no decimal point
* decimals/floats: fixed scale (default 6), zero-padded (``123.500000``)
* dates: ``YYYY-MM-DD``; timestamps: ``YYYY-MM-DD HH:MI:SS.FFFFFF`` (precision
  configurable; engines with coarser storage truncate before padding)
* booleans: ``1``/``0``; binary: lowercase hex
* hashes: lowercase hex MD5

``canonical_cast`` dispatches on :meth:`type_category` and delegates to the
per-category primitives — dialects override only the primitives.
"""

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from enum import Enum
from typing import Any, ClassVar, Sequence

from dvf.core.exceptions import NotSupportedError, UnsupportedHashError
from dvf.metadata.models import ColumnDef


class TypeCat(str, Enum):
    STRING = "STRING"
    INTEGER = "INTEGER"
    DECIMAL = "DECIMAL"
    FLOAT = "FLOAT"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    BOOLEAN = "BOOLEAN"
    BINARY = "BINARY"
    OTHER = "OTHER"


#: substring -> category, checked in order (first match wins)
_TYPE_KEYWORDS: list[tuple[str, TypeCat]] = [
    ("TIMESTAMP", TypeCat.TIMESTAMP), ("DATETIME", TypeCat.TIMESTAMP),
    ("SMALLDATETIME", TypeCat.TIMESTAMP),
    ("DATE", TypeCat.DATE), ("TIME", TypeCat.OTHER),
    ("BOOL", TypeCat.BOOLEAN), ("BIT", TypeCat.BOOLEAN),
    ("BIGINT", TypeCat.INTEGER), ("SMALLINT", TypeCat.INTEGER),
    ("TINYINT", TypeCat.INTEGER), ("INTEGER", TypeCat.INTEGER), ("INT", TypeCat.INTEGER),
    ("BYTEINT", TypeCat.INTEGER),
    ("DECIMAL", TypeCat.DECIMAL), ("NUMERIC", TypeCat.DECIMAL),
    ("NUMBER", TypeCat.DECIMAL), ("MONEY", TypeCat.DECIMAL),
    ("DOUBLE", TypeCat.FLOAT), ("FLOAT", TypeCat.FLOAT), ("REAL", TypeCat.FLOAT),
    ("BINARY", TypeCat.BINARY), ("BLOB", TypeCat.BINARY), ("RAW", TypeCat.BINARY),
    ("BYTES", TypeCat.BINARY), ("GRAPHIC", TypeCat.STRING),
    ("CHAR", TypeCat.STRING), ("TEXT", TypeCat.STRING), ("STRING", TypeCat.STRING),
    ("CLOB", TypeCat.STRING), ("XML", TypeCat.STRING), ("JSON", TypeCat.STRING),
    ("UNIQUEIDENTIFIER", TypeCat.STRING), ("VARIANT", TypeCat.STRING),
]


@dataclass(frozen=True)
class CanonOptions:
    """Per-validation canonicalization knobs (defaults from framework settings)."""
    delimiter: str = "|~|"
    null_sentinel: str = "<NULL>"
    decimal_scale: int = 6
    ts_precision: int = 6
    case_insensitive: bool = False


class SQLDialect(ABC):
    """Engine-specific SQL primitives. Stateless — one shared instance per engine."""

    name: ClassVar[str] = "base"
    supports_hash: ClassVar[bool] = True
    #: hash hex slices parseable to BIGINT (bucket signatures). DB2/Teradata: False.
    supports_hash_slices: ClassVar[bool] = True
    supports_except: ClassVar[bool] = True
    except_keyword: ClassVar[str] = "EXCEPT"
    quote_open: ClassVar[str] = '"'
    quote_close: ClassVar[str] = '"'
    dummy_from: ClassVar[str] = ""          # e.g. " FROM DUAL", " FROM SYSIBM.SYSDUMMY1"

    # ------------------------------------------------------------------ #
    # Identifiers / literals
    # ------------------------------------------------------------------ #
    def quote(self, ident: str) -> str:
        return f"{self.quote_open}{ident}{self.quote_close}"

    def qualify(self, schema: str | None, obj: str) -> str:
        return f"{self.quote(schema)}.{self.quote(obj)}" if schema else self.quote(obj)

    def literal(self, value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, (int, float)):
            return str(value)
        import datetime as _dt
        if isinstance(value, _dt.datetime):
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S.%f')}'"
        if isinstance(value, _dt.date):
            return f"'{value.isoformat()}'"
        text = str(value).replace("'", "''")
        return f"'{text}'"

    def select_one_sql(self) -> str:
        """Cheap connectivity probe."""
        return f"SELECT 1{self.dummy_from}"

    # ------------------------------------------------------------------ #
    # Statement shaping
    # ------------------------------------------------------------------ #
    def limit_clause(self, sql: str, n: int) -> str:
        """Wrap/extend a SELECT so at most n rows return. ANSI FETCH by default."""
        return f"{sql} FETCH FIRST {n} ROWS ONLY"

    def tablesample_clause(self, pct: float) -> str | None:
        """Suffix after a table reference for percent sampling; None = unsupported."""
        return None

    def sample_predicate(self, pct: float) -> str | None:
        """WHERE-style random sampling fallback for engines without TABLESAMPLE."""
        return None

    def concat_ws(self, sep_literal: str, exprs: Sequence[str]) -> str:
        """Concatenate expressions with a literal separator. ANSI || by default."""
        sep = self.literal(sep_literal)
        joined = f" || {sep} || ".join(exprs)
        return f"({joined})"

    def regexp_predicate(self, expr: str, pattern: str) -> str:
        """Predicate: expr matches regex pattern. Engines without native regex
        (e.g. SQL Server) raise NotSupportedError — use DOMAIN/BUSINESS_RULE there."""
        raise NotSupportedError(f"dialect '{self.name}' has no native regex support")

    def in_list_predicate(self, cols: Sequence[str], keys: Sequence[Sequence[Any]]) -> str:
        """Predicate matching any of the key tuples. OR-groups: portable everywhere."""
        if len(cols) == 1:
            values = ", ".join(self.literal(k[0]) for k in keys)
            return f"{cols[0]} IN ({values})"
        groups = []
        for key in keys:
            parts = [f"{c} = {self.literal(v)}" for c, v in zip(cols, key)]
            groups.append("(" + " AND ".join(parts) + ")")
        return "(" + " OR ".join(groups) + ")"

    # ------------------------------------------------------------------ #
    # Hashing / bucketing (comparison tiers)
    # ------------------------------------------------------------------ #
    def md5_hex_lower(self, expr: str) -> str:
        """Lowercase hex MD5 of a string expression."""
        raise UnsupportedHashError(f"dialect '{self.name}' has no server-side MD5")

    def hex_slice_to_bigint(self, hash_hex_expr: str, start: int, length: int) -> str:
        """Parse a slice of a hex string into a BIGINT (for bucket signatures)."""
        raise NotSupportedError(f"dialect '{self.name}' cannot parse hex slices",)

    def hash_slice_sum(self, hash_hex_expr: str, start: int, length: int) -> str:
        if not self.supports_hash_slices:
            raise NotSupportedError(
                f"dialect '{self.name}' cannot sum hash slices — aggregate "
                "signatures are used instead")
        return f"SUM({self.hex_slice_to_bigint(hash_hex_expr, start, length)})"

    def mod_expr(self, expr: str, modulus: int) -> str:
        return f"MOD({expr}, {modulus})"

    def bucket_expr(self, key_hash_int_expr: str, modulus: int, depth: int) -> str:
        """Independent bucket digit per drill-down level: MOD(FLOOR(h / N^d), N)."""
        divided = key_hash_int_expr if depth == 0 else \
            f"FLOOR({key_hash_int_expr} / {modulus ** depth})"
        return self.mod_expr(divided, modulus)

    # ------------------------------------------------------------------ #
    # Type categorization
    # ------------------------------------------------------------------ #
    def type_category(self, native_type: str) -> TypeCat:
        base = (native_type or "").upper().split("(")[0].strip()
        full = (native_type or "").upper()
        for keyword, cat in _TYPE_KEYWORDS:
            if keyword in base or keyword in full.split("(")[0]:
                return cat
        return TypeCat.OTHER

    # ------------------------------------------------------------------ #
    # Canonicalization — contract implemented here, primitives per dialect
    # ------------------------------------------------------------------ #
    def canonical_cast(self, col: ColumnDef, opts: CanonOptions) -> str:
        """Render a column into its canonical STRING (NULL propagates; caller
        wraps with COALESCE via canonical_column)."""
        expr = self.quote(col.name)
        cat = self.type_category(col.native_type)
        if cat == TypeCat.STRING:
            return self.canon_string(expr, upper=opts.case_insensitive)
        if cat == TypeCat.INTEGER:
            return self.canon_integer(expr)
        if cat == TypeCat.DECIMAL and col.scale == 0:
            # Scale-0 decimals (Oracle NUMBER(10,0), DB2 DECIMAL(18,0)) must hash
            # identically to true integer types on the other side.
            return self.canon_integer(expr)
        if cat in (TypeCat.DECIMAL, TypeCat.FLOAT):
            scale = opts.decimal_scale
            if cat == TypeCat.DECIMAL and col.scale is not None and \
                    col.scale > opts.decimal_scale:
                scale = col.scale       # never lose declared precision
            return self.canon_decimal(expr, scale)
        if cat == TypeCat.DATE:
            return self.canon_date(expr)
        if cat == TypeCat.TIMESTAMP:
            return self.canon_timestamp(expr, opts.ts_precision)
        if cat == TypeCat.BOOLEAN:
            return self.canon_boolean(expr)
        if cat == TypeCat.BINARY:
            return self.canon_binary(expr)
        return self.canon_string(self.cast_to_string(expr), upper=False)

    def canonical_column(self, col: ColumnDef, opts: CanonOptions) -> str:
        """canonical_cast wrapped with the NULL sentinel."""
        return f"COALESCE({self.canonical_cast(col, opts)}, {self.literal(opts.null_sentinel)})"

    def row_canonical_expr(self, cols: Sequence[ColumnDef], opts: CanonOptions) -> str:
        """The full canonical row string: delimiter-joined canonical columns."""
        return self.concat_ws(opts.delimiter, [self.canonical_column(c, opts) for c in cols])

    def row_hash_expr(self, cols: Sequence[ColumnDef], opts: CanonOptions) -> str:
        return self.md5_hex_lower(self.row_canonical_expr(cols, opts))

    # ---- per-category primitives (override per dialect) ----------------- #
    def cast_to_string(self, expr: str) -> str:
        return f"CAST({expr} AS VARCHAR(4000))"

    def canon_string(self, expr: str, upper: bool) -> str:
        inner = f"RTRIM({expr})"
        return f"UPPER({inner})" if upper else inner

    def canon_integer(self, expr: str) -> str:
        return self.cast_to_string(f"CAST({expr} AS BIGINT)")

    def canon_decimal(self, expr: str, scale: int) -> str:
        raise NotSupportedError(f"dialect '{self.name}': canon_decimal not implemented")

    def canon_date(self, expr: str) -> str:
        raise NotSupportedError(f"dialect '{self.name}': canon_date not implemented")

    def canon_timestamp(self, expr: str, precision: int) -> str:
        raise NotSupportedError(f"dialect '{self.name}': canon_timestamp not implemented")

    def canon_boolean(self, expr: str) -> str:
        return (f"CASE WHEN {expr} IS NULL THEN NULL "
                f"WHEN {expr} = 0 THEN '0' ELSE '1' END")

    def canon_binary(self, expr: str) -> str:
        raise NotSupportedError(f"dialect '{self.name}': canon_binary not implemented")
