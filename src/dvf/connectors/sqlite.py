"""SqliteConnector — zero-infrastructure engine for tests and demos.

NOT for production validation. It exists so the full framework (engine, all
validators, every comparison tier) runs and is provable without any external
system. Registers Python UDFs at connect time so SqliteDialect can hash/bucket:
``dvf_md5(text)`` and ``dvf_hex_to_int(hex_text)``.
"""

from __future__ import annotations

import hashlib
import sqlite3
from typing import Any, ClassVar

from dvf.connectors.base import BaseConnector
from dvf.core.exceptions import (
    ConnectionFailedError, QueryExecutionError,
)
from dvf.metadata.models import ColumnDef, ConstraintDef, IndexDef


def _udf_md5(value: Any) -> str | None:
    if value is None:
        return None
    return hashlib.md5(str(value).encode("utf-8")).hexdigest()


def _udf_hex_to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value), 16)
    except ValueError:
        return None


def _udf_regexp(pattern: Any, value: Any) -> int | None:
    """sqlite REGEXP operator: `value REGEXP pattern` calls regexp(pattern, value)."""
    if pattern is None or value is None:
        return None
    import re
    return 1 if re.search(str(pattern), str(value)) else 0


class SqliteConnector(BaseConnector):
    connection_types: ClassVar[tuple[str, ...]] = ("SQLITE",)

    def __init__(self, conn, secrets=None, spark=None):
        super().__init__(conn, secrets, spark)
        self._db: sqlite3.Connection | None = None

    # ------------------------------------------------------------------ #
    def connect(self) -> None:
        if self._connected:
            return
        path = self.conn.database_name or ":memory:"
        try:
            self._db = sqlite3.connect(path, timeout=30, check_same_thread=False)
        except sqlite3.Error as exc:
            raise ConnectionFailedError(
                f"sqlite connect failed for '{path}': {exc}",
                connection_name=self.conn.connection_name) from exc
        self._db.create_function("dvf_md5", 1, _udf_md5, deterministic=True)
        self._db.create_function("dvf_hex_to_int", 1, _udf_hex_to_int, deterministic=True)
        self._db.create_function("regexp", 2, _udf_regexp, deterministic=True)
        self._connected = True

    def disconnect(self) -> None:
        if self._db is not None:
            self._db.close()
            self._db = None
        self._connected = False

    # ------------------------------------------------------------------ #
    def execute_query(self, sql: str, timeout_s: int | None = None) -> list[tuple]:
        self._ensure()
        try:
            cur = self._db.execute(sql)
            return [tuple(r) for r in cur.fetchall()]
        except sqlite3.Error as exc:
            raise QueryExecutionError(
                f"sqlite query failed: {exc}\nSQL: {sql}",
                connection_name=self.conn.connection_name) from exc

    def execute_non_query(self, sql: str) -> int:
        self._ensure()
        try:
            cur = self._db.execute(sql)
            self._db.commit()
            return cur.rowcount
        except sqlite3.Error as exc:
            raise QueryExecutionError(
                f"sqlite statement failed: {exc}\nSQL: {sql}",
                connection_name=self.conn.connection_name) from exc

    # ------------------------------------------------------------------ #
    def get_schema(self, schema: str | None, obj: str) -> list[ColumnDef]:
        self._ensure()
        rows = self._db.execute(f"PRAGMA table_info({self.dialect.quote(obj)})").fetchall()
        cols: list[ColumnDef] = []
        for cid, name, ctype, notnull, default, _pk in rows:
            native = ctype or "TEXT"
            length = precision = scale = None
            if "(" in native:
                inner = native[native.index("(") + 1: native.rindex(")")]
                parts = [p.strip() for p in inner.split(",")]
                if len(parts) == 2:
                    precision, scale = int(parts[0]), int(parts[1])
                elif parts[0].isdigit():
                    length = int(parts[0])
            cols.append(ColumnDef(
                name=name, native_type=native, length=length, precision=precision,
                scale=scale, nullable=not notnull, position=cid + 1,
                default_value=str(default) if default is not None else None))
        return cols

    def get_primary_keys(self, schema: str | None, obj: str) -> list[str]:
        self._ensure()
        rows = self._db.execute(f"PRAGMA table_info({self.dialect.quote(obj)})").fetchall()
        pk_cols = [(r[5], r[1]) for r in rows if r[5] > 0]
        return [name for _, name in sorted(pk_cols)]

    def get_indexes(self, schema: str | None, obj: str) -> list[IndexDef]:
        self._ensure()
        result: list[IndexDef] = []
        for row in self._db.execute(f"PRAGMA index_list({self.dialect.quote(obj)})").fetchall():
            idx_name, unique = row[1], bool(row[2])
            cols = [r[2] for r in self._db.execute(f"PRAGMA index_info({self.dialect.quote(idx_name)})").fetchall()]
            result.append(IndexDef(name=idx_name, columns=cols, unique=unique))
        return result

    def get_constraints(self, schema: str | None, obj: str) -> list[ConstraintDef]:
        self._ensure()
        result: list[ConstraintDef] = []
        pks = self.get_primary_keys(schema, obj)
        if pks:
            result.append(ConstraintDef(name=f"pk_{obj}", constraint_type="PRIMARY KEY", columns=pks))
        for row in self._db.execute(f"PRAGMA foreign_key_list({self.dialect.quote(obj)})").fetchall():
            result.append(ConstraintDef(
                name=f"fk_{obj}_{row[0]}", constraint_type="FOREIGN KEY",
                columns=[row[3]], definition=f"REFERENCES {row[2]}({row[4]})"))
        return result

    def table_exists(self, schema: str | None, obj: str) -> bool:
        self._ensure()
        row = self._db.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type IN ('table','view') AND name = ?",
            (obj,)).fetchone()
        return bool(row and row[0])

    def list_objects(self, schema: str | None) -> list[str]:
        self._ensure()
        rows = self._db.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table','view') "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name").fetchall()
        return [r[0] for r in rows]

    # ------------------------------------------------------------------ #
    def _ensure(self) -> None:
        if not self._connected:
            self.connect()
