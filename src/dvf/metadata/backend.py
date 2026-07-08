"""Storage backends — one interface, two physical stores.

* DeltaBackend  — Unity Catalog Delta tables (production): catalog.schema.table
* SqliteBackend — single local file (tests/demo): schema_table

Every metadata/audit/result read & write in the framework flows through this
interface, which is what makes the whole engine provable without a cluster.
Dict-typed (JSON) columns are persisted as JSON strings in both stores;
pydantic models re-hydrate them on read.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence

from dvf.core.exceptions import MetadataError
from dvf.metadata.ddl import (
    ALL_TABLES, EXECUTION_HISTORY_SQL, EXECUTION_HISTORY_VIEW, PROFILE_SEEDS,
    Table, get_table,
)

_SQLITE_TYPES = {"STRING": "TEXT", "INT": "INTEGER", "BIGINT": "INTEGER",
                 "DOUBLE": "REAL", "BOOLEAN": "INTEGER", "TIMESTAMP": "TEXT",
                 "JSON": "TEXT"}
_DELTA_TYPES = {"STRING": "STRING", "INT": "INT", "BIGINT": "BIGINT",
                "DOUBLE": "DOUBLE", "BOOLEAN": "BOOLEAN", "TIMESTAMP": "TIMESTAMP",
                "JSON": "STRING"}


class StorageBackend(ABC):
    """Persistence for framework tables (NOT for validated business data)."""

    # ------------------------------------------------------------------ #
    @abstractmethod
    def create_all(self) -> None:
        """Idempotent bootstrap: schemas, tables, execution-history view, seeds."""

    @abstractmethod
    def table_name(self, schema: str, table: str) -> str:
        """Physical, query-ready table name."""

    @abstractmethod
    def read_rows(self, schema: str, table: str,
                  where_eq: Mapping[str, Any] | None = None,
                  where_in: Mapping[str, Sequence[Any]] | None = None) -> list[dict]: ...

    @abstractmethod
    def append_rows(self, schema: str, table: str, rows: list[dict]) -> None: ...

    @abstractmethod
    def merge_rows(self, schema: str, table: str, rows: list[dict]) -> None:
        """Upsert on the table's declared primary key; row may be a partial update."""

    @abstractmethod
    def delete_rows(self, schema: str, table: str, where_eq: Mapping[str, Any]) -> int: ...

    @abstractmethod
    def query(self, sql: str) -> list[dict]:
        """Free-form read (dashboards/reports) against physical names."""

    # ------------------------------------------------------------------ #
    def seed_profiles(self) -> None:
        self.merge_rows("meta", "validation_profiles", list(PROFILE_SEEDS))

    @staticmethod
    def _serialize(table: Table, row: Mapping[str, Any]) -> dict:
        """Full-width row with JSON dicts serialized; unknown keys dropped."""
        out: dict[str, Any] = {}
        for col in table.columns:
            value = row.get(col.name)
            if col.type == "JSON" and isinstance(value, (dict, list)):
                value = json.dumps(value, default=str, sort_keys=True)
            out[col.name] = value
        return out

    @staticmethod
    def _partial(table: Table, row: Mapping[str, Any]) -> dict:
        """Like _serialize but keeps only the keys present (for partial merges)."""
        known = {c.name: c for c in table.columns}
        out: dict[str, Any] = {}
        for key, value in row.items():
            col = known.get(key)
            if col is None:
                continue
            if col.type == "JSON" and isinstance(value, (dict, list)):
                value = json.dumps(value, default=str, sort_keys=True)
            out[key] = value
        return out


# =========================================================================== #
# sqlite backend (tests / demo)
# =========================================================================== #
class SqliteBackend(StorageBackend):
    def __init__(self, path: str):
        self.path = path
        self._lock = threading.RLock()
        self._db = sqlite3.connect(path, timeout=30, check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")

    def close(self) -> None:
        with self._lock:
            self._db.close()

    # ------------------------------------------------------------------ #
    def table_name(self, schema: str, table: str) -> str:
        return f"{schema}_{table}"

    def create_all(self) -> None:
        with self._lock:
            for t in ALL_TABLES:
                cols = ", ".join(f'"{c.name}" {_SQLITE_TYPES[c.type]}' for c in t.columns)
                pk = f', PRIMARY KEY ({", ".join(t.primary_key)})' if t.primary_key else ""
                self._db.execute(
                    f'CREATE TABLE IF NOT EXISTS "{self.table_name(t.schema, t.name)}" ({cols}{pk})')
            view_sql = EXECUTION_HISTORY_SQL.format(
                run_audit=f'"{self.table_name("audit", "run_audit")}"',
                validation_run_audit=f'"{self.table_name("audit", "validation_run_audit")}"',
                validation_config=f'"{self.table_name("meta", "validation_config")}"')
            self._db.execute(
                f'CREATE VIEW IF NOT EXISTS "{self.table_name("audit", EXECUTION_HISTORY_VIEW)}" '
                f"AS {view_sql}")
            self._db.commit()
        self.seed_profiles()

    # ------------------------------------------------------------------ #
    @staticmethod
    def _to_db(value: Any) -> Any:
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc).isoformat(timespec="microseconds") \
                if value.tzinfo else value.isoformat(timespec="microseconds")
        return value

    def _where(self, where_eq, where_in) -> tuple[str, list]:
        clauses, params = [], []
        for col, val in (where_eq or {}).items():
            if val is None:
                clauses.append(f'"{col}" IS NULL')
            else:
                clauses.append(f'"{col}" = ?')
                params.append(self._to_db(val))
        for col, vals in (where_in or {}).items():
            vals = list(vals)
            if not vals:
                clauses.append("1 = 0")
                continue
            clauses.append(f'"{col}" IN ({", ".join("?" for _ in vals)})')
            params.extend(self._to_db(v) for v in vals)
        return (" WHERE " + " AND ".join(clauses)) if clauses else "", params

    def read_rows(self, schema, table, where_eq=None, where_in=None) -> list[dict]:
        name = self.table_name(schema, table)
        where, params = self._where(where_eq, where_in)
        with self._lock:
            cur = self._db.execute(f'SELECT * FROM "{name}"{where}', params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]

    def append_rows(self, schema, table, rows) -> None:
        if not rows:
            return
        t = get_table(schema, table)
        name = self.table_name(schema, table)
        col_names = t.column_names()
        placeholders = ", ".join("?" for _ in col_names)
        col_sql = ", ".join(f'"{c}"' for c in col_names)
        prepared = [tuple(self._to_db(self._serialize(t, r)[c]) for c in col_names) for r in rows]
        with self._lock:
            self._db.executemany(
                f'INSERT INTO "{name}" ({col_sql}) VALUES ({placeholders})', prepared)
            self._db.commit()

    def merge_rows(self, schema, table, rows) -> None:
        if not rows:
            return
        t = get_table(schema, table)
        if not t.primary_key:
            raise MetadataError(f"merge_rows: {schema}.{table} has no primary key")
        name = self.table_name(schema, table)
        with self._lock:
            for row in rows:
                data = self._partial(t, row)
                missing = [k for k in t.primary_key if k not in data]
                if missing:
                    raise MetadataError(
                        f"merge_rows into {schema}.{table}: missing key column(s) {missing}")
                cols = list(data.keys())
                col_sql = ", ".join(f'"{c}"' for c in cols)
                placeholders = ", ".join("?" for _ in cols)
                updates = [c for c in cols if c not in t.primary_key]
                if updates:
                    set_sql = ", ".join(f'"{c}" = excluded."{c}"' for c in updates)
                    conflict = f'ON CONFLICT ({", ".join(t.primary_key)}) DO UPDATE SET {set_sql}'
                else:
                    conflict = f'ON CONFLICT ({", ".join(t.primary_key)}) DO NOTHING'
                self._db.execute(
                    f'INSERT INTO "{name}" ({col_sql}) VALUES ({placeholders}) {conflict}',
                    tuple(self._to_db(data[c]) for c in cols))
            self._db.commit()

    def delete_rows(self, schema, table, where_eq) -> int:
        name = self.table_name(schema, table)
        where, params = self._where(where_eq, None)
        if not where:
            raise MetadataError("delete_rows requires a filter")
        with self._lock:
            cur = self._db.execute(f'DELETE FROM "{name}"{where}', params)
            self._db.commit()
            return cur.rowcount

    def query(self, sql: str) -> list[dict]:
        with self._lock:
            cur = self._db.execute(sql)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


# =========================================================================== #
# Delta / Unity Catalog backend (production)
# =========================================================================== #
class DeltaBackend(StorageBackend):
    def __init__(self, spark, catalog: str):
        if spark is None:
            raise MetadataError("DeltaBackend requires a SparkSession")
        self.spark = spark
        self.catalog = catalog

    # ------------------------------------------------------------------ #
    def table_name(self, schema: str, table: str) -> str:
        return f"`{self.catalog}`.`{schema}`.`{table}`"

    def create_all(self) -> None:
        for schema in ("meta", "audit", "results"):
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{self.catalog}`.`{schema}`")
        for t in ALL_TABLES:
            cols = ", ".join(f"`{c.name}` {_DELTA_TYPES[c.type]}" for c in t.columns)
            cluster = f" CLUSTER BY ({', '.join(f'`{c}`' for c in t.cluster_by)})" \
                if t.cluster_by else ""
            self.spark.sql(
                f"CREATE TABLE IF NOT EXISTS {self.table_name(t.schema, t.name)} "
                f"({cols}) USING DELTA{cluster}")
        view_sql = EXECUTION_HISTORY_SQL.format(
            run_audit=self.table_name("audit", "run_audit"),
            validation_run_audit=self.table_name("audit", "validation_run_audit"),
            validation_config=self.table_name("meta", "validation_config"))
        self.spark.sql(
            f"CREATE OR REPLACE VIEW `{self.catalog}`.`audit`.`{EXECUTION_HISTORY_VIEW}` "
            f"AS {view_sql}")
        self.seed_profiles()

    # ------------------------------------------------------------------ #
    def _struct_type(self, t: Table):
        from pyspark.sql import types as T
        mapping = {"STRING": T.StringType(), "INT": T.IntegerType(),
                   "BIGINT": T.LongType(), "DOUBLE": T.DoubleType(),
                   "BOOLEAN": T.BooleanType(), "TIMESTAMP": T.TimestampType(),
                   "JSON": T.StringType()}
        return T.StructType([T.StructField(c.name, mapping[c.type], True) for c in t.columns])

    def _df(self, t: Table, rows: list[dict]):
        data = []
        for row in rows:
            full = self._serialize(t, row)
            data.append(tuple(self._coerce(full[c.name], c.type) for c in t.columns))
        return self.spark.createDataFrame(data, schema=self._struct_type(t))

    @staticmethod
    def _coerce(value: Any, logical_type: str) -> Any:
        if value is None:
            return None
        if logical_type in ("INT", "BIGINT"):
            return int(value)
        if logical_type == "DOUBLE":
            return float(value)
        if logical_type == "BOOLEAN":
            return bool(value)
        if logical_type == "TIMESTAMP" and isinstance(value, str):
            return datetime.fromisoformat(value)
        if logical_type in ("STRING", "JSON") and not isinstance(value, str):
            return str(value)
        return value

    def _literal(self, value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, datetime):
            return f"TIMESTAMP '{value.strftime('%Y-%m-%d %H:%M:%S.%f')}'"
        return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"

    def _where(self, where_eq, where_in) -> str:
        clauses = []
        for col, val in (where_eq or {}).items():
            clauses.append(f"`{col}` IS NULL" if val is None
                           else f"`{col}` = {self._literal(val)}")
        for col, vals in (where_in or {}).items():
            vals = list(vals)
            clauses.append("1 = 0" if not vals else
                           f"`{col}` IN ({', '.join(self._literal(v) for v in vals)})")
        return (" WHERE " + " AND ".join(clauses)) if clauses else ""

    def read_rows(self, schema, table, where_eq=None, where_in=None) -> list[dict]:
        sql = f"SELECT * FROM {self.table_name(schema, table)}{self._where(where_eq, where_in)}"
        return [row.asDict() for row in self.spark.sql(sql).collect()]

    def append_rows(self, schema, table, rows) -> None:
        if not rows:
            return
        t = get_table(schema, table)
        (self._df(t, rows).write.format("delta").mode("append")
             .saveAsTable(f"{self.catalog}.{t.schema}.{t.name}"))

    def merge_rows(self, schema, table, rows) -> None:
        if not rows:
            return
        t = get_table(schema, table)
        if not t.primary_key:
            raise MetadataError(f"merge_rows: {schema}.{table} has no primary key")
        # Group rows by their provided column set so each MERGE is uniform.
        by_shape: dict[tuple, list[dict]] = {}
        for row in rows:
            data = self._partial(t, row)
            missing = [k for k in t.primary_key if k not in data]
            if missing:
                raise MetadataError(
                    f"merge_rows into {schema}.{table}: missing key column(s) {missing}")
            by_shape.setdefault(tuple(sorted(data.keys())), []).append(data)
        for shape, shaped_rows in by_shape.items():
            cols = list(shape)
            sub = Table(t.schema, t.name,
                        tuple(c for c in t.columns if c.name in shape), t.primary_key)
            df = self._df(sub, shaped_rows)
            view = f"dvf_merge_{t.schema}_{t.name}"
            df.createOrReplaceTempView(view)
            on = " AND ".join(f"tgt.`{k}` = src.`{k}`" for k in t.primary_key)
            updates = [c for c in cols if c not in t.primary_key]
            update_sql = (f"WHEN MATCHED THEN UPDATE SET "
                          + ", ".join(f"tgt.`{c}` = src.`{c}`" for c in updates)) if updates else ""
            insert_cols = ", ".join(f"`{c}`" for c in cols)
            insert_vals = ", ".join(f"src.`{c}`" for c in cols)
            self.spark.sql(
                f"MERGE INTO {self.table_name(schema, table)} tgt "
                f"USING {view} src ON {on} {update_sql} "
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})")
            self.spark.catalog.dropTempView(view)

    def delete_rows(self, schema, table, where_eq) -> int:
        where = self._where(where_eq, None)
        if not where:
            raise MetadataError("delete_rows requires a filter")
        self.spark.sql(f"DELETE FROM {self.table_name(schema, table)}{where}")
        return -1  # Spark DELETE does not report row counts portably

    def query(self, sql: str) -> list[dict]:
        return [row.asDict() for row in self.spark.sql(sql).collect()]
