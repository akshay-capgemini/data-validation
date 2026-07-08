"""DeltaConnector — Spark SQL over Unity Catalog / metastore tables or delta paths.

Object resolution:
* connection.database_name = UC catalog (optional)
* validation source_schema = schema/database, source_object = table name
* OR source_object is an absolute path ('/...', 's3://...', 'dbfs:/...', 'abfss://...')
  -> queried as delta.`path`
"""

from __future__ import annotations

from typing import Any, ClassVar

from dvf.connectors.base import BaseConnector, PartitionSpec
from dvf.core.exceptions import ConnectionFailedError, QueryExecutionError
from dvf.metadata.models import ColumnDef

_PATH_PREFIXES = ("/", "s3://", "s3a://", "dbfs:/", "abfss://", "file:/")


class DeltaConnector(BaseConnector):
    connection_types: ClassVar[tuple[str, ...]] = ("DELTA",)

    def connect(self) -> None:
        if self.spark is None:
            from dvf.utils.spark import get_spark
            self.spark = get_spark()
        if self.spark is None:
            raise ConnectionFailedError(
                "DeltaConnector requires an active SparkSession",
                connection_name=self.conn.connection_name)
        self._connected = True

    def disconnect(self) -> None:
        self._connected = False          # never stop the shared SparkSession

    # ------------------------------------------------------------------ #
    def qualified_name(self, schema: str | None, obj: str) -> str:
        if obj.startswith(_PATH_PREFIXES):
            return f"delta.`{obj}`"
        parts = [p for p in (self.conn.database_name, schema, obj) if p]
        return ".".join(f"`{p}`" for p in parts)

    def execute_query(self, sql: str, timeout_s: int | None = None) -> list[tuple]:
        self._ensure()
        try:
            return [tuple(r) for r in self.spark.sql(sql).collect()]
        except Exception as exc:
            raise QueryExecutionError(
                f"Spark SQL failed: {exc}\nSQL: {sql}",
                connection_name=self.conn.connection_name) from exc

    def read_dataframe(self, sql: str, partition: PartitionSpec | None = None):
        self._ensure()
        try:
            return self.spark.sql(sql)
        except Exception as exc:
            raise QueryExecutionError(
                f"Spark SQL failed: {exc}\nSQL: {sql}",
                connection_name=self.conn.connection_name) from exc

    # ------------------------------------------------------------------ #
    def get_schema(self, schema: str | None, obj: str) -> list[ColumnDef]:
        self._ensure()
        name = self.qualified_name(schema, obj)
        rows = self.spark.sql(f"DESCRIBE TABLE {name}").collect()
        cols: list[ColumnDef] = []
        for i, row in enumerate(rows):
            col_name = (row["col_name"] or "").strip()
            if not col_name or col_name.startswith("#"):
                break                    # partition/detail sections follow
            native = (row["data_type"] or "STRING").strip()
            precision = scale = length = None
            if native.upper().startswith("DECIMAL(") and native.endswith(")"):
                inner = native[native.index("(") + 1:-1].split(",")
                precision = int(inner[0])
                scale = int(inner[1]) if len(inner) > 1 else 0
            cols.append(ColumnDef(name=col_name, native_type=native, length=length,
                                  precision=precision, scale=scale, position=i + 1))
        return cols

    def table_exists(self, schema: str | None, obj: str) -> bool:
        self._ensure()
        if obj.startswith(_PATH_PREFIXES):
            return self.file_exists(obj)
        try:
            return self.spark.catalog.tableExists(
                self.qualified_name(schema, obj).replace("`", ""))
        except Exception:
            return False

    def file_exists(self, path: str) -> bool:
        self._ensure()
        try:
            self.spark.sql(f"SELECT 1 FROM delta.`{path}` LIMIT 1").collect()
            return True
        except Exception:
            return False

    def list_objects(self, schema: str | None) -> list[str]:
        self._ensure()
        parts = [p for p in (self.conn.database_name, schema) if p]
        scope = ".".join(f"`{p}`" for p in parts)
        rows = self.spark.sql(f"SHOW TABLES IN {scope}" if scope
                              else "SHOW TABLES").collect()
        return sorted(r["tableName"] for r in rows)

    def _ensure(self) -> None:
        if not self._connected:
            self.connect()
