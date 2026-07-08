"""BaseConnector — the single interface every source/target system implements.

Adding a new system = implement this ABC (+ an SQLDialect) and register it with
the ConnectorFactory. No validation-engine changes required.

Two data paths:
* control path — execute_query/execute_scalar: tiny results (counts, aggregates,
  signatures, schema metadata) fetched into the driver.
* bulk path — read_dataframe: hash sets / drill-down fetches as Spark DataFrames
  (only for connectors that support it; guarded by NotSupportedError otherwise).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, ClassVar, Optional, Sequence, TYPE_CHECKING

from dvf.core.exceptions import NotSupportedError
from dvf.dialects import SQLDialect, dialect_for_connection_type
from dvf.metadata.models import (
    ColumnDef, ConnectionDef, ConstraintDef, IndexDef, TableStats,
)

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession
    from dvf.utils.secrets import SecretResolver


@dataclass(frozen=True)
class PartitionSpec:
    """Partitioned bulk-read hints for spark.read.jdbc."""
    column: str
    lower_bound: Any
    upper_bound: Any
    num_partitions: int = 8


class BaseConnector(ABC):
    """Common connector contract. Instances are NOT thread-safe; the engine
    creates one connector per (connection_name, worker thread)."""

    #: connection_type values this connector serves (factory registration aid)
    connection_types: ClassVar[tuple[str, ...]] = ()

    def __init__(self, conn: ConnectionDef,
                 secrets: "SecretResolver | None" = None,
                 spark: "SparkSession | None" = None):
        self.conn = conn
        self.secrets = secrets
        self.spark = spark
        self._connected = False

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    @abstractmethod
    def connect(self) -> None: ...

    @abstractmethod
    def disconnect(self) -> None: ...

    @property
    def is_connected(self) -> bool:
        return self._connected

    def __enter__(self) -> "BaseConnector":
        self.connect()
        return self

    def __exit__(self, *exc) -> None:
        self.disconnect()

    # ------------------------------------------------------------------ #
    # Execution
    # ------------------------------------------------------------------ #
    @abstractmethod
    def execute_query(self, sql: str, timeout_s: int | None = None) -> list[tuple]:
        """Run pushdown SQL, return all rows as tuples. Control path only —
        callers must ensure the result set is small (counts/aggregates/samples)."""

    def execute_scalar(self, sql: str, timeout_s: int | None = None) -> Any:
        rows = self.execute_query(sql, timeout_s=timeout_s)
        return rows[0][0] if rows and rows[0] else None

    def execute_non_query(self, sql: str) -> int:
        raise NotSupportedError(
            f"{type(self).__name__} is read-only (validation never mutates sources)",
            connection_name=self.conn.connection_name)

    def read_dataframe(self, sql: str, partition: PartitionSpec | None = None) -> "DataFrame":
        raise NotSupportedError(
            f"{type(self).__name__} has no bulk (DataFrame) path",
            connection_name=self.conn.connection_name)

    # ------------------------------------------------------------------ #
    # Introspection
    # ------------------------------------------------------------------ #
    @abstractmethod
    def get_schema(self, schema: str | None, obj: str) -> list[ColumnDef]: ...

    def get_columns(self, schema: str | None, obj: str) -> list[str]:
        return [c.name for c in self.get_schema(schema, obj)]

    def get_primary_keys(self, schema: str | None, obj: str) -> list[str]:
        return []

    def get_indexes(self, schema: str | None, obj: str) -> list[IndexDef]:
        return []

    def get_constraints(self, schema: str | None, obj: str) -> list[ConstraintDef]:
        return []

    def get_statistics(self, schema: str | None, obj: str) -> TableStats:
        return TableStats()

    @abstractmethod
    def table_exists(self, schema: str | None, obj: str) -> bool: ...

    def list_objects(self, schema: str | None) -> list[str]:
        """Table/view names in a schema (object discovery)."""
        raise NotSupportedError(
            f"{type(self).__name__} does not support object discovery",
            connection_name=self.conn.connection_name)

    def file_exists(self, path: str) -> bool:
        raise NotSupportedError(
            f"{type(self).__name__} is not a file connector",
            connection_name=self.conn.connection_name)

    # ------------------------------------------------------------------ #
    # Object resolution (file connectors map objects to temp views here)
    # ------------------------------------------------------------------ #
    def resolve_object(self, schema: str | None, obj: str | None) -> tuple[str | None, str | None]:
        """(schema, object) as they must appear in generated SQL. File
        connectors read the file on first touch and return its temp-view name."""
        return schema, obj

    # ------------------------------------------------------------------ #
    # Dialect
    # ------------------------------------------------------------------ #
    @property
    def dialect(self) -> SQLDialect:
        return dialect_for_connection_type(self.conn.connection_type)

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    def ping(self) -> bool:
        """Cheap connectivity probe using the dialect's SELECT 1 form."""
        return self.execute_scalar(self.dialect.select_one_sql()) is not None

    def __repr__(self) -> str:  # pragma: no cover
        return f"<{type(self).__name__} {self.conn.connection_name} connected={self._connected}>"
