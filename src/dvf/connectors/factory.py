"""ConnectorFactory — connection_type -> connector class, lazily imported.

Extensibility contract: adding a system = implement BaseConnector (+ dialect)
and call register_connector(); zero engine changes.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING

from dvf.core.exceptions import ConfigurationError
from dvf.metadata.models import ConnectionDef

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession
    from dvf.connectors.base import BaseConnector
    from dvf.utils.secrets import SecretResolver

#: connection_type -> "module:Class" (lazy so optional drivers never block startup)
_CONNECTORS: dict[str, str] = {
    "SQLITE": "dvf.connectors.sqlite:SqliteConnector",
    "DELTA": "dvf.connectors.delta:DeltaConnector",
    "SNOWFLAKE": "dvf.connectors.snowflake:SnowflakeConnector",
    "SQLSERVER": "dvf.connectors.jdbc:SqlServerConnector",
    "ORACLE": "dvf.connectors.jdbc:OracleConnector",
    "POSTGRES": "dvf.connectors.jdbc:PostgresConnector",
    "MYSQL": "dvf.connectors.jdbc:MySqlConnector",
    "DB2_LUW": "dvf.connectors.jdbc:Db2LuwConnector",
    "DB2_ZOS": "dvf.connectors.jdbc:Db2ZosConnector",
    "TERADATA": "dvf.connectors.jdbc:TeradataConnector",
    "FILE_CSV": "dvf.connectors.files:CsvFileConnector",
    "FILE_FIXED": "dvf.connectors.files:FixedWidthFileConnector",
    "FILE_PARQUET": "dvf.connectors.files:ParquetFileConnector",
    "FILE_DELTA": "dvf.connectors.files:DeltaFileConnector",
    "FILE_JSON": "dvf.connectors.files:JsonFileConnector",
    "FILE_XML": "dvf.connectors.files:XmlFileConnector",
    "FILE_EXCEL": "dvf.connectors.files:ExcelFileConnector",
    "FILE_COBOL": "dvf.connectors.files:CobolFileConnector",
}


def register_connector(connection_type: str, target: str) -> None:
    """Extension hook, e.g. register_connector('HANA', 'myext.hana:HanaConnector')."""
    _CONNECTORS[connection_type.upper()] = target


class ConnectorFactory:
    """Creates connector instances bound to a run's SparkSession + SecretResolver."""

    def __init__(self, secrets: "SecretResolver | None" = None,
                 spark: "SparkSession | None" = None):
        self.secrets = secrets
        self.spark = spark

    def create(self, conn: ConnectionDef) -> "BaseConnector":
        ct = (conn.connection_type or "").upper()
        target = _CONNECTORS.get(ct)
        if not target:
            raise ConfigurationError(
                f"No connector registered for connection type '{conn.connection_type}' "
                f"(connection '{conn.connection_name}'). Known: {sorted(_CONNECTORS)}")
        module_name, _, class_name = target.partition(":")
        try:
            cls = getattr(import_module(module_name), class_name)
        except (ImportError, AttributeError) as exc:
            raise ConfigurationError(
                f"Connector class '{target}' for type '{ct}' could not be loaded: {exc}") from exc
        return cls(conn, secrets=self.secrets, spark=self.spark)
