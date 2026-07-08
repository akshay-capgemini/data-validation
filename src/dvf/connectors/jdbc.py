"""JDBC connector family — SQL Server, Oracle, PostgreSQL, MySQL, DB2 (LUW &
z/OS), Teradata.

Two data paths:
* control path — java.sql.DriverManager via the cluster JVM (py4j), reusing the
  JDBC driver jars already installed on the cluster. Zero extra pip deps. Only
  tiny result sets (counts/aggregates/signatures/samples) ever cross py4j.
* bulk path   — spark.read.jdbc for hash sets and drill-down fetches, with
  optional key-range partitioning.

Introspection uses JDBC DatabaseMetaData — one implementation for all engines.
"""

from __future__ import annotations

from typing import Any, ClassVar

from dvf.connectors.base import BaseConnector, PartitionSpec
from dvf.core.exceptions import (
    ConnectionFailedError, QueryExecutionError, QueryTimeoutError,
)
from dvf.metadata.models import ColumnDef, ConstraintDef, IndexDef


class JdbcConnector(BaseConnector):
    driver_class: ClassVar[str] = ""
    default_port: ClassVar[int] = 0
    #: how this engine stores unquoted identifiers in its catalog
    metadata_case: ClassVar[str] = "preserve"     # preserve | upper | lower

    def __init__(self, conn, secrets=None, spark=None):
        super().__init__(conn, secrets, spark)
        self._jconn = None
        self._jvm = None

    # ------------------------------------------------------------------ #
    # URL / properties (per engine)
    # ------------------------------------------------------------------ #
    def jdbc_url(self) -> str:
        raise NotImplementedError

    def jdbc_properties(self) -> dict[str, str]:
        """Driver properties; credentials resolved at call time, never stored."""
        props: dict[str, str] = {}
        creds = self.secrets.resolve(self.conn) if self.secrets else None
        if creds and creds.user:
            props["user"] = creds.user
        if creds and creds.password:
            props["password"] = creds.password
        for key, value in self.conn.extra_properties.items():
            if key.startswith("jdbc."):
                props[key[len("jdbc."):]] = str(value)
        return props

    # ------------------------------------------------------------------ #
    # Lifecycle (control path)
    # ------------------------------------------------------------------ #
    def connect(self) -> None:
        if self._connected:
            return
        if self.spark is None:
            from dvf.utils.spark import get_spark
            self.spark = get_spark()
        if self.spark is None:
            raise ConnectionFailedError(
                f"JDBC control path needs the cluster JVM (SparkSession) for "
                f"connection '{self.conn.connection_name}' — run on Databricks "
                f"or provide a SparkSession",
                connection_name=self.conn.connection_name)
        self._jvm = self.spark._jvm  # noqa: SLF001 — sanctioned py4j access
        try:
            self._jvm.java.lang.Class.forName(self.driver_class)
            props = self._jvm.java.util.Properties()
            for key, value in self.jdbc_properties().items():
                props.setProperty(key, value)
            self._jconn = self._jvm.java.sql.DriverManager.getConnection(
                self.jdbc_url(), props)
        except Exception as exc:
            raise ConnectionFailedError(
                f"JDBC connect failed for '{self.conn.connection_name}' "
                f"({self.jdbc_url()}): {_java_msg(exc)}",
                connection_name=self.conn.connection_name) from exc
        self._connected = True

    def disconnect(self) -> None:
        if self._jconn is not None:
            try:
                self._jconn.close()
            except Exception:  # pragma: no cover — best-effort close
                pass
            self._jconn = None
        self._connected = False

    # ------------------------------------------------------------------ #
    # Execution
    # ------------------------------------------------------------------ #
    def execute_query(self, sql: str, timeout_s: int | None = None) -> list[tuple]:
        self._ensure()
        stmt = None
        try:
            stmt = self._jconn.createStatement()
            if timeout_s:
                stmt.setQueryTimeout(int(timeout_s))
            rs = stmt.executeQuery(sql)
            meta = rs.getMetaData()
            n = meta.getColumnCount()
            rows: list[tuple] = []
            while rs.next():
                row = []
                for i in range(1, n + 1):
                    row.append(self._convert(rs.getObject(i)))
                rows.append(tuple(row))
            rs.close()
            return rows
        except Exception as exc:
            raise self._classify(exc, sql) from exc
        finally:
            if stmt is not None:
                try:
                    stmt.close()
                except Exception:  # pragma: no cover
                    pass

    def read_dataframe(self, sql: str, partition: PartitionSpec | None = None):
        if self.spark is None:
            from dvf.utils.spark import get_spark
            self.spark = get_spark()
        if self.spark is None:
            raise ConnectionFailedError(
                "Bulk path needs a SparkSession",
                connection_name=self.conn.connection_name)
        reader = (self.spark.read.format("jdbc")
                  .option("url", self.jdbc_url())
                  .option("driver", self.driver_class)
                  .option("dbtable", f"({sql}) dvf_q"))
        for key, value in self.jdbc_properties().items():
            reader = reader.option(key, value)
        if partition is not None:
            reader = (reader
                      .option("partitionColumn", partition.column)
                      .option("lowerBound", str(partition.lower_bound))
                      .option("upperBound", str(partition.upper_bound))
                      .option("numPartitions", str(partition.num_partitions)))
        return reader.load()

    # ------------------------------------------------------------------ #
    # Introspection via DatabaseMetaData (uniform across engines)
    # ------------------------------------------------------------------ #
    def _meta_name(self, name: str | None) -> str | None:
        if name is None:
            return None
        if self.metadata_case == "upper":
            return name.upper()
        if self.metadata_case == "lower":
            return name.lower()
        return name

    def _catalog(self) -> str | None:
        return None                                  # engines override if needed

    def get_schema(self, schema: str | None, obj: str) -> list[ColumnDef]:
        self._ensure()
        meta = self._jconn.getMetaData()
        rs = meta.getColumns(self._catalog(), self._meta_name(schema),
                             self._meta_name(obj), None)
        cols: list[ColumnDef] = []
        while rs.next():
            size = rs.getInt("COLUMN_SIZE")
            digits = rs.getInt("DECIMAL_DIGITS")
            digits = None if rs.wasNull() else digits
            type_name = rs.getString("TYPE_NAME") or ""
            cols.append(ColumnDef(
                name=rs.getString("COLUMN_NAME"),
                native_type=type_name,
                length=size if self.dialect.type_category(type_name).value == "STRING"
                else None,
                precision=size if digits is not None else None,
                scale=digits,
                nullable=rs.getInt("NULLABLE") != 0,
                position=rs.getInt("ORDINAL_POSITION"),
                default_value=rs.getString("COLUMN_DEF")))
        rs.close()
        cols.sort(key=lambda c: c.position or 0)
        return cols

    def get_primary_keys(self, schema: str | None, obj: str) -> list[str]:
        self._ensure()
        meta = self._jconn.getMetaData()
        rs = meta.getPrimaryKeys(self._catalog(), self._meta_name(schema),
                                 self._meta_name(obj))
        keyed = []
        while rs.next():
            keyed.append((rs.getInt("KEY_SEQ"), rs.getString("COLUMN_NAME")))
        rs.close()
        return [name for _, name in sorted(keyed)]

    def get_indexes(self, schema: str | None, obj: str) -> list[IndexDef]:
        self._ensure()
        meta = self._jconn.getMetaData()
        rs = meta.getIndexInfo(self._catalog(), self._meta_name(schema),
                               self._meta_name(obj), False, True)
        by_name: dict[str, dict] = {}
        while rs.next():
            name = rs.getString("INDEX_NAME")
            col = rs.getString("COLUMN_NAME")
            if not name or not col:
                continue
            entry = by_name.setdefault(name, {"columns": [],
                                              "unique": not rs.getBoolean("NON_UNIQUE")})
            entry["columns"].append((rs.getInt("ORDINAL_POSITION"), col))
        rs.close()
        return [IndexDef(name=n,
                         columns=[c for _, c in sorted(e["columns"])],
                         unique=e["unique"])
                for n, e in by_name.items()]

    def get_constraints(self, schema: str | None, obj: str) -> list[ConstraintDef]:
        pks = self.get_primary_keys(schema, obj)
        return [ConstraintDef(name=f"pk_{obj}", constraint_type="PRIMARY KEY",
                              columns=pks)] if pks else []

    def table_exists(self, schema: str | None, obj: str) -> bool:
        self._ensure()
        meta = self._jconn.getMetaData()
        rs = meta.getTables(self._catalog(), self._meta_name(schema),
                            self._meta_name(obj),
                            None)
        found = rs.next()
        rs.close()
        return bool(found)

    def list_objects(self, schema: str | None) -> list[str]:
        self._ensure()
        meta = self._jconn.getMetaData()
        rs = meta.getTables(self._catalog(), self._meta_name(schema), "%", None)
        names = []
        while rs.next():
            if (rs.getString("TABLE_TYPE") or "").upper() in ("TABLE", "VIEW"):
                names.append(rs.getString("TABLE_NAME"))
        rs.close()
        return sorted(names)

    # ------------------------------------------------------------------ #
    def _ensure(self) -> None:
        if not self._connected:
            self.connect()

    @staticmethod
    def _convert(value: Any) -> Any:
        """py4j auto-converts primitives; BigDecimal/Timestamp/etc. -> str."""
        if value is None or isinstance(value, (int, float, str, bool, bytes)):
            return value
        return str(value)

    def _classify(self, exc: Exception, sql: str) -> QueryExecutionError:
        message = _java_msg(exc)
        sqlstate = None
        vendor = None
        java_exc = getattr(exc, "java_exception", None)
        if java_exc is not None:
            try:
                sqlstate = java_exc.getSQLState()
                vendor = java_exc.getErrorCode()
            except Exception:  # pragma: no cover
                pass
        if "timeout" in message.lower() or (sqlstate or "").upper() in ("HYT00", "HYT01"):
            return QueryTimeoutError(
                f"JDBC query timed out: {message}",
                connection_name=self.conn.connection_name, sqlstate=sqlstate)
        return QueryExecutionError(
            f"JDBC query failed: {message}\nSQL: {sql}",
            connection_name=self.conn.connection_name,
            sqlstate=sqlstate, vendor_code=vendor)


def _java_msg(exc: Exception) -> str:
    java_exc = getattr(exc, "java_exception", None)
    if java_exc is not None:
        try:
            return java_exc.getMessage() or str(exc)
        except Exception:  # pragma: no cover
            return str(exc)
    return str(exc)


# =========================================================================== #
# Engine-specific subclasses
# =========================================================================== #
class SqlServerConnector(JdbcConnector):
    connection_types = ("SQLSERVER",)
    driver_class = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    default_port = 1433

    def jdbc_url(self) -> str:
        port = self.conn.port or self.default_port
        encrypt = self.conn.prop("encrypt", "true")
        trust = self.conn.prop("trustServerCertificate", "true")
        return (f"jdbc:sqlserver://{self.conn.host}:{port};"
                f"databaseName={self.conn.database_name};"
                f"encrypt={encrypt};trustServerCertificate={trust}")


class OracleConnector(JdbcConnector):
    connection_types = ("ORACLE",)
    driver_class = "oracle.jdbc.OracleDriver"
    default_port = 1521
    metadata_case = "upper"

    def jdbc_url(self) -> str:
        port = self.conn.port or self.default_port
        service = self.conn.prop("service_name") or self.conn.database_name
        return f"jdbc:oracle:thin:@//{self.conn.host}:{port}/{service}"


class PostgresConnector(JdbcConnector):
    connection_types = ("POSTGRES",)
    driver_class = "org.postgresql.Driver"
    default_port = 5432
    metadata_case = "lower"

    def jdbc_url(self) -> str:
        port = self.conn.port or self.default_port
        return f"jdbc:postgresql://{self.conn.host}:{port}/{self.conn.database_name}"


class MySqlConnector(JdbcConnector):
    connection_types = ("MYSQL",)
    driver_class = "com.mysql.cj.jdbc.Driver"
    default_port = 3306

    def jdbc_url(self) -> str:
        port = self.conn.port or self.default_port
        return (f"jdbc:mysql://{self.conn.host}:{port}/{self.conn.database_name}"
                f"?useSSL={self.conn.prop('useSSL', 'true')}")

    def _catalog(self) -> str | None:
        return self.conn.database_name               # MySQL: catalog == database


class Db2LuwConnector(JdbcConnector):
    connection_types = ("DB2_LUW",)
    driver_class = "com.ibm.db2.jcc.DB2Driver"
    default_port = 50000
    metadata_case = "upper"

    def jdbc_url(self) -> str:
        port = self.conn.port or self.default_port
        return f"jdbc:db2://{self.conn.host}:{port}/{self.conn.database_name}"


class Db2ZosConnector(Db2LuwConnector):
    connection_types = ("DB2_ZOS",)
    default_port = 446

    def jdbc_properties(self) -> dict[str, str]:
        props = super().jdbc_properties()
        # type 4 z/OS connectivity; location name is the database_name
        props.setdefault("retrieveMessagesFromServerOnGetMessage", "true")
        return props


class TeradataConnector(JdbcConnector):
    connection_types = ("TERADATA",)
    driver_class = "com.teradata.jdbc.TeraDriver"
    default_port = 1025
    metadata_case = "upper"

    def jdbc_url(self) -> str:
        parts = [f"jdbc:teradata://{self.conn.host}/"]
        opts = [f"DATABASE={self.conn.database_name}"] if self.conn.database_name else []
        opts.append(f"DBS_PORT={self.conn.port or self.default_port}")
        tmode = self.conn.prop("tmode")
        if tmode:
            opts.append(f"TMODE={tmode}")
        return parts[0] + ",".join(opts)
