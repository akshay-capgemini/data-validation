"""SnowflakeConnector — control path via snowflake-connector-python,
bulk path via the Spark Snowflake connector.

Connection definition:
* host             = account URL or bare account identifier (xy12345.us-east-1)
* database_name    = database
* extra_properties = {"warehouse": ..., "role": ..., "schema": ..., "account": ...,
                      "query_tag": ...}
* auth via SecretResolver (user/password keys or AWS secret {username,password})
"""

from __future__ import annotations

from typing import Any, ClassVar

from dvf.connectors.base import BaseConnector, PartitionSpec
from dvf.core.exceptions import (
    ConnectionFailedError, QueryExecutionError, QueryTimeoutError,
)
from dvf.metadata.models import ColumnDef, ConstraintDef, TableStats


class SnowflakeConnector(BaseConnector):
    connection_types: ClassVar[tuple[str, ...]] = ("SNOWFLAKE",)

    def __init__(self, conn, secrets=None, spark=None):
        super().__init__(conn, secrets, spark)
        self._sf = None

    # ------------------------------------------------------------------ #
    def _account(self) -> str:
        account = self.conn.prop("account") or self.conn.host or ""
        return account.replace(".snowflakecomputing.com", "").replace("https://", "")

    def connect(self) -> None:
        if self._connected:
            return
        try:
            import snowflake.connector  # lazy: optional dependency
        except ImportError as exc:  # pragma: no cover
            raise ConnectionFailedError(
                "snowflake-connector-python is not installed (pip install 'dvf[snowflake]')",
                connection_name=self.conn.connection_name) from exc
        creds = self.secrets.resolve(self.conn) if self.secrets else None
        params: dict[str, Any] = {
            "account": self._account(),
            "database": self.conn.database_name,
            "warehouse": self.conn.prop("warehouse"),
            "role": self.conn.prop("role"),
            "schema": self.conn.prop("schema"),
            "session_parameters": {"QUERY_TAG": self.conn.prop("query_tag", "dvf")},
        }
        if creds:
            params["user"] = creds.user
            params["password"] = creds.password
        params = {k: v for k, v in params.items() if v is not None}
        try:
            self._sf = snowflake.connector.connect(**params)
        except Exception as exc:
            raise ConnectionFailedError(
                f"Snowflake connect failed: {exc}",
                connection_name=self.conn.connection_name) from exc
        self._connected = True

    def disconnect(self) -> None:
        if self._sf is not None:
            try:
                self._sf.close()
            except Exception:  # pragma: no cover
                pass
            self._sf = None
        self._connected = False

    # ------------------------------------------------------------------ #
    def execute_query(self, sql: str, timeout_s: int | None = None) -> list[tuple]:
        self._ensure()
        cur = self._sf.cursor()
        try:
            cur.execute(sql, timeout=timeout_s)
            return [tuple(r) for r in cur.fetchall()]
        except Exception as exc:
            if "timeout" in str(exc).lower() or type(exc).__name__ == "QueryTimeout":
                raise QueryTimeoutError(
                    f"Snowflake query timed out after {timeout_s}s",
                    connection_name=self.conn.connection_name) from exc
            sqlstate = getattr(exc, "sqlstate", None)
            raise QueryExecutionError(
                f"Snowflake query failed: {exc}\nSQL: {sql}",
                connection_name=self.conn.connection_name,
                sqlstate=sqlstate,
                vendor_code=getattr(exc, "errno", None)) from exc
        finally:
            cur.close()

    def read_dataframe(self, sql: str, partition: PartitionSpec | None = None):
        if self.spark is None:
            from dvf.utils.spark import get_spark
            self.spark = get_spark()
        if self.spark is None:
            raise ConnectionFailedError(
                "Bulk path needs a SparkSession (Spark Snowflake connector)",
                connection_name=self.conn.connection_name)
        creds = self.secrets.resolve(self.conn) if self.secrets else None
        options = {
            "sfUrl": f"{self._account()}.snowflakecomputing.com",
            "sfDatabase": self.conn.database_name or "",
            "sfWarehouse": self.conn.prop("warehouse") or "",
            "sfRole": self.conn.prop("role") or "",
            "sfSchema": self.conn.prop("schema") or "PUBLIC",
        }
        if creds:
            options["sfUser"] = creds.user or ""
            options["sfPassword"] = creds.password or ""
        options = {k: v for k, v in options.items() if v}
        return (self.spark.read.format("snowflake").options(**options)
                .option("query", sql).load())

    # ------------------------------------------------------------------ #
    def get_schema(self, schema: str | None, obj: str) -> list[ColumnDef]:
        db = self.conn.database_name
        prefix = f"{self.dialect.quote(db)}." if db else ""
        sql = (
            "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, "
            "NUMERIC_SCALE, IS_NULLABLE, ORDINAL_POSITION, COLUMN_DEFAULT "
            f"FROM {prefix}INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA = {self.dialect.literal((schema or '').upper())} "
            f"AND TABLE_NAME = {self.dialect.literal(obj.upper())} "
            "ORDER BY ORDINAL_POSITION"
        )
        return [
            ColumnDef(name=r[0], native_type=r[1], length=r[2], precision=r[3], scale=r[4],
                      nullable=(str(r[5]).upper() == "YES"), position=r[6],
                      default_value=str(r[7]) if r[7] is not None else None)
            for r in self.execute_query(sql)
        ]

    def get_primary_keys(self, schema: str | None, obj: str) -> list[str]:
        db = self.conn.database_name
        fqn = ".".join(self.dialect.quote(p.upper()) for p in (db, schema, obj) if p)
        try:
            rows = self.execute_query(f"SHOW PRIMARY KEYS IN TABLE {fqn}")
        except QueryExecutionError:
            return []
        # SHOW PRIMARY KEYS: created_on, database, schema, table, column_name, key_sequence, ...
        keyed = sorted(((r[5], r[4]) for r in rows), key=lambda t: t[0])
        return [name for _, name in keyed]

    def get_constraints(self, schema: str | None, obj: str) -> list[ConstraintDef]:
        pks = self.get_primary_keys(schema, obj)
        return [ConstraintDef(name=f"pk_{obj}", constraint_type="PRIMARY KEY", columns=pks)] if pks else []

    def get_statistics(self, schema: str | None, obj: str) -> TableStats:
        db = self.conn.database_name
        prefix = f"{self.dialect.quote(db)}." if db else ""
        sql = (
            f"SELECT ROW_COUNT, BYTES FROM {prefix}INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = {self.dialect.literal((schema or '').upper())} "
            f"AND TABLE_NAME = {self.dialect.literal(obj.upper())}"
        )
        rows = self.execute_query(sql)
        if rows:
            return TableStats(row_estimate=rows[0][0], size_bytes=rows[0][1])
        return TableStats()

    def table_exists(self, schema: str | None, obj: str) -> bool:
        db = self.conn.database_name
        prefix = f"{self.dialect.quote(db)}." if db else ""
        sql = (
            f"SELECT COUNT(*) FROM {prefix}INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = {self.dialect.literal((schema or '').upper())} "
            f"AND TABLE_NAME = {self.dialect.literal(obj.upper())}"
        )
        return bool(self.execute_scalar(sql))

    def list_objects(self, schema: str | None) -> list[str]:
        db = self.conn.database_name
        prefix = f"{self.dialect.quote(db)}." if db else ""
        sql = (
            f"SELECT TABLE_NAME FROM {prefix}INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = {self.dialect.literal((schema or '').upper())} "
            "ORDER BY TABLE_NAME"
        )
        return [r[0] for r in self.execute_query(sql)]

    def _ensure(self) -> None:
        if not self._connected:
            self.connect()
