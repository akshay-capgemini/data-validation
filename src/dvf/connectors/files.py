"""File connectors — CSV/TXT, fixed-width, Parquet, Delta paths, JSON, XML,
Excel, and COBOL copybook files (Cobrix), including mainframe sequential/VSAM
dumps and GDG resolution.

Pattern: the file is read ONCE by Spark into a temp view; every interface
method then behaves like a database — "pushdown" = Spark SQL against the view
(SparkDialect). Object resolution:

* connection.database_name = base path (s3://bucket/landing, /Volumes/..., dbfs:/)
* validation source_object = file/dir path relative to the base (or absolute)
* validation source_schema is unused for files

Connection extra_properties (per type):
* CSV   : delimiter, header, encoding, quote, escape, multiline, infer_schema, schema_ddl
* FIXED : layout (JSON array of {name,start,length,type}), encoding, trim='true'
* JSON  : multiline;  XML: row_tag (spark-xml cluster lib);  EXCEL: sheet, header
* COBOL : copybook (path) or copybook_contents, record_format F|V|VB,
          is_record_sequence (VSAM dumps), encoding (default EBCDIC cp037),
          gdg ('latest' resolves the newest GxxxxVyy generation),
          cobrix.* passthrough options
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, ClassVar

from dvf.connectors.base import BaseConnector, PartitionSpec
from dvf.core.exceptions import (
    ConfigurationError, ConnectionFailedError, QueryExecutionError,
)
from dvf.metadata.models import ColumnDef

_GDG_PATTERN = re.compile(r"G(\d{4})V(\d{2})", re.IGNORECASE)


def pick_latest_generation(names: list[str]) -> str | None:
    """Highest GxxxxVyy generation from a list of file/dir names (GDG)."""
    best: tuple[int, int] | None = None
    best_name: str | None = None
    for name in names:
        m = _GDG_PATTERN.search(name)
        if not m:
            continue
        gen = (int(m.group(1)), int(m.group(2)))
        if best is None or gen > best:
            best, best_name = gen, name
    return best_name


def parse_fixed_layout(raw: Any) -> list[dict]:
    """Validate a fixed-width layout spec: [{name,start,length,type?}, ...]
    (start is 1-based, inclusive)."""
    layout = json.loads(raw) if isinstance(raw, str) else raw
    if not isinstance(layout, list) or not layout:
        raise ConfigurationError("fixed-width 'layout' must be a non-empty JSON array")
    fields = []
    for i, f in enumerate(layout):
        try:
            fields.append({"name": str(f["name"]),
                           "start": int(f["start"]),
                           "length": int(f["length"]),
                           "type": str(f.get("type", "string")).lower()})
        except (KeyError, TypeError, ValueError) as exc:
            raise ConfigurationError(
                f"fixed-width layout entry {i} invalid ({f!r}): needs name/start/length") from exc
        if fields[-1]["start"] < 1 or fields[-1]["length"] < 1:
            raise ConfigurationError(
                f"fixed-width layout entry {i}: start/length must be >= 1")
    return fields


class FileConnectorBase(BaseConnector):
    """Temp-view mechanics shared by every file connector."""

    format_label: ClassVar[str] = "file"

    def __init__(self, conn, secrets=None, spark=None):
        super().__init__(conn, secrets, spark)
        self._views: dict[str, str] = {}          # resolved path -> view name

    # ------------------------------------------------------------------ #
    def connect(self) -> None:
        if self.spark is None:
            from dvf.utils.spark import get_spark
            self.spark = get_spark()
        if self.spark is None:
            raise ConnectionFailedError(
                f"File connector '{self.conn.connection_name}' requires a "
                "SparkSession (files are read via Spark)",
                connection_name=self.conn.connection_name)
        self._connected = True

    def disconnect(self) -> None:
        for view in self._views.values():
            try:
                self.spark.catalog.dropTempView(view)
            except Exception:  # pragma: no cover — best-effort cleanup
                pass
        self._views.clear()
        self._connected = False

    # ------------------------------------------------------------------ #
    def full_path(self, obj: str) -> str:
        if obj.startswith(("s3://", "s3a://", "dbfs:/", "abfss://", "file:/", "/")):
            path = obj
        else:
            base = (self.conn.database_name or "").rstrip("/")
            path = f"{base}/{obj}" if base else obj
        if str(self.conn.prop("gdg", "")).lower() == "latest":
            path = self._resolve_gdg(path)
        return path

    def _resolve_gdg(self, path: str) -> str:
        from dvf.utils.spark import get_dbutils
        dbutils = get_dbutils(self.spark)
        if dbutils is None:
            raise ConnectionFailedError(
                "GDG resolution needs dbutils (Databricks runtime)",
                connection_name=self.conn.connection_name)
        entries = [e.name.rstrip("/") for e in dbutils.fs.ls(path)]
        latest = pick_latest_generation(entries)
        if latest is None:
            raise QueryExecutionError(
                f"No GxxxxVyy generations found under {path}",
                connection_name=self.conn.connection_name)
        return f"{path.rstrip('/')}/{latest}"

    def resolve_object(self, schema, obj):
        """Read the file on first touch; SQL then targets the temp view."""
        return None, self._ensure_view(obj)

    def _ensure_view(self, obj: str) -> str:
        self._ensure()
        path = self.full_path(obj)
        view = self._views.get(path)
        if view is None:
            view = f"dvf_file_{hashlib.md5(path.encode()).hexdigest()[:12]}"
            try:
                df = self._read(path)
            except Exception as exc:
                raise QueryExecutionError(
                    f"Failed to read {self.format_label} '{path}': {exc}",
                    connection_name=self.conn.connection_name) from exc
            df.createOrReplaceTempView(view)
            self._views[path] = view
        return view

    def _read(self, path: str):
        raise NotImplementedError

    # ------------------------------------------------------------------ #
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
        return self.spark.sql(sql)

    def get_schema(self, schema: str | None, obj: str) -> list[ColumnDef]:
        view = self._ensure_view(obj)
        df = self.spark.table(view)
        return [ColumnDef(name=f.name, native_type=f.dataType.simpleString().upper(),
                          nullable=f.nullable, position=i + 1)
                for i, f in enumerate(df.schema.fields)]

    def table_exists(self, schema: str | None, obj: str) -> bool:
        try:
            self._ensure_view(obj)
            return True
        except QueryExecutionError:
            return False

    def file_exists(self, path: str) -> bool:
        return self.table_exists(None, path)

    def _ensure(self) -> None:
        if not self._connected:
            self.connect()

    def _bool_prop(self, key: str, default: str = "true") -> bool:
        return str(self.conn.prop(key, default)).lower() == "true"


# =========================================================================== #
class CsvFileConnector(FileConnectorBase):
    connection_types = ("FILE_CSV",)
    format_label = "CSV"

    def _read(self, path: str):
        reader = (self.spark.read.format("csv")
                  .option("sep", self.conn.prop("delimiter", ","))
                  .option("header", self.conn.prop("header", "true"))
                  .option("encoding", self.conn.prop("encoding", "UTF-8"))
                  .option("quote", self.conn.prop("quote", '"'))
                  .option("escape", self.conn.prop("escape", "\\"))
                  .option("multiLine", self.conn.prop("multiline", "false"))
                  .option("mode", "PERMISSIVE"))
        schema_ddl = self.conn.prop("schema_ddl")
        if schema_ddl:
            reader = reader.schema(schema_ddl)
        elif self._bool_prop("infer_schema"):
            reader = reader.option("inferSchema", "true")
        return reader.load(path)


class FixedWidthFileConnector(FileConnectorBase):
    connection_types = ("FILE_FIXED",)
    format_label = "fixed-width file"

    _TYPE_CAST = {"int": "BIGINT", "bigint": "BIGINT", "decimal": "DECIMAL(38,6)",
                  "double": "DOUBLE", "date": "DATE", "timestamp": "TIMESTAMP",
                  "string": None}

    def _read(self, path: str):
        from pyspark.sql import functions as F
        layout = parse_fixed_layout(self.conn.prop("layout") or "[]")
        trim = self._bool_prop("trim")
        df = (self.spark.read.format("text")
              .option("encoding", self.conn.prop("encoding", "UTF-8"))
              .load(path))
        cols = []
        for f in layout:
            col = F.substring(F.col("value"), f["start"], f["length"])
            if trim:
                col = F.trim(col)
            cast = self._TYPE_CAST.get(f["type"], None)
            if cast:
                col = col.cast(cast)
            cols.append(col.alias(f["name"]))
        return df.select(*cols)


class ParquetFileConnector(FileConnectorBase):
    connection_types = ("FILE_PARQUET",)
    format_label = "Parquet"

    def _read(self, path: str):
        return self.spark.read.parquet(path)


class DeltaFileConnector(FileConnectorBase):
    connection_types = ("FILE_DELTA",)
    format_label = "Delta path"

    def _read(self, path: str):
        return self.spark.read.format("delta").load(path)


class JsonFileConnector(FileConnectorBase):
    connection_types = ("FILE_JSON",)
    format_label = "JSON"

    def _read(self, path: str):
        return (self.spark.read.format("json")
                .option("multiLine", self.conn.prop("multiline", "false"))
                .load(path))


class XmlFileConnector(FileConnectorBase):
    connection_types = ("FILE_XML",)
    format_label = "XML"

    def _read(self, path: str):
        row_tag = self.conn.prop("row_tag")
        if not row_tag:
            raise ConfigurationError(
                f"FILE_XML connection '{self.conn.connection_name}' needs "
                "extra_properties['row_tag'] (spark-xml library on the cluster)")
        return (self.spark.read.format("xml")
                .option("rowTag", row_tag).load(path))


class ExcelFileConnector(FileConnectorBase):
    connection_types = ("FILE_EXCEL",)
    format_label = "Excel"

    def _read(self, path: str):
        sheet = self.conn.prop("sheet", "0")
        data_address = self.conn.prop("data_address", f"'{sheet}'!A1") \
            if not str(sheet).isdigit() else self.conn.prop("data_address", f"{sheet}!A1")
        return (self.spark.read.format("excel")          # com.crealytics cluster lib
                .option("header", self.conn.prop("header", "true"))
                .option("dataAddress", data_address)
                .option("inferSchema", self.conn.prop("infer_schema", "true"))
                .load(path))


class CobolFileConnector(FileConnectorBase):
    """COBOL copybook files via Cobrix (za.co.absa.cobrix:spark-cobol maven lib):
    mainframe sequential files, VSAM dumps, fixed/variable records, EBCDIC."""

    connection_types = ("FILE_COBOL",)
    format_label = "COBOL/copybook file"

    def _read(self, path: str):
        copybook = self.conn.prop("copybook")
        copybook_contents = self.conn.prop("copybook_contents")
        if not copybook and not copybook_contents:
            raise ConfigurationError(
                f"FILE_COBOL connection '{self.conn.connection_name}' needs "
                "extra_properties['copybook'] (path) or ['copybook_contents']")
        reader = self.spark.read.format("cobol")
        if copybook:
            reader = reader.option("copybook", copybook)
        else:
            reader = reader.option("copybook_contents", copybook_contents)
        record_format = self.conn.prop("record_format", "F")
        reader = (reader
                  .option("record_format", record_format)
                  .option("encoding", self.conn.prop("encoding", "ebcdic"))
                  .option("ebcdic_code_page", self.conn.prop("code_page", "cp037"))
                  .option("schema_retention_policy", "collapse_root"))
        if self._bool_prop("is_record_sequence", "false"):
            reader = reader.option("is_record_sequence", "true")
        for key, value in self.conn.extra_properties.items():
            if key.startswith("cobrix."):
                reader = reader.option(key[len("cobrix."):], str(value))
        return reader.load(path)
