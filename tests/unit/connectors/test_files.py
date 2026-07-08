"""File connectors — Spark-free logic: GDG resolution, fixed-width layout,
factory wiring, path resolution, clean no-Spark errors.
(The Spark read paths are exercised on-cluster by notebook 09.)"""

from __future__ import annotations

import pytest

from dvf.connectors.factory import ConnectorFactory
from dvf.connectors.files import (
    CobolFileConnector, CsvFileConnector, parse_fixed_layout,
    pick_latest_generation,
)
from dvf.core.exceptions import ConfigurationError, ConnectionFailedError
from dvf.metadata.models import ConnectionDef


def _conn(ct: str, base: str = "s3://bucket/landing", **props) -> ConnectionDef:
    return ConnectionDef(connection_name=f"{ct}_TEST", connection_type=ct,
                         database_name=base, extra_properties=props)


def test_gdg_latest_generation():
    names = ["CUST.G0001V00", "CUST.G0012V00", "CUST.G0012V01",
             "CUST.G0002V00", "README.txt"]
    assert pick_latest_generation(names) == "CUST.G0012V01"
    assert pick_latest_generation(["nope", "also.nope"]) is None
    assert pick_latest_generation([]) is None


def test_fixed_layout_parser():
    layout = parse_fixed_layout(
        '[{"name":"id","start":1,"length":6,"type":"int"},'
        ' {"name":"name","start":7,"length":20}]')
    assert layout[0] == {"name": "id", "start": 1, "length": 6, "type": "int"}
    assert layout[1]["type"] == "string"

    with pytest.raises(ConfigurationError):
        parse_fixed_layout("[]")
    with pytest.raises(ConfigurationError):
        parse_fixed_layout('[{"name":"x","start":0,"length":5}]')   # 1-based
    with pytest.raises(ConfigurationError):
        parse_fixed_layout('[{"start":1,"length":5}]')              # no name


def test_factory_creates_every_file_type():
    factory = ConnectorFactory()
    for ct in ("FILE_CSV", "FILE_FIXED", "FILE_PARQUET", "FILE_DELTA",
               "FILE_JSON", "FILE_XML", "FILE_EXCEL", "FILE_COBOL"):
        connector = factory.create(_conn(ct))
        assert connector.conn.connection_type == ct
        assert connector.dialect.name == "spark"   # files speak Spark SQL


def test_path_resolution():
    c = CsvFileConnector(_conn("FILE_CSV", base="s3://bucket/landing"))
    assert c.full_path("daily/customers.csv") == "s3://bucket/landing/daily/customers.csv"
    assert c.full_path("s3://other/abs.csv") == "s3://other/abs.csv"
    assert c.full_path("/dbfs/mnt/x.csv") == "/dbfs/mnt/x.csv"


def test_connect_without_spark_raises_cleanly():
    c = CsvFileConnector(_conn("FILE_CSV"))
    with pytest.raises(ConnectionFailedError, match="SparkSession"):
        c.connect()


def test_cobol_requires_copybook(monkeypatch):
    c = CobolFileConnector(_conn("FILE_COBOL"))
    c.spark = object()          # fake session — _read fails before using it
    c._connected = True
    with pytest.raises(ConfigurationError, match="copybook"):
        c._read("s3://bucket/mainframe/CUST.DATA")
