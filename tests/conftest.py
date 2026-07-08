"""Shared fixtures — sqlite metadata backend + seeded source/target databases."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest


def pytest_addoption(parser):
    parser.addoption("--update-golden", action="store_true", default=False,
                     help="regenerate golden SQL files instead of comparing")

sys.path.insert(0, str(Path(__file__).parent))  # expose tests/fixtures as a package path

from dvf.config.settings import FrameworkSettings
from dvf.metadata.backend import SqliteBackend
from dvf.metadata.models import AuthMethods, ConnectionDef, ConnectionTypes
from dvf.metadata.repository import MetadataRepository


@pytest.fixture
def settings(tmp_path) -> FrameworkSettings:
    s = FrameworkSettings()
    s.reporting.output_path = str(tmp_path / "reports")
    s.reporting.formats = ["html"]
    s.audit.flush_interval_s = 0.2
    s.logging.json_format = False
    return s


@pytest.fixture
def backend(tmp_path):
    b = SqliteBackend(str(tmp_path / "dvf_framework.db"))
    b.create_all()
    yield b
    b.close()


@pytest.fixture
def repo(backend) -> MetadataRepository:
    return MetadataRepository(backend)


def sqlite_connection(name: str, db_path: str) -> ConnectionDef:
    return ConnectionDef(
        connection_name=name,
        connection_type=ConnectionTypes.SQLITE,
        database_name=db_path,
        auth_method=AuthMethods.NONE,
        enabled=True,
    )


@pytest.fixture
def register_sqlite_pair(repo, tmp_path):
    """Factory: seed a source/target sqlite pair and register connections.

    Usage: src, tgt = register_sqlite_pair('SRC', 'TGT', seeder_fn)
    """
    def _register(src_name: str, tgt_name: str, seeder, **kwargs):
        src_path = str(tmp_path / f"{src_name.lower()}.db")
        tgt_path = str(tmp_path / f"{tgt_name.lower()}.db")
        seeder(src_path, tgt_path, **kwargs)
        repo.upsert_connections([
            sqlite_connection(src_name, src_path),
            sqlite_connection(tgt_name, tgt_path),
        ], actor="pytest")
        return src_path, tgt_path
    return _register
