"""ObjectDiscovery — proposals from live sqlite schemas."""

from __future__ import annotations

import sqlite3

from dvf.connectors.factory import ConnectorFactory
from dvf.metadata.discovery import ObjectDiscovery
from fixtures.seeddata import seed_identical_pair


def test_discovery_proposes_disabled_configs(settings, backend, repo,
                                             register_sqlite_pair, tmp_path):
    src, tgt = register_sqlite_pair("D_SRC", "D_TGT", seed_identical_pair, n=5)
    # extra source-only table -> must be skipped (missing on target)
    db = sqlite3.connect(src)
    db.execute("CREATE TABLE src_only (id INTEGER PRIMARY KEY)")
    db.commit(), db.close()

    discovery = ObjectDiscovery(repo, ConnectorFactory())
    proposals = discovery.discover(
        source_connection="D_SRC", source_schema=None,
        target_connection="D_TGT", target_schema=None,
        execution_group="DISC")

    assert [p.source_object for p in proposals] == ["customers"]
    p = proposals[0]
    assert p.validation_id == "VAL_DISC_CUSTOMERS"
    assert p.primary_key == "id"                  # detected from PRAGMA
    assert p.enabled is False                     # human review gate

    assert discovery.register(proposals, actor="pytest") == 1
    stored = repo.load_validation_configs(enabled_only=False)
    assert any(c.validation_id == "VAL_DISC_CUSTOMERS" and not c.enabled
               for c in stored)

    # re-discovery skips already-registered ids
    again = discovery.discover(
        source_connection="D_SRC", source_schema=None,
        target_connection="D_TGT", target_schema=None,
        execution_group="DISC")
    assert again == []


def test_discovery_include_filter(settings, backend, repo, register_sqlite_pair):
    register_sqlite_pair("D2_SRC", "D2_TGT", seed_identical_pair, n=3)
    discovery = ObjectDiscovery(repo, ConnectorFactory())
    proposals = discovery.discover(
        source_connection="D2_SRC", source_schema=None,
        target_connection="D2_TGT", target_schema=None,
        execution_group="DISC2", include=lambda n: "nomatch" in n)
    assert proposals == []
