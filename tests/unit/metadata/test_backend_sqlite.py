"""SqliteBackend — DDL bootstrap, append/merge/read/delete, JSON round-trips."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from dvf.core.exceptions import MetadataError
from dvf.metadata.ddl import ALL_TABLES, PROFILE_SEEDS
from dvf.metadata.models import ConnectionDef


def test_create_all_is_idempotent_and_seeds_profiles(backend):
    backend.create_all()  # second call must not fail
    rows = backend.read_rows("meta", "validation_profiles")
    assert len(rows) == len(PROFILE_SEEDS)
    quick = backend.read_rows("meta", "validation_profiles",
                              where_eq={"profile_name": "QUICK"})
    assert {r["rule_type"] for r in quick} == {
        "CONNECTION", "OBJECT_EXISTS", "SCHEMA_COMPARE", "COLUMN_COUNT", "RECORD_COUNT"}


def test_all_tables_exist(backend):
    for t in ALL_TABLES:
        rows = backend.read_rows(t.schema, t.name)
        if (t.schema, t.name) == ("meta", "validation_profiles"):
            assert len(rows) == len(PROFILE_SEEDS)   # seeded at bootstrap
        else:
            assert rows == []


def test_json_and_bool_and_ts_round_trip(backend):
    ts = datetime(2026, 7, 8, 12, 0, 0, tzinfo=timezone.utc)
    backend.merge_rows("meta", "connections", [{
        "connection_name": "C1", "connection_type": "SQLITE",
        "extra_properties": {"a": "1", "b": "two"},
        "enabled": True, "created_ts": ts,
    }])
    row = backend.read_rows("meta", "connections",
                            where_eq={"connection_name": "C1"})[0]
    conn = ConnectionDef.from_row(row)
    assert conn.extra_properties == {"a": "1", "b": "two"}
    assert conn.enabled is True
    assert conn.created_ts is not None and conn.created_ts.year == 2026


def test_merge_partial_update_keeps_other_columns(backend):
    backend.merge_rows("audit", "run_audit", [{
        "run_id": "R1", "status": "RUNNING", "trigger_type": "MANUAL",
        "total_validations": 5,
    }])
    backend.merge_rows("audit", "run_audit", [{"run_id": "R1", "status": "SUCCEEDED"}])
    row = backend.read_rows("audit", "run_audit", where_eq={"run_id": "R1"})[0]
    assert row["status"] == "SUCCEEDED"
    assert row["total_validations"] == 5          # untouched by partial merge
    assert row["trigger_type"] == "MANUAL"


def test_merge_without_key_raises(backend):
    with pytest.raises(MetadataError):
        backend.merge_rows("audit", "run_audit", [{"status": "RUNNING"}])


def test_where_in_and_delete(backend):
    backend.append_rows("audit", "run_audit", [
        {"run_id": f"R{i}", "status": "SUCCEEDED"} for i in range(4)])
    rows = backend.read_rows("audit", "run_audit",
                             where_in={"run_id": ["R1", "R3"]})
    assert {r["run_id"] for r in rows} == {"R1", "R3"}
    assert backend.read_rows("audit", "run_audit", where_in={"run_id": []}) == []
    backend.delete_rows("audit", "run_audit", where_eq={"run_id": "R1"})
    assert backend.read_rows("audit", "run_audit",
                             where_eq={"run_id": "R1"}) == []


def test_execution_history_view_exists(backend):
    view = backend.table_name("audit", "v_execution_history")
    assert backend.query(f'SELECT * FROM "{view}"') == []
