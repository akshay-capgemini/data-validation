"""Transient retry + RerunManager."""

from __future__ import annotations

from dvf.audit.models import RunStatus, ValidationStatus
from dvf.connectors.factory import register_connector
from dvf.core.engine import ValidationEngine
from dvf.core.rerun import RerunManager
from dvf.metadata.models import ConnectionDef, RunRequest, ValidationConfig
from fixtures.harness import FlakySqliteConnector
from fixtures.seeddata import seed_count_mismatch_pair, seed_identical_pair

from dvf.dialects import map_connection_type

register_connector("FLAKY_SQLITE", "fixtures.harness:FlakySqliteConnector")
map_connection_type("FLAKY_SQLITE", "sqlite")


def _config(vid: str, src: str, tgt: str, **over) -> ValidationConfig:
    base = dict(validation_id=vid, source_connection=src, source_object="customers",
                target_connection=tgt, target_object="customers",
                validation_profile="QUICK", primary_key="id")
    return ValidationConfig(**{**base, **over})


def test_transient_failure_retried_to_success(settings, backend, repo, tmp_path):
    src, tgt = str(tmp_path / "fs.db"), str(tmp_path / "ft.db")
    seed_identical_pair(src, tgt, n=5)
    repo.upsert_connections([
        ConnectionDef(connection_name="FLAKY_SRC", connection_type="FLAKY_SQLITE",
                      database_name=src),
        ConnectionDef(connection_name="TGT_N", connection_type="SQLITE",
                      database_name=tgt)])
    repo.upsert_validation_configs([_config("VAL_R", "FLAKY_SRC", "TGT_N")])

    FlakySqliteConnector.reset(fail_times=1)      # first connect() fails, then OK
    settings.engine.retry_backoff_s = 0
    engine = ValidationEngine(settings, backend)
    summary = engine.run(RunRequest())

    assert summary.passed == 1 and summary.status == RunStatus.SUCCEEDED
    attempts = backend.read_rows("audit", "validation_run_audit",
                                 where_eq={"run_id": summary.run_id})
    by_attempt = {a["attempt"]: a["status"] for a in attempts}
    assert by_attempt == {1: ValidationStatus.ERROR, 2: ValidationStatus.PASSED}


def test_nontransient_failure_not_retried(settings, backend, repo,
                                          register_sqlite_pair):
    register_sqlite_pair("S_NR", "T_NR", seed_count_mismatch_pair,
                         n=6, missing_in_target=2)
    repo.upsert_validation_configs([_config("VAL_NR", "S_NR", "T_NR")])
    settings.engine.retry_backoff_s = 0
    engine = ValidationEngine(settings, backend)
    summary = engine.run(RunRequest())
    attempts = backend.read_rows("audit", "validation_run_audit",
                                 where_eq={"run_id": summary.run_id})
    assert len(attempts) == 1                     # FAILED (data) => no retry
    assert attempts[0]["status"] == ValidationStatus.FAILED


def test_retries_exhausted_stays_error(settings, backend, repo, tmp_path):
    src, tgt = str(tmp_path / "fx.db"), str(tmp_path / "fy.db")
    seed_identical_pair(src, tgt, n=3)
    repo.upsert_connections([
        ConnectionDef(connection_name="FLAKY_SRC2", connection_type="FLAKY_SQLITE",
                      database_name=src),
        ConnectionDef(connection_name="TGT_N2", connection_type="SQLITE",
                      database_name=tgt)])
    repo.upsert_validation_configs([_config("VAL_RX", "FLAKY_SRC2", "TGT_N2")])

    FlakySqliteConnector.reset(fail_times=99)     # never recovers
    settings.engine.retry_backoff_s = 0
    settings.engine.retry_max = 2
    engine = ValidationEngine(settings, backend)
    summary = engine.run(RunRequest())
    assert summary.errors == 1
    attempts = backend.read_rows("audit", "validation_run_audit",
                                 where_eq={"run_id": summary.run_id})
    assert len(attempts) == 3                     # initial + 2 retries


def test_rerun_failed_only_reruns_failures(settings, backend, repo,
                                           register_sqlite_pair):
    register_sqlite_pair("S_P", "T_P", seed_identical_pair, n=5)
    register_sqlite_pair("S_F", "T_F", seed_count_mismatch_pair,
                         n=5, missing_in_target=1)
    repo.upsert_validation_configs([
        _config("VAL_PASS", "S_P", "T_P"),
        _config("VAL_FAIL", "S_F", "T_F")])
    engine = ValidationEngine(settings, backend)
    first = engine.run(RunRequest())
    assert first.passed == 1 and first.failed == 1

    manager = RerunManager(engine)
    assert manager.failed_validation_ids(first.run_id) == ["VAL_FAIL"]

    second = manager.rerun_failed(first.run_id, triggered_by="pytest")
    assert second is not None
    assert [v.validation_id for v in second.validations] == ["VAL_FAIL"]
    run_row = backend.read_rows("audit", "run_audit",
                                where_eq={"run_id": second.run_id})[0]
    assert run_row["trigger_type"] == "RERUN"
    assert run_row["parent_run_id"] == first.run_id


def test_rerun_failed_nothing_to_do(settings, backend, repo, register_sqlite_pair):
    register_sqlite_pair("S_P2", "T_P2", seed_identical_pair, n=4)
    repo.upsert_validation_configs([_config("VAL_OKONLY", "S_P2", "T_P2")])
    engine = ValidationEngine(settings, backend)
    first = engine.run(RunRequest())
    assert RerunManager(engine).rerun_failed(first.run_id) is None
