"""ValidationEngine — sequential run, audit trail, resume-skip, expansion errors."""

from __future__ import annotations

from dvf.audit.models import RunStatus, ValidationStatus
from dvf.core.engine import ValidationEngine
from dvf.metadata.models import RunRequest, ValidationConfig
from fixtures.seeddata import seed_count_mismatch_pair, seed_identical_pair


def _config(vid: str, src: str, tgt: str, **over) -> ValidationConfig:
    base = dict(validation_id=vid, execution_group="G1",
                source_connection=src, source_object="customers",
                target_connection=tgt, target_object="customers",
                validation_profile="QUICK", primary_key="id",
                enabled=True, priority=100, execution_order=100)
    return ValidationConfig(**{**base, **over})


def test_run_mixed_outcomes(settings, backend, repo, register_sqlite_pair):
    register_sqlite_pair("SRC_OK", "TGT_OK", seed_identical_pair, n=10)
    register_sqlite_pair("SRC_BAD", "TGT_BAD", seed_count_mismatch_pair,
                         n=10, missing_in_target=2)
    repo.upsert_validation_configs([
        _config("VAL_OK", "SRC_OK", "TGT_OK", execution_order=10),
        _config("VAL_BAD", "SRC_BAD", "TGT_BAD", execution_order=20),
    ])

    engine = ValidationEngine(settings, backend)
    summary = engine.run(RunRequest(run_name="unit", triggered_by="pytest"))

    assert summary.total == 2
    assert summary.passed == 1 and summary.failed == 1
    assert summary.status == RunStatus.PARTIAL
    assert engine.writer.errors == []

    run_rows = backend.read_rows("audit", "run_audit",
                                 where_eq={"run_id": summary.run_id})
    assert run_rows[0]["status"] == RunStatus.PARTIAL
    assert run_rows[0]["passed"] == 1 and run_rows[0]["failed"] == 1
    assert run_rows[0]["framework_version"]

    audits = {r["validation_id"]: r for r in backend.read_rows(
        "audit", "validation_run_audit", where_eq={"run_id": summary.run_id})}
    assert audits["VAL_OK"]["status"] == ValidationStatus.PASSED
    assert audits["VAL_BAD"]["status"] == ValidationStatus.FAILED
    assert audits["VAL_OK"]["rules_total"] == 5           # QUICK profile
    assert audits["VAL_BAD"]["source_row_count"] == 10
    assert audits["VAL_BAD"]["target_row_count"] == 8
    assert audits["VAL_BAD"]["error_message"].startswith("RECORD_COUNT")

    results = backend.read_rows("results", "result_detail",
                                where_eq={"run_id": summary.run_id,
                                          "validation_id": "VAL_BAD"})
    by_type = {r["rule_type"]: r for r in results}
    assert by_type["RECORD_COUNT"]["status"] == "FAIL"
    assert by_type["RECORD_COUNT"]["source_sql"].startswith("SELECT COUNT(*)")


def test_selection_filters(settings, backend, repo, register_sqlite_pair):
    register_sqlite_pair("S1", "T1", seed_identical_pair, n=5)
    repo.upsert_validation_configs([
        _config("VAL_A", "S1", "T1", execution_group="GA"),
        _config("VAL_B", "S1", "T1", execution_group="GB"),
        _config("VAL_C", "S1", "T1", execution_group="GB", enabled=False),
    ])
    engine = ValidationEngine(settings, backend)

    summary = engine.run(RunRequest(execution_groups=["GB"]))
    assert [v.validation_id for v in summary.validations] == ["VAL_B"]

    summary = engine.run(RunRequest(validation_ids=["VAL_A"]))
    assert [v.validation_id for v in summary.validations] == ["VAL_A"]


def test_resume_skips_passed(settings, backend, repo, register_sqlite_pair):
    register_sqlite_pair("S2", "T2", seed_identical_pair, n=5)
    register_sqlite_pair("S3", "T3", seed_count_mismatch_pair, n=5,
                         missing_in_target=1)
    repo.upsert_validation_configs([
        _config("VAL_P", "S2", "T2"),
        _config("VAL_F", "S3", "T3"),
    ])
    engine = ValidationEngine(settings, backend)
    first = engine.run(RunRequest())
    assert first.passed == 1 and first.failed == 1

    second = engine.run(RunRequest(resume_run_id=first.run_id))
    by_id = {v.validation_id: v for v in second.validations}
    assert by_id["VAL_P"].status == ValidationStatus.SKIPPED
    assert by_id["VAL_F"].status == ValidationStatus.FAILED
    assert second.skipped == 1

    row = backend.read_rows("audit", "run_audit",
                            where_eq={"run_id": second.run_id})[0]
    assert row["parent_run_id"] == first.run_id


def test_unknown_profile_becomes_error_row(settings, backend, repo,
                                           register_sqlite_pair):
    register_sqlite_pair("S4", "T4", seed_identical_pair, n=3)
    repo.upsert_validation_configs([
        _config("VAL_X", "S4", "T4", validation_profile="NOPE")])
    engine = ValidationEngine(settings, backend)
    summary = engine.run(RunRequest())
    assert summary.errors == 1
    assert summary.validations[0].status == ValidationStatus.ERROR
    assert "NOPE" in summary.validations[0].error_message


def test_missing_connection_is_error_not_crash(settings, backend, repo,
                                               register_sqlite_pair):
    register_sqlite_pair("S5", "T5", seed_identical_pair, n=3)
    repo.upsert_validation_configs([
        _config("VAL_Y", "S5", "NO_SUCH_CONN")])
    engine = ValidationEngine(settings, backend)
    summary = engine.run(RunRequest())
    assert summary.errors == 1
    assert "NO_SUCH_CONN" in summary.validations[0].error_message
