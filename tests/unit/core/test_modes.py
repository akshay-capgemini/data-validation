"""Validation modes — INCREMENTAL (watermark lifecycle) and SAMPLE."""

from __future__ import annotations

import sqlite3

from dvf.audit.models import ValidationStatus
from dvf.core.engine import ValidationEngine
from dvf.metadata.models import RunRequest, ValidationConfig
from fixtures.seeddata import seed_identical_pair


def _insert(path: str, rows: list[tuple]) -> None:
    db = sqlite3.connect(path)
    db.executemany("INSERT INTO customers VALUES (?, ?, ?, ?, ?, ?)", rows)
    db.commit(), db.close()


def _config(vid: str, **over) -> ValidationConfig:
    base = dict(validation_id=vid, source_connection="WS", source_object="customers",
                target_connection="WT", target_object="customers",
                validation_profile="QUICK", primary_key="id")
    return ValidationConfig(**{**base, **over})


def test_incremental_watermark_lifecycle(settings, backend, repo,
                                         register_sqlite_pair, tmp_path):
    src, tgt = register_sqlite_pair("WS", "WT", seed_identical_pair, n=10)
    repo.upsert_validation_configs([_config(
        "VAL_INC", validation_mode="INCREMENTAL", watermark_column="id")])
    engine = ValidationEngine(settings, backend)

    # run 1: no watermark yet -> full scan, PASS, watermark advances to 10
    first = engine.run(RunRequest(validation_ids=["VAL_INC"]))
    assert first.validations[0].status == ValidationStatus.PASSED
    wm = repo.get_watermark("VAL_INC")
    assert wm is not None and wm.last_value_num == 10.0

    # new rows land: 11+12 in source, only 11 in target
    _insert(src, [(11, "New A", "a11@example.com", 1.0, 1, "2026-02-01 00:00:00"),
                  (12, "New B", "b12@example.com", 2.0, 0, "2026-02-01 00:00:00")])
    _insert(tgt, [(11, "New A", "a11@example.com", 1.0, 1, "2026-02-01 00:00:00")])

    # run 2: only the increment is validated -> 2 vs 1 -> FAILED, watermark holds
    second = engine.run(RunRequest(validation_ids=["VAL_INC"]))
    assert second.validations[0].status == ValidationStatus.FAILED
    assert second.validations[0].source_row_count == 2
    assert second.validations[0].target_row_count == 1
    results = backend.read_rows("results", "result_detail",
                                where_eq={"run_id": second.run_id,
                                          "rule_type": "RECORD_COUNT"})
    assert "> 10" in results[0]["source_sql"]
    assert repo.get_watermark("VAL_INC").last_value_num == 10.0   # NOT advanced

    # run 3: target catches up -> PASS, watermark advances to 12
    _insert(tgt, [(12, "New B", "b12@example.com", 2.0, 0, "2026-02-01 00:00:00")])
    third = engine.run(RunRequest(validation_ids=["VAL_INC"]))
    assert third.validations[0].status == ValidationStatus.PASSED
    assert repo.get_watermark("VAL_INC").last_value_num == 12.0


def test_sample_mode_applies_predicate(settings, backend, repo,
                                       register_sqlite_pair):
    register_sqlite_pair("SS", "ST", seed_identical_pair, n=50)
    repo.upsert_validation_configs([ValidationConfig(
        validation_id="VAL_SAMPLE", source_connection="SS",
        source_object="customers", target_connection="ST",
        target_object="customers", validation_profile="QUICK", primary_key="id",
        validation_mode="SAMPLE", sampling_type="RANDOM", sampling_value=50,
        tolerance_type="PERCENT", tolerance_value=100.0)])
    engine = ValidationEngine(settings, backend)
    summary = engine.run(RunRequest(validation_ids=["VAL_SAMPLE"]))
    results = backend.read_rows("results", "result_detail",
                                where_eq={"run_id": summary.run_id,
                                          "rule_type": "RECORD_COUNT"})
    # sqlite has no TABLESAMPLE -> random predicate fallback in the pushdown SQL
    assert "% 100) < 50" in results[0]["source_sql"]
