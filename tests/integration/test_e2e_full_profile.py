"""E2E FULL profile: counts match but rows drifted — only hash comparison
catches it; mismatch repository + audit rollup + reports all populated."""

from __future__ import annotations

from pathlib import Path

from dvf.audit.models import ValidationStatus
from dvf.core.engine import ValidationEngine
from dvf.metadata.models import RunRequest, ValidationConfig
from dvf.reporting.manager import generate_reports
from fixtures.seeddata import seed_reconciliation_pair


def test_full_profile_end_to_end(settings, backend, repo, register_sqlite_pair):
    register_sqlite_pair("F_SRC", "F_TGT", seed_reconciliation_pair, n=10)
    repo.upsert_validation_configs([ValidationConfig(
        validation_id="E2E_FULL", execution_group="RECON",
        source_connection="F_SRC", source_object="customers",
        target_connection="F_TGT", target_object="customers",
        validation_profile="FULL", primary_key="id")])

    engine = ValidationEngine(settings, backend)
    summary = engine.run(RunRequest(execution_groups=["RECON"]))

    assert engine.writer.errors == []
    audit = summary.validations[0]
    assert audit.status == ValidationStatus.FAILED

    # record counts MATCH (10=10) yet reconciliation catches the drift
    assert audit.source_row_count == 10 and audit.target_row_count == 10
    assert audit.rows_missing == 2
    assert audit.rows_extra == 2
    assert audit.rows_mismatched == 2
    assert audit.rows_matched == 6
    assert audit.comparison_tier_used == "SMALL"

    results = backend.read_rows("results", "result_detail",
                                where_eq={"run_id": summary.run_id})
    by_type = {r["rule_type"]: r for r in results}
    assert by_type["RECORD_COUNT"]["status"] == "PASS"        # the deceiver
    assert by_type["ROW_HASH_COMPARE"]["status"] == "FAIL"    # the catcher
    assert by_type["MISSING_RECORDS"]["status"] == "FAIL"
    assert by_type["EXTRA_RECORDS"]["status"] == "FAIL"
    assert by_type["COLUMN_COMPARE"]["status"] == "FAIL"

    mismatches = backend.read_rows("results", "mismatch_detail",
                                   where_eq={"run_id": summary.run_id})
    types = {}
    for m in mismatches:
        types[m["mismatch_type"]] = types.get(m["mismatch_type"], 0) + 1
    assert types["MISSING_IN_TARGET"] == 2
    assert types["EXTRA_IN_TARGET"] == 2
    assert types["COLUMN_MISMATCH"] == 2          # one per drifted column

    written = generate_reports(settings, backend, summary.run_id,
                               formats=["html", "excel"])
    html = Path(written["html"]).read_text(encoding="utf-8")
    assert "MISSING_IN_TARGET" in html and "ROW_HASH_COMPARE" in html
