"""End-to-end (no Spark): bootstrap -> register -> run -> audit -> HTML report.

This is the phase-gate test: the full pipeline proven with zero external
infrastructure.
"""

from __future__ import annotations

from pathlib import Path

from dvf.audit.models import RunStatus, ValidationStatus
from dvf.core.engine import ValidationEngine
from dvf.metadata.models import RunRequest, ValidationConfig
from dvf.reporting.manager import generate_reports
from fixtures.seeddata import seed_count_mismatch_pair, seed_identical_pair


def test_full_pipeline_sqlite(settings, backend, repo, register_sqlite_pair,
                              tmp_path):
    # --- register endpoints & validations (metadata-driven, nothing hardcoded)
    register_sqlite_pair("ODS", "CORE", seed_identical_pair, n=10)
    register_sqlite_pair("ODS2", "CORE2", seed_count_mismatch_pair,
                         n=10, missing_in_target=3)
    repo.upsert_validation_configs([
        ValidationConfig(validation_id="E2E_OK", execution_group="E2E",
                         source_connection="ODS", source_object="customers",
                         target_connection="CORE", target_object="customers",
                         validation_profile="QUICK", primary_key="id",
                         execution_order=1),
        ValidationConfig(validation_id="E2E_BAD", execution_group="E2E",
                         source_connection="ODS2", source_object="customers",
                         target_connection="CORE2", target_object="customers",
                         validation_profile="QUICK", primary_key="id",
                         execution_order=2),
    ])

    # --- run
    engine = ValidationEngine(settings, backend)
    summary = engine.run(RunRequest(run_name="e2e", trigger_type="MANUAL",
                                    triggered_by="pytest",
                                    execution_groups=["E2E"]))

    # --- engine outcome
    assert summary.total == 2
    assert summary.status == RunStatus.PARTIAL
    assert engine.writer.errors == []
    statuses = {v.validation_id: v.status for v in summary.validations}
    assert statuses == {"E2E_OK": ValidationStatus.PASSED,
                        "E2E_BAD": ValidationStatus.FAILED}

    # --- audit completeness: 5 QUICK rules per validation, every row finalized
    audits = backend.read_rows("audit", "validation_run_audit",
                               where_eq={"run_id": summary.run_id})
    assert len(audits) == 2
    assert all(a["end_ts"] is not None and a["duration_sec"] is not None
               for a in audits)
    results = backend.read_rows("results", "result_detail",
                                where_eq={"run_id": summary.run_id})
    assert len(results) == 10                     # 2 validations x 5 QUICK rules
    assert all(r["source_sql"] for r in results if r["rule_type"] == "RECORD_COUNT")

    # --- report renders from audit data only
    written = generate_reports(settings, backend, summary.run_id)
    assert "html" in written
    html = Path(written["html"]).read_text(encoding="utf-8")
    assert summary.run_id in html
    assert "E2E_BAD" in html and "FAILED" in html
    assert "SELECT COUNT(*)" in html              # embedded pushdown SQL

    # --- resume: passed validation skipped, failed one re-attempted
    resumed = engine.run(RunRequest(resume_run_id=summary.run_id,
                                    execution_groups=["E2E"]))
    by_id = {v.validation_id: v.status for v in resumed.validations}
    assert by_id["E2E_OK"] == ValidationStatus.SKIPPED
    assert by_id["E2E_BAD"] == ValidationStatus.FAILED
