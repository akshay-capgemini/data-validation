"""Excel report — workbook structure + status coloring from a real run."""

from __future__ import annotations

from openpyxl import load_workbook

from dvf.core.engine import ValidationEngine
from dvf.metadata.models import RunRequest, ValidationConfig
from dvf.reporting.manager import generate_reports
from fixtures.seeddata import seed_count_mismatch_pair


def test_excel_report(settings, backend, repo, register_sqlite_pair, tmp_path):
    register_sqlite_pair("XS", "XT", seed_count_mismatch_pair,
                         n=8, missing_in_target=2)
    repo.upsert_validation_configs([ValidationConfig(
        validation_id="VAL_XL", source_connection="XS", source_object="customers",
        target_connection="XT", target_object="customers",
        validation_profile="QUICK", primary_key="id")])
    engine = ValidationEngine(settings, backend)
    summary = engine.run(RunRequest())

    written = generate_reports(settings, backend, summary.run_id,
                               formats=["excel"])
    assert "excel" in written

    wb = load_workbook(written["excel"])
    assert wb.sheetnames == ["Summary", "Validations", "RuleResults", "Mismatches"]

    ws = wb["Summary"]
    labels = {ws.cell(row=i, column=1).value: ws.cell(row=i, column=2).value
              for i in range(1, 20)}
    assert labels["Run ID"] == summary.run_id
    assert labels["Status"] == "FAILED"
    assert labels["Failed"] == 1

    vs = wb["Validations"]
    header = [c.value for c in vs[1]]
    assert header[0] == "validation_id"
    assert vs.cell(row=2, column=1).value == "VAL_XL"
    assert vs.cell(row=2, column=header.index("status") + 1).value == "FAILED"

    rr = wb["RuleResults"]
    rule_types = {rr.cell(row=i, column=2).value
                  for i in range(2, rr.max_row + 1)}
    assert "RECORD_COUNT" in rule_types
