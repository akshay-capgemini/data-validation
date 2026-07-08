"""CSV/JSON exports + email message building (no network)."""

from __future__ import annotations

import csv
import json
from pathlib import Path

from dvf.core.engine import ValidationEngine
from dvf.metadata.models import RunRequest, ValidationConfig
from dvf.reporting.data import ReportDataProvider
from dvf.reporting.email import build_message, resolve_recipients
from dvf.reporting.manager import generate_reports
from fixtures.seeddata import seed_reconciliation_pair


def _run(settings, backend, repo, register_sqlite_pair):
    register_sqlite_pair("CJ_S", "CJ_T", seed_reconciliation_pair, n=10)
    repo.upsert_validation_configs([ValidationConfig(
        validation_id="VAL_CJ", source_connection="CJ_S", source_object="customers",
        target_connection="CJ_T", target_object="customers",
        validation_profile="FULL", primary_key="id")])
    return ValidationEngine(settings, backend).run(RunRequest())


def test_csv_and_json_reports(settings, backend, repo, register_sqlite_pair):
    summary = _run(settings, backend, repo, register_sqlite_pair)
    written = generate_reports(settings, backend, summary.run_id,
                               formats=["csv", "json"])

    doc = json.loads(Path(written["json"]).read_text(encoding="utf-8"))
    assert doc["run"]["run_id"] == summary.run_id
    assert any(v["validation_id"] == "VAL_CJ" for v in doc["validations"])
    assert doc["mismatch_total"] == 6            # 2 missing + 2 extra + 2 col rows

    summary_csv = Path(written["csv"])
    base = summary_csv.name.replace("_summary.csv", "")
    val_csv = summary_csv.with_name(f"{base}_validations.csv")
    with open(val_csv, newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert rows[0]["validation_id"] == "VAL_CJ"
    assert rows[0]["status"] == "FAILED"
    mm_csv = summary_csv.with_name(f"{base}_mismatches.csv")
    with open(mm_csv, newline="", encoding="utf-8") as fh:
        mm_rows = list(csv.DictReader(fh))
    assert {r["mismatch_type"] for r in mm_rows} == \
        {"MISSING_IN_TARGET", "EXTRA_IN_TARGET", "COLUMN_MISMATCH"}


def test_email_message_build(settings, backend, repo, register_sqlite_pair,
                             tmp_path):
    summary = _run(settings, backend, repo, register_sqlite_pair)
    data = ReportDataProvider(backend).fetch(summary.run_id)

    settings.reporting.email.recipients = ["dq-team@example.com"]
    settings.reporting.email.recipients_by_group = {
        None: [], "OTHER": ["other@example.com"]}
    recipients = resolve_recipients(settings.reporting.email, ["OTHER"])
    assert recipients == ["dq-team@example.com", "other@example.com"]

    attachment = tmp_path / "report.xlsx"
    attachment.write_bytes(b"fake-xlsx")
    msg = build_message(settings, data, recipients, [str(attachment)])
    assert "[DVF] PARTIAL" in msg["Subject"] or "[DVF] FAILED" in msg["Subject"]
    assert summary.run_id in msg["Subject"]
    parts = msg.get_payload()
    assert any(p.get_content_type() == "text/html" for p in parts)
    assert any(p.get_filename() == "report.xlsx" for p in parts)
