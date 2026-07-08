# Databricks notebook source
# MAGIC %md
# MAGIC # 10 — Scheduled Driver (Databricks Jobs entrypoint)
# MAGIC Headless variant of 03: parameters via job widgets, run + reports + email,
# MAGIC then `dbutils.notebook.exit(json)` so downstream tasks can gate on status.
# MAGIC Optionally fails the task on FAILED/PARTIAL runs for alerting.

# COMMAND ----------

dbutils.widgets.text("env", "dev", "Environment")
dbutils.widgets.text("config_dir", "../config", "Config directory")
dbutils.widgets.text("execution_groups", "", "Execution groups (csv)")
dbutils.widgets.text("validation_ids", "", "Validation IDs (csv)")
dbutils.widgets.text("profile_override", "", "Profile override")
dbutils.widgets.text("run_name", "scheduled", "Run name")
dbutils.widgets.text("fail_task_on", "FAILED", "Fail task when run status in (csv; blank=never)")
dbutils.widgets.text("send_email", "true", "Send email (true/false)")

# COMMAND ----------

import json
import os

from dvf.config.loader import load_settings
from dvf.core.engine import ValidationEngine
from dvf.metadata.backend import DeltaBackend
from dvf.metadata.models import RunRequest
from dvf.reporting.manager import generate_reports

settings = load_settings(os.path.abspath(dbutils.widgets.get("config_dir")),
                         env=dbutils.widgets.get("env"))
backend = DeltaBackend(spark, settings.catalog)

def _csv(name):
    return [v.strip() for v in dbutils.widgets.get(name).split(",") if v.strip()]

engine = ValidationEngine(settings, backend, spark=spark, dbutils=dbutils)
summary = engine.run(RunRequest(
    run_name=dbutils.widgets.get("run_name") or "scheduled",
    trigger_type="SCHEDULED",
    triggered_by="databricks-job",
    execution_groups=_csv("execution_groups"),
    validation_ids=_csv("validation_ids"),
    profile_override=dbutils.widgets.get("profile_override") or None))

written = generate_reports(settings, backend, summary.run_id)

if dbutils.widgets.get("send_email").lower() == "true":
    from dvf.reporting.data import ReportDataProvider
    from dvf.reporting.email import send_report_email
    data = ReportDataProvider(backend).fetch(summary.run_id)
    groups = sorted({v.get("execution_group") for v in data.validations
                     if v.get("execution_group")})
    try:
        send_report_email(settings, data, groups=groups)
    except Exception as exc:  # alerting failure must not mask run status
        print(f"email failed: {exc}")

result = {"run_id": summary.run_id, "status": summary.status,
          "passed": summary.passed, "failed": summary.failed,
          "warnings": summary.warnings, "errors": summary.errors,
          "skipped": summary.skipped, "reports": written}
print(json.dumps(result, indent=2))

fail_on = {s.strip().upper() for s in
           dbutils.widgets.get("fail_task_on").split(",") if s.strip()}
if summary.status in fail_on:
    raise RuntimeError(f"DVF run {summary.run_id} finished {summary.status} — "
                       f"{summary.failed} failed / {summary.errors} errors "
                       f"(rerun via notebook 04)")
dbutils.notebook.exit(json.dumps(result))
