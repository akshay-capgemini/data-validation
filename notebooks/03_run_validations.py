# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Run Validations
# MAGIC Main driver: selects enabled validations from metadata (optionally filtered by
# MAGIC group/ids), executes them with pushdown SQL, checkpoints audit/results to
# MAGIC Delta, and renders the configured report formats.
# MAGIC
# MAGIC Everything is metadata-driven — this notebook only wires widgets → engine → report.

# COMMAND ----------

# %pip install /Volumes/<catalog>/<schema>/<volume>/dvf-0.1.0-py3-none-any.whl
# dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "qa", "prod"], "Environment")
dbutils.widgets.text("config_dir", "../config", "Config directory")
dbutils.widgets.text("execution_groups", "", "Execution groups (csv, blank = all)")
dbutils.widgets.text("validation_ids", "", "Validation IDs (csv, blank = all)")
dbutils.widgets.text("profile_override", "", "Profile override (blank = per metadata)")
dbutils.widgets.text("run_name", "", "Run name")
dbutils.widgets.dropdown("generate_reports", "true", ["true", "false"], "Generate reports")
dbutils.widgets.text("resume_run_id", "", "Resume from run_id (skip PASSED)")

# COMMAND ----------

import os

from dvf.config.loader import load_settings
from dvf.core.engine import ValidationEngine
from dvf.metadata.backend import DeltaBackend
from dvf.metadata.models import RunRequest

env = dbutils.widgets.get("env")
settings = load_settings(os.path.abspath(dbutils.widgets.get("config_dir")), env=env)
backend = DeltaBackend(spark, settings.catalog)

def _csv(name):
    return [v.strip() for v in dbutils.widgets.get(name).split(",") if v.strip()]

request = RunRequest(
    run_name=dbutils.widgets.get("run_name") or None,
    trigger_type="MANUAL",
    triggered_by=spark.sql("SELECT current_user()").first()[0],
    execution_groups=_csv("execution_groups"),
    validation_ids=_csv("validation_ids"),
    profile_override=dbutils.widgets.get("profile_override") or None,
    resume_run_id=dbutils.widgets.get("resume_run_id") or None,
)

# COMMAND ----------

engine = ValidationEngine(settings, backend, spark=spark, dbutils=dbutils)
summary = engine.run(request)

print(f"run_id   : {summary.run_id}")
print(f"status   : {summary.status}")
print(f"passed   : {summary.passed} / failed: {summary.failed} / "
      f"warnings: {summary.warnings} / errors: {summary.errors} / "
      f"skipped: {summary.skipped}")
print(f"duration : {summary.duration_sec:.1f}s")

# COMMAND ----------

# MAGIC %md ## Validation outcomes

# COMMAND ----------

display(spark.sql(f"""
    SELECT validation_id, status, rules_passed, rules_total,
           source_row_count, target_row_count, rows_mismatched,
           duration_sec, error_message
    FROM `{settings.catalog}`.`audit`.`validation_run_audit`
    WHERE run_id = '{summary.run_id}'
    ORDER BY validation_id"""))

# COMMAND ----------

# MAGIC %md ## Reports

# COMMAND ----------

if dbutils.widgets.get("generate_reports") == "true":
    from dvf.reporting.manager import generate_reports
    written = generate_reports(settings, backend, summary.run_id)
    for fmt, path in written.items():
        print(f"[{fmt}] {path}")

    if "html" in written:
        from dvf.reporting.data import ReportDataProvider
        from dvf.reporting.html import HtmlReportGenerator
        data = ReportDataProvider(backend).fetch(summary.run_id)
        displayHTML(HtmlReportGenerator(settings).render(data))

# COMMAND ----------

if summary.status in ("FAILED", "PARTIAL"):
    print(f"⚠ run {summary.run_id} finished {summary.status} — "
          f"rerun failures with notebook 04 (widget run_id={summary.run_id})")
