# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Generate Report
# MAGIC Re-render any format for ANY past run_id — reports read only from
# MAGIC audit/result Delta tables, so regeneration is byte-stable and free of
# MAGIC source-system access. Optionally email the summary.

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "qa", "prod"], "Environment")
dbutils.widgets.text("config_dir", "../config", "Config directory")
dbutils.widgets.text("run_id", "", "Run ID")
dbutils.widgets.text("formats", "html,excel,json", "Formats (csv of html,excel,csv,json,pdf)")
dbutils.widgets.dropdown("send_email", "false", ["false", "true"], "Send email")

# COMMAND ----------

import os

from dvf.config.loader import load_settings
from dvf.metadata.backend import DeltaBackend
from dvf.reporting.manager import generate_reports

run_id = dbutils.widgets.get("run_id").strip()
assert run_id, "run_id widget is required"
settings = load_settings(os.path.abspath(dbutils.widgets.get("config_dir")),
                         env=dbutils.widgets.get("env"))
backend = DeltaBackend(spark, settings.catalog)
formats = [f.strip() for f in dbutils.widgets.get("formats").split(",") if f.strip()]

written = generate_reports(settings, backend, run_id, formats=formats)
for fmt, path in written.items():
    print(f"[{fmt}] {path}")

# COMMAND ----------

if "html" in written:
    from dvf.reporting.data import ReportDataProvider
    from dvf.reporting.html import HtmlReportGenerator
    displayHTML(HtmlReportGenerator(settings).render(
        ReportDataProvider(backend).fetch(run_id)))

# COMMAND ----------

if dbutils.widgets.get("send_email") == "true":
    from dvf.reporting.data import ReportDataProvider
    from dvf.reporting.email import send_report_email
    data = ReportDataProvider(backend).fetch(run_id)
    groups = sorted({v.get("execution_group") for v in data.validations
                     if v.get("execution_group")})
    attachments = [p for f, p in written.items()
                   if f == "excel" and not p.startswith(("s3://", "dbfs:/"))]
    sent = send_report_email(settings, data, groups=groups, attachments=attachments)
    print("email sent" if sent else "email skipped (disabled or no recipients)")
