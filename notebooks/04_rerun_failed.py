# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Rerun Failed / Resume
# MAGIC * **rerun_failed**: new run (trigger RERUN, parent_run_id set) covering only the
# MAGIC   validations whose latest attempt in the given run failed/errored/timed out.
# MAGIC * **resume**: re-executes everything from the given run that has not PASSED
# MAGIC   (use after an interrupted run).

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "qa", "prod"], "Environment")
dbutils.widgets.text("config_dir", "../config", "Config directory")
dbutils.widgets.text("run_id", "", "Run ID to rerun/resume")
dbutils.widgets.dropdown("mode", "rerun_failed", ["rerun_failed", "resume"], "Mode")
dbutils.widgets.dropdown("generate_reports", "true", ["true", "false"], "Generate reports")

# COMMAND ----------

import os

from dvf.config.loader import load_settings
from dvf.core.engine import ValidationEngine
from dvf.core.rerun import RerunManager
from dvf.metadata.backend import DeltaBackend

run_id = dbutils.widgets.get("run_id").strip()
assert run_id, "run_id widget is required"

settings = load_settings(os.path.abspath(dbutils.widgets.get("config_dir")),
                         env=dbutils.widgets.get("env"))
backend = DeltaBackend(spark, settings.catalog)
engine = ValidationEngine(settings, backend, spark=spark, dbutils=dbutils)
manager = RerunManager(engine)
user = spark.sql("SELECT current_user()").first()[0]

# COMMAND ----------

if dbutils.widgets.get("mode") == "resume":
    summary = manager.resume(run_id, triggered_by=user)
else:
    failed = manager.failed_validation_ids(run_id)
    print(f"failed in {run_id}: {failed or '(none)'}")
    summary = manager.rerun_failed(run_id, triggered_by=user)

if summary is None:
    print("Nothing to rerun — all validations passed.")
else:
    print(f"new run_id : {summary.run_id}")
    print(f"status     : {summary.status}")
    print(f"passed={summary.passed} failed={summary.failed} "
          f"errors={summary.errors} skipped={summary.skipped}")

# COMMAND ----------

if summary is not None:
    display(spark.sql(f"""
        SELECT validation_id, attempt, status, rules_passed, rules_total,
               duration_sec, error_message
        FROM `{settings.catalog}`.`audit`.`validation_run_audit`
        WHERE run_id = '{summary.run_id}'
        ORDER BY validation_id, attempt"""))
    if dbutils.widgets.get("generate_reports") == "true":
        from dvf.reporting.manager import generate_reports
        for fmt, path in generate_reports(settings, backend, summary.run_id).items():
            print(f"[{fmt}] {path}")
