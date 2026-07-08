# Databricks notebook source
# MAGIC %md
# MAGIC # 11 — Retention Purge
# MAGIC Deletes audit/result rows past their retention windows
# MAGIC (`retention.*` in framework.yaml), then VACUUMs the Delta tables.
# MAGIC Schedule monthly alongside the validation jobs.

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "qa", "prod"], "Environment")
dbutils.widgets.text("config_dir", "../config", "Config directory")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry run")

# COMMAND ----------

import os

from dvf.config.loader import load_settings

settings = load_settings(os.path.abspath(dbutils.widgets.get("config_dir")),
                         env=dbutils.widgets.get("env"))
catalog = settings.catalog
dry = dbutils.widgets.get("dry_run") == "true"

TARGETS = [
    ("audit", "run_audit", "start_ts", settings.retention.audit_days),
    ("audit", "validation_run_audit", "start_ts", settings.retention.audit_days),
    ("results", "result_detail", "start_ts", settings.retention.results_days),
    ("results", "mismatch_detail", "captured_ts", settings.retention.results_days),
    ("results", "profile_detail", None, settings.retention.results_days),
]

# COMMAND ----------

for schema, table, ts_col, days in TARGETS:
    name = f"`{catalog}`.`{schema}`.`{table}`"
    if ts_col is None:
        # profile_detail has no timestamp of its own — purge via run_audit age
        predicate = (f"run_id IN (SELECT run_id FROM `{catalog}`.`audit`.`run_audit` "
                     f"WHERE start_ts < current_timestamp() - INTERVAL {days} DAYS)")
    else:
        predicate = f"{ts_col} < current_timestamp() - INTERVAL {days} DAYS"
    count = spark.sql(f"SELECT COUNT(*) AS c FROM {name} WHERE {predicate}").first()["c"]
    print(f"{name}: {count} row(s) older than {days}d", "— DRY RUN" if dry else "")
    if not dry and count:
        spark.sql(f"DELETE FROM {name} WHERE {predicate}")
        spark.sql(f"VACUUM {name}")
        print(f"  deleted + vacuumed")

print("done" + (" (dry run — flip the widget to purge)" if dry else ""))
