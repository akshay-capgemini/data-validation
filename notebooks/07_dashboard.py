# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Dashboard
# MAGIC Execution history, trends, failure hot-spots, and DQ score — all from
# MAGIC `audit.v_execution_history` (a view; no dual-write drift). Pin these cells
# MAGIC to a Lakeview / SQL dashboard for standing visibility.

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "qa", "prod"], "Environment")
dbutils.widgets.text("config_dir", "../config", "Config directory")
dbutils.widgets.text("days", "30", "Lookback days")

# COMMAND ----------

import os

from dvf.config.loader import load_settings

settings = load_settings(os.path.abspath(dbutils.widgets.get("config_dir")),
                         env=dbutils.widgets.get("env"))
V = f"`{settings.catalog}`.`audit`.`v_execution_history`"
days = int(dbutils.widgets.get("days"))

# COMMAND ----------

# MAGIC %md ## Run trend — success rate & duration

# COMMAND ----------

display(spark.sql(f"""
    SELECT DATE(run_start_ts) AS run_date,
           COUNT(DISTINCT run_id) AS runs,
           SUM(CASE WHEN validation_status = 'PASSED' THEN 1 ELSE 0 END) AS passed,
           SUM(CASE WHEN validation_status IN ('FAILED','ERROR','TIMEOUT')
               THEN 1 ELSE 0 END) AS failed,
           ROUND(AVG(duration_sec), 1) AS avg_validation_secs,
           ROUND(AVG(dq_score), 2) AS avg_dq_score
    FROM {V}
    WHERE run_start_ts >= current_timestamp() - INTERVAL {days} DAYS
    GROUP BY DATE(run_start_ts) ORDER BY run_date"""))

# COMMAND ----------

# MAGIC %md ## Failure hot-spots (last N days)

# COMMAND ----------

display(spark.sql(f"""
    SELECT validation_id, execution_group, source_object, target_object,
           COUNT(*) AS attempts,
           SUM(CASE WHEN validation_status IN ('FAILED','ERROR','TIMEOUT')
               THEN 1 ELSE 0 END) AS failures,
           ROUND(AVG(dq_score), 2) AS avg_dq_score,
           MAX(end_ts) AS last_run
    FROM {V}
    WHERE run_start_ts >= current_timestamp() - INTERVAL {days} DAYS
    GROUP BY validation_id, execution_group, source_object, target_object
    HAVING failures > 0
    ORDER BY failures DESC, avg_dq_score ASC
    LIMIT 50"""))

# COMMAND ----------

# MAGIC %md ## DQ score by execution group

# COMMAND ----------

display(spark.sql(f"""
    SELECT execution_group,
           COUNT(DISTINCT validation_id) AS validations,
           ROUND(AVG(dq_score), 2) AS dq_score,
           SUM(COALESCE(rows_mismatched,0) + COALESCE(rows_missing,0)
               + COALESCE(rows_extra,0)) AS total_row_diffs
    FROM {V}
    WHERE run_start_ts >= current_timestamp() - INTERVAL {days} DAYS
    GROUP BY execution_group ORDER BY dq_score ASC"""))

# COMMAND ----------

# MAGIC %md ## Latest run detail

# COMMAND ----------

display(spark.sql(f"""
    WITH latest AS (SELECT MAX(run_id) AS run_id FROM {V})
    SELECT validation_id, validation_status, rules_passed, rules_total,
           source_row_count, target_row_count, rows_mismatched, rows_missing,
           rows_extra, comparison_tier_used, duration_sec, dq_score, error_message
    FROM {V} JOIN latest USING (run_id)
    ORDER BY validation_status DESC, validation_id"""))
