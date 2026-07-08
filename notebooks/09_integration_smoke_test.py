# Databricks notebook source
# MAGIC %md
# MAGIC # 09 — Integration Smoke Test (Delta ↔ Delta)
# MAGIC Proves the FULL pipeline on a real cluster with ZERO external systems:
# MAGIC seeds two Delta tables with planted defects, bootstraps an isolated smoke
# MAGIC catalog schema, runs a FULL-profile validation, and asserts exact outcomes.
# MAGIC
# MAGIC Green here = engine, dialects, comparison tiers, audit, and reporting all
# MAGIC work on this workspace.

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "qa", "prod"], "Environment")
dbutils.widgets.text("config_dir", "../config", "Config directory")
dbutils.widgets.text("smoke_schema", "dvf_smoke", "Schema for seeded tables")

# COMMAND ----------

import os

from dvf.config.loader import load_settings
from dvf.core.engine import ValidationEngine
from dvf.metadata.backend import DeltaBackend
from dvf.metadata.models import (
    AuthMethods, ConnectionDef, ConnectionTypes, RunRequest, ValidationConfig,
)
from dvf.metadata.repository import MetadataRepository

settings = load_settings(os.path.abspath(dbutils.widgets.get("config_dir")),
                         env=dbutils.widgets.get("env"))
catalog = settings.catalog
schema = dbutils.widgets.get("smoke_schema")
backend = DeltaBackend(spark, catalog)
backend.create_all()
repo = MetadataRepository(backend)

# COMMAND ----------

# MAGIC %md ## Seed: 1,000 rows; target drifts — counts equal, rows differ

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
src, tgt = f"`{catalog}`.`{schema}`.`cust_src`", f"`{catalog}`.`{schema}`.`cust_tgt`"

spark.sql(f"DROP TABLE IF EXISTS {src}")
spark.sql(f"DROP TABLE IF EXISTS {tgt}")
spark.sql(f"""
    CREATE TABLE {src} AS
    SELECT id,
           CONCAT('Customer ', LPAD(id, 4, '0'))       AS name,
           CONCAT('cust', id, '@example.com')          AS email,
           CAST(id * 100.5 AS DECIMAL(18,2))           AS amount,
           id % 2 = 0                                  AS is_active,
           TIMESTAMPADD(MINUTE, id, TIMESTAMP'2026-01-01 08:00:00') AS created_ts
    FROM RANGE(1, 1001)""")
# target: drop ids 100,500,900 (missing x3); add 2000,2001 (extra x2 -> 999 rows);
# drift amount on id 42 and name on id 777 (mismatched x2)
spark.sql(f"""
    CREATE TABLE {tgt} AS
    SELECT id,
           CASE WHEN id = 777 THEN 'DRIFTED NAME' ELSE name END       AS name,
           email,
           CASE WHEN id = 42 THEN CAST(amount + 1 AS DECIMAL(18,2))
                ELSE amount END                                       AS amount,
           is_active, created_ts
    FROM {src} WHERE id NOT IN (100, 500, 900)
    UNION ALL
    SELECT id, CONCAT('Extra ', id), CONCAT('x', id, '@example.com'),
           CAST(9.99 AS DECIMAL(18,2)), true,
           TIMESTAMP'2026-01-02 00:00:00'
    FROM RANGE(2000, 2002)""")
print(f"src={spark.table(src.replace('`','')).count()} rows, "
      f"tgt={spark.table(tgt.replace('`','')).count()} rows")

# COMMAND ----------

repo.upsert_connections([ConnectionDef(
    connection_name="SMOKE_LAKE", connection_type=ConnectionTypes.DELTA,
    database_name=catalog, auth_method=AuthMethods.NONE)], actor="smoke")
repo.upsert_validation_configs([ValidationConfig(
    validation_id="SMOKE_FULL", execution_group="SMOKE",
    source_connection="SMOKE_LAKE", source_schema=schema, source_object="cust_src",
    target_connection="SMOKE_LAKE", target_schema=schema, target_object="cust_tgt",
    primary_key="id", validation_profile="FULL")], actor="smoke")

engine = ValidationEngine(settings, backend, spark=spark, dbutils=dbutils)
summary = engine.run(RunRequest(run_name="smoke", validation_ids=["SMOKE_FULL"]))
audit = summary.validations[0]
print(f"status={audit.status} tier={audit.comparison_tier_used} "
      f"missing={audit.rows_missing} extra={audit.rows_extra} "
      f"mismatched={audit.rows_mismatched}")

# COMMAND ----------

# MAGIC %md ## Assertions — exact planted-defect detection

# COMMAND ----------

assert audit.status == "FAILED", audit.status
assert audit.rows_missing == 3, audit.rows_missing
assert audit.rows_extra == 2, audit.rows_extra
assert audit.rows_mismatched == 2, audit.rows_mismatched
assert engine.writer.errors == [], engine.writer.errors

mm = spark.sql(f"""
    SELECT mismatch_type, COUNT(*) AS c
    FROM `{catalog}`.`results`.`mismatch_detail`
    WHERE run_id = '{summary.run_id}' GROUP BY mismatch_type""").collect()
counts = {r["mismatch_type"]: r["c"] for r in mm}
assert counts["MISSING_IN_TARGET"] == 3 and counts["EXTRA_IN_TARGET"] == 2, counts
print("✅ SMOKE TEST PASSED — pipeline verified on this workspace")

# COMMAND ----------

from dvf.reporting.manager import generate_reports
written = generate_reports(settings, backend, summary.run_id,
                           formats=["html", "excel", "json"])
for fmt, path in written.items():
    print(f"[{fmt}] {path}")
