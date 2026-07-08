# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Bootstrap Metadata
# MAGIC Idempotent: creates the Unity Catalog schemas (`meta`, `audit`, `results`),
# MAGIC every framework Delta table, the `v_execution_history` view, and seeds the
# MAGIC validation profiles (QUICK/STANDARD/FULL/MIGRATION/REGRESSION/INCREMENTAL).
# MAGIC
# MAGIC **Prereqs**
# MAGIC * `dvf` wheel installed on the cluster (or `%pip install` below)
# MAGIC * `CREATE SCHEMA` / `CREATE TABLE` grants on the target catalog
# MAGIC * The catalog itself must exist (ask a metastore admin, or run the optional cell)

# COMMAND ----------

# %pip install /Volumes/<catalog>/<schema>/<volume>/dvf-0.1.0-py3-none-any.whl
# dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "qa", "prod"], "Environment")
dbutils.widgets.text("config_dir", "../config", "Config directory")
dbutils.widgets.dropdown("create_catalog", "false", ["false", "true"],
                         "Attempt CREATE CATALOG")

# COMMAND ----------

import os

from dvf.config.loader import load_settings
from dvf.metadata.backend import DeltaBackend

env = dbutils.widgets.get("env")
config_dir = os.path.abspath(dbutils.widgets.get("config_dir"))
settings = load_settings(config_dir, env=env)
print(f"environment = {settings.environment}")
print(f"catalog     = {settings.catalog}")

# COMMAND ----------

if dbutils.widgets.get("create_catalog") == "true":
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{settings.catalog}`")

backend = DeltaBackend(spark, settings.catalog)
backend.create_all()
print("Bootstrap complete.")

# COMMAND ----------

# MAGIC %md ## Verify

# COMMAND ----------

for schema in ("meta", "audit", "results"):
    print(f"-- {settings.catalog}.{schema}")
    for row in spark.sql(f"SHOW TABLES IN `{settings.catalog}`.`{schema}`").collect():
        print(f"   {row.tableName}")

display(spark.sql(
    f"SELECT profile_name, COUNT(*) AS rule_count "
    f"FROM `{settings.catalog}`.`meta`.`validation_profiles` "
    f"GROUP BY profile_name ORDER BY profile_name"))
