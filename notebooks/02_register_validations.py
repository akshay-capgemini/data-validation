# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Register Validations
# MAGIC Bulk-upsert `meta.validation_config` (and optional `meta.validation_rules`)
# MAGIC from a CSV/JSON file on S3/Volumes, or from the inline examples below.
# MAGIC
# MAGIC CSV columns = validation_config column names (JSON columns like
# MAGIC `column_mapping` hold JSON strings).

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "qa", "prod"], "Environment")
dbutils.widgets.text("config_dir", "../config", "Config directory")
dbutils.widgets.text("import_path", "", "CSV/JSON path (blank = inline examples)")

# COMMAND ----------

import os

from dvf.config.loader import load_settings
from dvf.metadata.backend import DeltaBackend
from dvf.metadata.models import ValidationConfig
from dvf.metadata.repository import MetadataRepository

settings = load_settings(os.path.abspath(dbutils.widgets.get("config_dir")),
                         env=dbutils.widgets.get("env"))
repo = MetadataRepository(DeltaBackend(spark, settings.catalog))
actor = spark.sql("SELECT current_user()").first()[0]

# COMMAND ----------

import_path = dbutils.widgets.get("import_path").strip()
if import_path:
    reader = (spark.read.format("json").option("multiLine", "true")
              if import_path.lower().endswith(".json")
              else spark.read.format("csv").option("header", "true"))
    rows = [r.asDict() for r in reader.load(import_path).collect()]
    configs = [ValidationConfig.from_row(r) for r in rows]
else:
    configs = [
        ValidationConfig(
            validation_id="VAL_SALES_CUSTOMERS",
            execution_group="SALES",
            description="ODS customers vs Snowflake customers",
            source_connection="SQLSERVER_ODS", source_schema="dbo",
            source_object="customers",
            target_connection="SNOWFLAKE_DW", target_schema="CORE",
            target_object="CUSTOMERS",
            primary_key="customer_id",
            validation_profile="FULL", priority=100, execution_order=10),
        ValidationConfig(
            validation_id="VAL_SALES_ORDERS_INC",
            execution_group="SALES",
            description="Incremental orders reconciliation",
            source_connection="SQLSERVER_ODS", source_schema="dbo",
            source_object="orders",
            target_connection="SNOWFLAKE_DW", target_schema="CORE",
            target_object="ORDERS",
            primary_key="order_id",
            validation_profile="INCREMENTAL", validation_mode="INCREMENTAL",
            watermark_column="order_id", priority=100, execution_order=20),
    ]

repo.upsert_validation_configs(configs, actor=actor)
print(f"upserted {len(configs)} validation config(s)")
display(spark.sql(
    f"SELECT validation_id, execution_group, source_connection, source_object, "
    f"target_connection, target_object, validation_profile, enabled "
    f"FROM `{settings.catalog}`.`meta`.`validation_config` ORDER BY validation_id"))
