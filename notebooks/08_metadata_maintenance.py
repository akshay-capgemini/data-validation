# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Metadata Maintenance
# MAGIC Operational edits: enable/disable validations, reset watermarks, inspect
# MAGIC registered metadata. Every write is a MERGE with audit columns stamped.

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "qa", "prod"], "Environment")
dbutils.widgets.text("config_dir", "../config", "Config directory")
dbutils.widgets.dropdown("action", "show", ["show", "enable", "disable",
                                            "reset_watermark"], "Action")
dbutils.widgets.text("validation_ids", "", "Validation IDs (csv)")

# COMMAND ----------

import os

from dvf.config.loader import load_settings
from dvf.metadata.backend import DeltaBackend
from dvf.metadata.repository import MetadataRepository
from dvf.utils.timeutil import utcnow

settings = load_settings(os.path.abspath(dbutils.widgets.get("config_dir")),
                         env=dbutils.widgets.get("env"))
backend = DeltaBackend(spark, settings.catalog)
repo = MetadataRepository(backend)
actor = spark.sql("SELECT current_user()").first()[0]

action = dbutils.widgets.get("action")
ids = [v.strip() for v in dbutils.widgets.get("validation_ids").split(",") if v.strip()]

# COMMAND ----------

if action in ("enable", "disable"):
    assert ids, "validation_ids required"
    backend.merge_rows("meta", "validation_config", [
        {"validation_id": vid, "enabled": action == "enable",
         "updated_ts": utcnow(), "updated_by": actor} for vid in ids])
    print(f"{action}d: {', '.join(ids)}")

elif action == "reset_watermark":
    assert ids, "validation_ids required"
    for vid in ids:
        backend.delete_rows("meta", "watermarks", {"validation_id": vid})
    print(f"watermarks reset: {', '.join(ids)}")

# COMMAND ----------

print("— validation_config —")
display(spark.sql(
    f"SELECT validation_id, execution_group, validation_profile, validation_mode, "
    f"enabled, priority, execution_order, updated_ts, updated_by "
    f"FROM `{settings.catalog}`.`meta`.`validation_config` ORDER BY validation_id"))

# COMMAND ----------

print("— watermarks —")
display(spark.sql(f"SELECT * FROM `{settings.catalog}`.`meta`.`watermarks`"))
