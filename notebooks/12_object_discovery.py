# Databricks notebook source
# MAGIC %md
# MAGIC # 12 — Object Discovery
# MAGIC Scan a source schema, match objects on the target, detect primary keys, and
# MAGIC register DISABLED validation candidates for review — thousands of tables
# MAGIC onboarded without hand-writing metadata.
# MAGIC
# MAGIC Review with notebook 08, then enable.

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "qa", "prod"], "Environment")
dbutils.widgets.text("config_dir", "../config", "Config directory")
dbutils.widgets.text("source_connection", "", "Source connection")
dbutils.widgets.text("source_schema", "", "Source schema")
dbutils.widgets.text("target_connection", "", "Target connection")
dbutils.widgets.text("target_schema", "", "Target schema")
dbutils.widgets.text("execution_group", "DISCOVERED", "Execution group")
dbutils.widgets.text("profile", "STANDARD", "Validation profile")
dbutils.widgets.text("name_pattern", "", "Only objects containing (blank = all)")
dbutils.widgets.dropdown("uppercase_target", "false", ["false", "true"],
                         "Uppercase target object names")
dbutils.widgets.dropdown("register", "false", ["false", "true"],
                         "Register proposals (false = preview only)")

# COMMAND ----------

import os

from dvf.config.loader import load_settings
from dvf.connectors.factory import ConnectorFactory
from dvf.metadata.backend import DeltaBackend
from dvf.metadata.discovery import ObjectDiscovery
from dvf.metadata.repository import MetadataRepository
from dvf.utils.secrets import SecretResolver

settings = load_settings(os.path.abspath(dbutils.widgets.get("config_dir")),
                         env=dbutils.widgets.get("env"))
backend = DeltaBackend(spark, settings.catalog)
repo = MetadataRepository(backend)
factory = ConnectorFactory(SecretResolver(dbutils=dbutils), spark=spark)
discovery = ObjectDiscovery(repo, factory)

pattern = dbutils.widgets.get("name_pattern").strip().lower()
upper = dbutils.widgets.get("uppercase_target") == "true"

proposals = discovery.discover(
    source_connection=dbutils.widgets.get("source_connection").strip(),
    source_schema=dbutils.widgets.get("source_schema").strip() or None,
    target_connection=dbutils.widgets.get("target_connection").strip(),
    target_schema=dbutils.widgets.get("target_schema").strip() or None,
    execution_group=dbutils.widgets.get("execution_group").strip(),
    profile=dbutils.widgets.get("profile").strip().upper(),
    name_map=(lambda n: n.upper()) if upper else None,
    include=(lambda n: pattern in n.lower()) if pattern else None,
)
print(f"{len(proposals)} proposal(s)")
if proposals:
    display(spark.createDataFrame(
        [(p.validation_id, p.source_object, p.target_object,
          p.primary_key or "(none detected)", p.validation_profile)
         for p in proposals],
        ["validation_id", "source_object", "target_object", "primary_key",
         "profile"]))

# COMMAND ----------

if dbutils.widgets.get("register") == "true" and proposals:
    actor = spark.sql("SELECT current_user()").first()[0]
    count = discovery.register(proposals, actor=actor)
    print(f"registered {count} DISABLED validation(s) — review with notebook 08, "
          "then enable")
else:
    print("preview only — set register=true to persist")
