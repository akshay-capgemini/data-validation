# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Register Connections
# MAGIC Upsert connection definitions into `meta.connections`. **Never** put
# MAGIC credentials here — only secret-scope / AWS Secrets Manager references.
# MAGIC
# MAGIC Edit the `CONNECTIONS` list (or load your own JSON) and run.

# COMMAND ----------

dbutils.widgets.dropdown("env", "dev", ["dev", "qa", "prod"], "Environment")
dbutils.widgets.text("config_dir", "../config", "Config directory")

# COMMAND ----------

import os

from dvf.config.loader import load_settings
from dvf.metadata.backend import DeltaBackend
from dvf.metadata.models import AuthMethods, ConnectionDef, ConnectionTypes
from dvf.metadata.repository import MetadataRepository

settings = load_settings(os.path.abspath(dbutils.widgets.get("config_dir")),
                         env=dbutils.widgets.get("env"))
repo = MetadataRepository(DeltaBackend(spark, settings.catalog))

# COMMAND ----------

# EXAMPLES — replace with your endpoints. auth via secret references only.
CONNECTIONS = [
    ConnectionDef(
        connection_name="SNOWFLAKE_DW",
        connection_type=ConnectionTypes.SNOWFLAKE,
        host="acme-xy12345.snowflakecomputing.com",
        database_name="EDW",
        extra_properties={"warehouse": "VALIDATION_WH", "role": "DVF_READER",
                          "schema": "PUBLIC"},
        auth_method=AuthMethods.DATABRICKS_SCOPE,
        secret_scope=settings.secret_scope_default,
        secret_key_user="snowflake-user",
        secret_key_password="snowflake-password",
    ),
    ConnectionDef(
        connection_name="SQLSERVER_ODS",
        connection_type=ConnectionTypes.SQLSERVER,
        host="odsdb.corp.example.com", port=1433, database_name="ODS",
        auth_method=AuthMethods.AWS_SECRETS_MANAGER,
        aws_secret_arn="arn:aws:secretsmanager:us-east-1:111122223333:secret:dvf/sqlserver-ods",
    ),
    ConnectionDef(
        connection_name="DB2_CORE",
        connection_type=ConnectionTypes.DB2_ZOS,
        host="mainframe.corp.example.com", port=446, database_name="DSNLOC1",
        auth_method=AuthMethods.DATABRICKS_SCOPE,
        secret_scope=settings.secret_scope_default,
        secret_key_user="db2-user", secret_key_password="db2-password",
    ),
    ConnectionDef(
        connection_name="MAINFRAME_LANDING",
        connection_type=ConnectionTypes.FILE_COBOL,
        database_name="s3://acme-landing/mainframe",
        extra_properties={"copybook": "s3://acme-landing/copybooks/CUSTOMER.cpy",
                          "record_format": "F", "code_page": "cp037"},
        auth_method=AuthMethods.NONE,
    ),
    ConnectionDef(
        connection_name="LAKE",
        connection_type=ConnectionTypes.DELTA,
        database_name="main",           # UC catalog
        auth_method=AuthMethods.NONE,
    ),
]

repo.upsert_connections(CONNECTIONS,
                        actor=spark.sql("SELECT current_user()").first()[0])
print(f"upserted {len(CONNECTIONS)} connection(s)")
display(spark.sql(f"SELECT connection_name, connection_type, host, database_name, "
                  f"auth_method, enabled FROM `{settings.catalog}`.`meta`.`connections`"))
