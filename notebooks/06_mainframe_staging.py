# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Mainframe Staging
# MAGIC Decode a COBOL copybook / sequential / VSAM / GDG file once (Cobrix) and land
# MAGIC it as a Delta staging table, so repeated validations against the same
# MAGIC mainframe extract never re-decode EBCDIC.
# MAGIC
# MAGIC **Cluster libs**: Maven `za.co.absa.cobrix:spark-cobol_2.12:<latest>`
# MAGIC
# MAGIC After staging, register a `DELTA` connection pointing at the staging schema
# MAGIC and validate `staging_table ↔ target` as usual.

# COMMAND ----------

dbutils.widgets.text("data_path", "", "Mainframe file path (S3/Volume)")
dbutils.widgets.text("copybook_path", "", "Copybook path")
dbutils.widgets.dropdown("record_format", "F", ["F", "V", "VB"], "Record format")
dbutils.widgets.dropdown("is_record_sequence", "false", ["false", "true"],
                         "VSAM record sequence")
dbutils.widgets.text("code_page", "cp037", "EBCDIC code page")
dbutils.widgets.dropdown("resolve_gdg", "false", ["false", "true"],
                         "Resolve latest GDG generation")
dbutils.widgets.text("staging_table", "", "Target staging table (catalog.schema.table)")

# COMMAND ----------

from dvf.connectors.files import pick_latest_generation

data_path = dbutils.widgets.get("data_path").strip()
staging_table = dbutils.widgets.get("staging_table").strip()
assert data_path and staging_table, "data_path and staging_table are required"

if dbutils.widgets.get("resolve_gdg") == "true":
    entries = [e.name.rstrip("/") for e in dbutils.fs.ls(data_path)]
    latest = pick_latest_generation(entries)
    assert latest, f"no GxxxxVyy generations under {data_path}"
    data_path = f"{data_path.rstrip('/')}/{latest}"
    print(f"resolved GDG -> {data_path}")

# COMMAND ----------

reader = (spark.read.format("cobol")
          .option("copybook", dbutils.widgets.get("copybook_path"))
          .option("record_format", dbutils.widgets.get("record_format"))
          .option("encoding", "ebcdic")
          .option("ebcdic_code_page", dbutils.widgets.get("code_page"))
          .option("schema_retention_policy", "collapse_root"))
if dbutils.widgets.get("is_record_sequence") == "true":
    reader = reader.option("is_record_sequence", "true")

df = reader.load(data_path)
print(f"decoded schema:")
df.printSchema()

# COMMAND ----------

(df.write.format("delta").mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(staging_table))
count = spark.table(staging_table).count()
print(f"staged {count} rows -> {staging_table}")
print("Register a DELTA connection for this schema and point validations at "
      "the staging table.")
