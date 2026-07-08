# Metadata Reference

Catalog per environment: `dvf_dev` / `dvf_qa` / `dvf_prod` (from
`config/environments/*.yaml`). Schemas: `meta` (configuration), `audit`
(run lifecycle), `results` (outcomes + exceptions). All writes are MERGEs via
the repository/writer — never write these tables by hand.

## meta.connections

| Column | Notes |
|---|---|
| connection_name PK | referenced by validation_config |
| connection_type | `SQLSERVER` `ORACLE` `POSTGRES` `MYSQL` `DB2_LUW` `DB2_ZOS` `TERADATA` `SNOWFLAKE` `DELTA` `SQLITE`(test) `FILE_CSV` `FILE_FIXED` `FILE_PARQUET` `FILE_DELTA` `FILE_JSON` `FILE_XML` `FILE_EXCEL` `FILE_COBOL` |
| host / port | RDBMS endpoints (port defaults per engine) |
| database_name | database / catalog / Snowflake DB / UC catalog / **base path for FILE_*** |
| extra_properties JSON | engine specifics — see below |
| auth_method | `DATABRICKS_SCOPE` \| `AWS_SECRETS_MANAGER` \| `ENV`(local) \| `NONE` |
| secret_scope, secret_key_user, secret_key_password | scope references (DATABRICKS_SCOPE) |
| aws_secret_arn | Secrets Manager ARN with `{username, password}` JSON |
| enabled | disabled connections refuse to resolve |

**extra_properties by type**
* SNOWFLAKE: `warehouse`, `role`, `schema`, `account`, `query_tag`
* SQLSERVER: `encrypt`, `trustServerCertificate`, `jdbc.*` passthrough
* ORACLE: `service_name`; TERADATA: `tmode`; any JDBC: `jdbc.<prop>`
* any engine: `supports_hash: "false"` → force no-hash fallback (e.g. SQL Server < 2016); DB2 z/OS v13: `supports_hash: "true"` to opt in
* FILE_CSV: `delimiter, header, encoding, quote, escape, multiline, infer_schema, schema_ddl`
* FILE_FIXED: `layout` (JSON `[{name,start,length,type}]`, 1-based), `encoding`, `trim`
* FILE_JSON: `multiline`; FILE_XML: `row_tag`; FILE_EXCEL: `sheet`, `header`, `data_address`
* FILE_COBOL: `copybook` | `copybook_contents`, `record_format` (F/V/VB), `encoding`, `code_page`, `is_record_sequence` (VSAM), `gdg: "latest"`, `cobrix.*` passthrough

## meta.validation_config (master)

| Column | Notes |
|---|---|
| validation_id PK | e.g. `VAL_SALES_CUSTOMERS` |
| execution_group | selection dimension for runs & email routing |
| source_/target_ type, connection, schema, object, query, filter | `*_query` = full SQL override (wins over schema.object); `*_filter` = WHERE fragment |
| primary_key / comparison_key | csv column lists; comparison_key defaults to primary_key |
| column_mapping JSON | `{source_col: target_col}` renames |
| exclude_columns | csv — excluded everywhere (schema compare, hash, aggregates) |
| validation_profile | `QUICK` `STANDARD` `FULL` `MIGRATION` `REGRESSION` `INCREMENTAL` `CUSTOM` |
| validation_mode | `FULL` \| `INCREMENTAL` (watermark predicates) \| `SAMPLE` |
| tolerance_type / tolerance_value | `ABSOLUTE` \| `PERCENT` — applied to counts/aggregates/custom-SQL compares |
| sampling_type / sampling_value | `RANDOM` + percent (TABLESAMPLE or predicate fallback) |
| partition_column | drives PARTITION_COUNT and (numeric) LARGE bucketing |
| partition_buckets | override bucket count (default engine.bucket_count=1024) |
| watermark_column | INCREMENTAL mode: `col > last watermark` |
| comparison_tier | `AUTO`(default) `SMALL` `MEDIUM` `LARGE` `XLARGE` |
| max_mismatch_samples | full-row samples per validation (default 100) |
| case_insensitive | UPPER() in canonicalization (CI collations) |
| parallel_flag | false ⇒ barrier: runs alone within its level |
| enabled / priority / execution_order | priority = sequential level; order within level |

## meta.validation_rules (column-specific / custom rules)

`rule_id PK, validation_id, rule_type, column_name, rule_expression, severity
(ERROR|WARNING|INFO), threshold, parameters JSON, enabled`.

* A rule row **without** column_name **replaces** the same-type profile entry.
* `severity=WARNING` failures mark the validation WARNING, not FAILED.
* `parameters.side` = `source|target` (default target) for one-sided checks.

**rule_expression by type** — DOMAIN_CHECK: csv of allowed values ·
PATTERN_CHECK: regex · UNIQUE_KEY_CHECK/MANDATORY_CHECK/FK_CHECK: csv columns
(FK needs `parameters.ref_object`, optional `ref_schema`, `ref_columns`) ·
BUSINESS_RULE: SQL predicate of the EXPECTED condition (`parameters.mode:
violation` inverts) · CUSTOM_SQL: scalar SELECT (`parameters.expected` or
`parameters.source_sql` for two-sided compare).

## meta.validation_profiles (profiles are data)

Seeded at bootstrap:

| Profile | Rule types (order) |
|---|---|
| QUICK | CONNECTION, OBJECT_EXISTS, SCHEMA_COMPARE, COLUMN_COUNT, RECORD_COUNT |
| STANDARD | QUICK + DATA_TYPE, NULL_CHECK, BLANK_CHECK, DUPLICATE_CHECK, PK_CHECK, AGGREGATE |
| FULL | STANDARD + ROW_HASH_COMPARE, MISSING_RECORDS, EXTRA_RECORDS, COLUMN_COMPARE |
| MIGRATION | FULL + LENGTH, PRECISION, SCALE, DEFAULT_VALUE_CHECK, CONSTRAINT_CHECK*, INDEX_CHECK* (*WARNING severity) |
| REGRESSION | STANDARD + BUSINESS_RULE |
| INCREMENTAL | RECORD_COUNT, ROW_HASH_COMPARE (watermark-scoped) |
| CUSTOM | rules table only |

Additional registrable types not in default profiles: `UNIQUE_KEY_CHECK`,
`FK_CHECK`, `MANDATORY_CHECK`, `DOMAIN_CHECK`, `PATTERN_CHECK`, `DATE_CHECK`,
`TIMESTAMP_CHECK`, `CHECKSUM`, `PARTITION_COUNT`, `BUSINESS_RULE`,
`CUSTOM_SQL`, `DATA_PROFILE`.

## meta.watermarks

`validation_id PK, watermark_column, last_value_str/ts/num, last_run_id,
updated_ts`. Advanced only after PASSED/WARNING runs; reset via notebook 08.

## audit.run_audit (1 row / run)

`run_id (R{ts}_{uuid8}), run_name, trigger_type (MANUAL|SCHEDULED|RERUN),
parent_run_id, triggered_by, group/id filters, profile_override, status
(RUNNING|SUCCEEDED|FAILED|PARTIAL|CANCELLED), start/end/duration, counts
(total/passed/failed/warnings/errors/skipped), config_snapshot (full effective
settings JSON — reproducibility), cluster_id, framework_version`.

## audit.validation_run_audit (1 row / attempt — checkpoint grain)

`execution_id PK, run_id, validation_id, attempt, status (…PASSED|FAILED|
WARNING|ERROR|TIMEOUT|SKIPPED), timings, source/target_row_count,
rows_compared/matched/mismatched/missing/extra, rules_total/passed/failed,
comparison_tier_used, retry_count, error_message/stack, correlation_id`.
Clustered by (run_id, validation_id).

## results.result_detail (1 row / rule outcome)

Includes `source_value, target_value, difference, difference_pct,
tolerance_applied, details JSON` and — debug gold — the exact pushdown
`source_sql` / `target_sql`.

## results.mismatch_detail (exception repository)

`mismatch_type (MISSING_IN_TARGET|EXTRA_IN_TARGET|COLUMN_MISMATCH|
DUPLICATE_KEY), key_values JSON, column_name, source_value, target_value,
source_row/target_row JSON (canonical values, 4KB cap), bucket_path
(drill-down lineage "17/3/9"), captured_ts`.

## results.profile_detail

`side (SOURCE|TARGET), column_name (NULL = table-level), metric (ROW_COUNT,
NULL_COUNT, DISTINCT_COUNT, MIN, MAX, AVG, STDDEV, SUM, MIN_LENGTH,
MAX_LENGTH, TOP_VALUES), metric_value, details JSON`.

## audit.v_execution_history (view)

run × validation × config join + `dq_score` (rules_passed×100/rules_total).
The dashboarding source of truth — never a materialized copy.

---

## Worked examples

**SQL Server → Snowflake, full reconciliation**
```python
ValidationConfig(
    validation_id="VAL_SALES_CUSTOMERS", execution_group="SALES",
    source_connection="SQLSERVER_ODS", source_schema="dbo",
    source_object="customers",
    target_connection="SNOWFLAKE_DW", target_schema="CORE",
    target_object="CUSTOMERS",
    primary_key="customer_id", validation_profile="FULL",
    case_insensitive=True)               # SQL Server CI collation
```

**Mainframe COBOL file → Snowflake (counts + hash)**
```python
# connection MAINFRAME_LANDING: FILE_COBOL with copybook + gdg='latest'
ValidationConfig(
    validation_id="VAL_MF_CUSTOMER", execution_group="MAINFRAME",
    source_connection="MAINFRAME_LANDING",
    source_object="CUSTOMER/",           # GDG dir under the base path
    target_connection="SNOWFLAKE_DW", target_schema="STAGE",
    target_object="CUSTOMER_MASTER",
    primary_key="CUST_ID", validation_profile="FULL",
    parameters_note="ts_precision via rules if DB2-sourced timestamps")
```

**Snowflake ODS → Core with 0.5% count tolerance, weekly business rule**
```python
ValidationConfig(validation_id="VAL_ODS_CORE_ORDERS", ...,
    tolerance_type="PERCENT", tolerance_value=0.5,
    validation_profile="REGRESSION")
ValidationRule(rule_id="R1", validation_id="VAL_ODS_CORE_ORDERS",
    rule_type="BUSINESS_RULE",
    rule_expression="order_total >= 0 AND status IN ('OPEN','CLOSED')")
```

**Billion-row table, incremental daily**
```python
ValidationConfig(validation_id="VAL_BIG_FACT", ...,
    validation_profile="INCREMENTAL", validation_mode="INCREMENTAL",
    watermark_column="load_ts", comparison_tier="XLARGE",
    partition_buckets=2048, priority=200)
```
