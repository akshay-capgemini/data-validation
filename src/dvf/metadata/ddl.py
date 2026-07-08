"""Logical DDL — single source of truth for every framework table.

Tables are declared with *logical* column types; each StorageBackend renders
them to its physical types (Delta vs sqlite). Bootstrap is idempotent:
CREATE IF NOT EXISTS + profile-seed MERGE.

Logical types: STRING | INT | BIGINT | DOUBLE | BOOLEAN | TIMESTAMP | JSON
(JSON = dict persisted as a JSON string — portable across engines).
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Column:
    name: str
    type: str


@dataclass(frozen=True)
class Table:
    schema: str                      # meta | audit | results
    name: str
    columns: tuple[Column, ...]
    primary_key: tuple[str, ...] = ()
    cluster_by: tuple[str, ...] = ()

    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]


def _c(name: str, type_: str = "STRING") -> Column:
    return Column(name, type_)


_AUDIT_COLS = (_c("created_ts", "TIMESTAMP"), _c("created_by"),
               _c("updated_ts", "TIMESTAMP"), _c("updated_by"))

CONNECTIONS = Table(
    "meta", "connections",
    ( _c("connection_name"), _c("connection_type"), _c("host"), _c("port", "INT"),
      _c("database_name"), _c("extra_properties", "JSON"), _c("auth_method"),
      _c("secret_scope"), _c("secret_key_user"), _c("secret_key_password"),
      _c("aws_secret_arn"), _c("enabled", "BOOLEAN"), *_AUDIT_COLS ),
    primary_key=("connection_name",))

VALIDATION_CONFIG = Table(
    "meta", "validation_config",
    ( _c("validation_id"), _c("execution_group"), _c("description"),
      _c("source_type"), _c("source_connection"), _c("source_schema"),
      _c("source_object"), _c("source_query"), _c("source_filter"),
      _c("target_type"), _c("target_connection"), _c("target_schema"),
      _c("target_object"), _c("target_query"), _c("target_filter"),
      _c("primary_key"), _c("comparison_key"), _c("column_mapping", "JSON"),
      _c("exclude_columns"), _c("validation_profile"), _c("validation_mode"),
      _c("tolerance_type"), _c("tolerance_value", "DOUBLE"),
      _c("sampling_type"), _c("sampling_value", "DOUBLE"),
      _c("partition_column"), _c("partition_buckets", "INT"), _c("watermark_column"),
      _c("comparison_tier"), _c("max_mismatch_samples", "INT"),
      _c("case_insensitive", "BOOLEAN"), _c("parallel_flag", "BOOLEAN"),
      _c("enabled", "BOOLEAN"), _c("priority", "INT"), _c("execution_order", "INT"),
      *_AUDIT_COLS ),
    primary_key=("validation_id",))

VALIDATION_RULES = Table(
    "meta", "validation_rules",
    ( _c("rule_id"), _c("validation_id"), _c("rule_type"), _c("column_name"),
      _c("rule_expression"), _c("severity"), _c("threshold", "DOUBLE"),
      _c("parameters", "JSON"), _c("enabled", "BOOLEAN"),
      _c("created_ts", "TIMESTAMP"), _c("updated_ts", "TIMESTAMP") ),
    primary_key=("rule_id",))

VALIDATION_PROFILES = Table(
    "meta", "validation_profiles",
    ( _c("profile_name"), _c("rule_type"), _c("rule_order", "INT"),
      _c("default_parameters", "JSON"), _c("enabled", "BOOLEAN") ),
    primary_key=("profile_name", "rule_type"))

WATERMARKS = Table(
    "meta", "watermarks",
    ( _c("validation_id"), _c("watermark_column"), _c("last_value_str"),
      _c("last_value_ts", "TIMESTAMP"), _c("last_value_num", "DOUBLE"),
      _c("last_run_id"), _c("updated_ts", "TIMESTAMP") ),
    primary_key=("validation_id",))

RUN_AUDIT = Table(
    "audit", "run_audit",
    ( _c("run_id"), _c("run_name"), _c("trigger_type"), _c("parent_run_id"),
      _c("triggered_by"), _c("group_filter"), _c("validation_id_filter"),
      _c("profile_override"), _c("status"), _c("start_ts", "TIMESTAMP"),
      _c("end_ts", "TIMESTAMP"), _c("duration_sec", "DOUBLE"),
      _c("total_validations", "INT"), _c("passed", "INT"), _c("failed", "INT"),
      _c("warnings", "INT"), _c("errors", "INT"), _c("skipped", "INT"),
      _c("config_snapshot"), _c("cluster_id"), _c("notebook_path"),
      _c("framework_version") ),
    primary_key=("run_id",))

VALIDATION_RUN_AUDIT = Table(
    "audit", "validation_run_audit",
    ( _c("execution_id"), _c("run_id"), _c("validation_id"), _c("attempt", "INT"),
      _c("status"), _c("start_ts", "TIMESTAMP"), _c("end_ts", "TIMESTAMP"),
      _c("duration_sec", "DOUBLE"), _c("source_row_count", "BIGINT"),
      _c("target_row_count", "BIGINT"), _c("rows_compared", "BIGINT"),
      _c("rows_matched", "BIGINT"), _c("rows_mismatched", "BIGINT"),
      _c("rows_missing", "BIGINT"), _c("rows_extra", "BIGINT"),
      _c("rules_total", "INT"), _c("rules_passed", "INT"), _c("rules_failed", "INT"),
      _c("comparison_tier_used"), _c("retry_count", "INT"),
      _c("error_message"), _c("error_stack"), _c("correlation_id") ),
    primary_key=("execution_id",),
    cluster_by=("run_id", "validation_id"))

RESULT_DETAIL = Table(
    "results", "result_detail",
    ( _c("result_id"), _c("execution_id"), _c("run_id"), _c("validation_id"),
      _c("rule_type"), _c("rule_id"), _c("column_name"), _c("status"),
      _c("source_value"), _c("target_value"), _c("difference"),
      _c("difference_pct", "DOUBLE"), _c("tolerance_applied"), _c("details", "JSON"),
      _c("source_sql"), _c("target_sql"), _c("start_ts", "TIMESTAMP"),
      _c("end_ts", "TIMESTAMP"), _c("duration_sec", "DOUBLE") ),
    primary_key=("result_id",),
    cluster_by=("run_id", "validation_id"))

MISMATCH_DETAIL = Table(
    "results", "mismatch_detail",
    ( _c("mismatch_id"), _c("run_id"), _c("execution_id"), _c("validation_id"),
      _c("mismatch_type"), _c("key_values", "JSON"), _c("column_name"),
      _c("source_value"), _c("target_value"), _c("source_row"), _c("target_row"),
      _c("bucket_path"), _c("captured_ts", "TIMESTAMP") ),
    primary_key=("mismatch_id",),
    cluster_by=("run_id", "validation_id"))

PROFILE_DETAIL = Table(
    "results", "profile_detail",
    ( _c("profile_id"), _c("run_id"), _c("execution_id"), _c("validation_id"),
      _c("side"), _c("column_name"), _c("metric"), _c("metric_value"),
      _c("details", "JSON") ),
    primary_key=("profile_id",),
    cluster_by=("run_id", "validation_id"))

ALL_TABLES: tuple[Table, ...] = (
    CONNECTIONS, VALIDATION_CONFIG, VALIDATION_RULES, VALIDATION_PROFILES,
    WATERMARKS, RUN_AUDIT, VALIDATION_RUN_AUDIT, RESULT_DETAIL, MISMATCH_DETAIL,
    PROFILE_DETAIL,
)


def get_table(schema: str, name: str) -> Table:
    for t in ALL_TABLES:
        if t.schema == schema and t.name == name:
            return t
    raise KeyError(f"Unknown framework table {schema}.{name}")


# --------------------------------------------------------------------------- #
# Validation rule-type canon (registry keys) + profile seeds (profiles are DATA)
# --------------------------------------------------------------------------- #
class RuleTypes:
    CONNECTION = "CONNECTION"
    OBJECT_EXISTS = "OBJECT_EXISTS"
    SCHEMA_COMPARE = "SCHEMA_COMPARE"
    COLUMN_COUNT = "COLUMN_COUNT"
    RECORD_COUNT = "RECORD_COUNT"
    DATA_TYPE = "DATA_TYPE"
    LENGTH = "LENGTH"
    PRECISION = "PRECISION"
    SCALE = "SCALE"
    NULL_CHECK = "NULL_CHECK"
    BLANK_CHECK = "BLANK_CHECK"
    DUPLICATE_CHECK = "DUPLICATE_CHECK"
    PK_CHECK = "PK_CHECK"
    UNIQUE_KEY_CHECK = "UNIQUE_KEY_CHECK"
    FK_CHECK = "FK_CHECK"
    MANDATORY_CHECK = "MANDATORY_CHECK"
    DEFAULT_VALUE_CHECK = "DEFAULT_VALUE_CHECK"
    DOMAIN_CHECK = "DOMAIN_CHECK"
    PATTERN_CHECK = "PATTERN_CHECK"
    DATE_CHECK = "DATE_CHECK"
    TIMESTAMP_CHECK = "TIMESTAMP_CHECK"
    AGGREGATE = "AGGREGATE"
    CHECKSUM = "CHECKSUM"
    PARTITION_COUNT = "PARTITION_COUNT"
    ROW_HASH_COMPARE = "ROW_HASH_COMPARE"
    MISSING_RECORDS = "MISSING_RECORDS"
    EXTRA_RECORDS = "EXTRA_RECORDS"
    COLUMN_COMPARE = "COLUMN_COMPARE"
    BUSINESS_RULE = "BUSINESS_RULE"
    CONSTRAINT_CHECK = "CONSTRAINT_CHECK"
    INDEX_CHECK = "INDEX_CHECK"
    DATA_PROFILE = "DATA_PROFILE"
    CUSTOM_SQL = "CUSTOM_SQL"


_QUICK = [
    (RuleTypes.CONNECTION, 10), (RuleTypes.OBJECT_EXISTS, 20),
    (RuleTypes.SCHEMA_COMPARE, 30), (RuleTypes.COLUMN_COUNT, 40),
    (RuleTypes.RECORD_COUNT, 50),
]
_STANDARD = _QUICK + [
    (RuleTypes.DATA_TYPE, 60), (RuleTypes.NULL_CHECK, 70),
    (RuleTypes.BLANK_CHECK, 75), (RuleTypes.DUPLICATE_CHECK, 80),
    (RuleTypes.PK_CHECK, 85), (RuleTypes.AGGREGATE, 90),
]
_FULL = _STANDARD + [
    (RuleTypes.ROW_HASH_COMPARE, 100), (RuleTypes.MISSING_RECORDS, 101),
    (RuleTypes.EXTRA_RECORDS, 102), (RuleTypes.COLUMN_COMPARE, 103),
]
#: (rule_type, order, default_parameters) — parameters may carry a severity override
_MIGRATION = _FULL + [
    (RuleTypes.LENGTH, 61), (RuleTypes.PRECISION, 62), (RuleTypes.SCALE, 63),
    (RuleTypes.DEFAULT_VALUE_CHECK, 64), (RuleTypes.CONSTRAINT_CHECK, 65),
    (RuleTypes.INDEX_CHECK, 66),
]
#: advisory checks in MIGRATION downgrade to WARNING severity
_SEVERITY_OVERRIDES: dict[tuple[str, str], dict] = {
    ("MIGRATION", RuleTypes.DEFAULT_VALUE_CHECK): {"severity": "WARNING"},
    ("MIGRATION", RuleTypes.CONSTRAINT_CHECK): {"severity": "WARNING"},
    ("MIGRATION", RuleTypes.INDEX_CHECK): {"severity": "WARNING"},
}
_REGRESSION = _STANDARD + [(RuleTypes.BUSINESS_RULE, 110)]
_INCREMENTAL = [(RuleTypes.RECORD_COUNT, 50), (RuleTypes.ROW_HASH_COMPARE, 100)]

#: rows for meta.validation_profiles (CUSTOM has no seeds — rules table only)
PROFILE_SEEDS: list[dict] = [
    {"profile_name": profile, "rule_type": rt, "rule_order": order,
     "default_parameters": _SEVERITY_OVERRIDES.get((profile, rt), {}),
     "enabled": True}
    for profile, entries in (
        ("QUICK", _QUICK), ("STANDARD", _STANDARD), ("FULL", _FULL),
        ("MIGRATION", _MIGRATION), ("REGRESSION", _REGRESSION),
        ("INCREMENTAL", _INCREMENTAL),
    )
    for rt, order in entries
]


# --------------------------------------------------------------------------- #
# Execution-history view (rendered by backends with physical table names)
# --------------------------------------------------------------------------- #
EXECUTION_HISTORY_VIEW = "v_execution_history"

EXECUTION_HISTORY_SQL = """
SELECT
    r.run_id, r.run_name, r.trigger_type, r.parent_run_id, r.triggered_by,
    r.status AS run_status, r.start_ts AS run_start_ts, r.end_ts AS run_end_ts,
    r.duration_sec AS run_duration_sec,
    v.execution_id, v.validation_id, v.attempt, v.status AS validation_status,
    v.start_ts, v.end_ts, v.duration_sec,
    v.source_row_count, v.target_row_count, v.rows_compared, v.rows_matched,
    v.rows_mismatched, v.rows_missing, v.rows_extra,
    v.rules_total, v.rules_passed, v.rules_failed,
    v.comparison_tier_used, v.retry_count, v.error_message,
    c.execution_group, c.validation_profile, c.description,
    c.source_type, c.source_connection, c.source_schema, c.source_object,
    c.target_type, c.target_connection, c.target_schema, c.target_object,
    CASE WHEN v.rules_total > 0
         THEN v.rules_passed * 100.0 / v.rules_total
         ELSE NULL END AS dq_score
FROM {validation_run_audit} v
JOIN {run_audit} r ON r.run_id = v.run_id
LEFT JOIN {validation_config} c ON c.validation_id = v.validation_id
"""
