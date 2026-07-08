"""Metadata domain models — the typed contracts for every metadata table row.

Field names match the Delta/sqlite column names exactly so ``model_dump()``
round-trips through the storage backends without mapping code.
Connection/validation *types* are plain strings (validated at factory/registry
time) so new connectors and validators never require model edits.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


# --------------------------------------------------------------------------- #
# Canonical string constants (not enums — extensible without model changes)
# --------------------------------------------------------------------------- #
class ConnectionTypes:
    SQLSERVER = "SQLSERVER"
    DB2_LUW = "DB2_LUW"
    DB2_ZOS = "DB2_ZOS"
    ORACLE = "ORACLE"
    POSTGRES = "POSTGRES"
    MYSQL = "MYSQL"
    TERADATA = "TERADATA"
    SNOWFLAKE = "SNOWFLAKE"
    DELTA = "DELTA"          # Unity Catalog / metastore tables or delta paths via Spark SQL
    SQLITE = "SQLITE"        # test/demo only — not for production use
    FILE_CSV = "FILE_CSV"
    FILE_FIXED = "FILE_FIXED"
    FILE_PARQUET = "FILE_PARQUET"
    FILE_DELTA = "FILE_DELTA"
    FILE_JSON = "FILE_JSON"
    FILE_XML = "FILE_XML"
    FILE_EXCEL = "FILE_EXCEL"
    FILE_COBOL = "FILE_COBOL"


class AuthMethods:
    DATABRICKS_SCOPE = "DATABRICKS_SCOPE"
    AWS_SECRETS_MANAGER = "AWS_SECRETS_MANAGER"
    ENV = "ENV"              # local/dev only: DVF_SECRET_{SCOPE}_{KEY} env vars
    NONE = "NONE"


class Profiles:
    QUICK = "QUICK"
    STANDARD = "STANDARD"
    FULL = "FULL"
    MIGRATION = "MIGRATION"
    REGRESSION = "REGRESSION"
    INCREMENTAL = "INCREMENTAL"
    CUSTOM = "CUSTOM"


class ValidationModes:
    FULL = "FULL"
    INCREMENTAL = "INCREMENTAL"
    SAMPLE = "SAMPLE"


class ComparisonTiers:
    AUTO = "AUTO"
    SMALL = "SMALL"
    MEDIUM = "MEDIUM"
    LARGE = "LARGE"
    XLARGE = "XLARGE"


class ToleranceTypes:
    ABSOLUTE = "ABSOLUTE"
    PERCENT = "PERCENT"


class Severity:
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"


def _parse_json_field(value: Any) -> dict:
    """Accept dict or JSON string (storage backends persist dicts as JSON)."""
    if value is None or value == "":
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        return json.loads(value)
    raise ValueError(f"expected dict or JSON string, got {type(value)!r}")


class _Model(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    @classmethod
    def from_row(cls, row):
        """Hydrate from a storage row: NULL columns fall back to field defaults."""
        return cls(**{k: v for k, v in dict(row).items() if v is not None})


# --------------------------------------------------------------------------- #
# meta.connections
# --------------------------------------------------------------------------- #
class ConnectionDef(_Model):
    connection_name: str
    connection_type: str
    host: Optional[str] = None
    port: Optional[int] = None
    database_name: Optional[str] = None          # db/catalog; S3 base path for FILE_*
    extra_properties: dict = Field(default_factory=dict)
    auth_method: str = AuthMethods.NONE
    secret_scope: Optional[str] = None
    secret_key_user: Optional[str] = None
    secret_key_password: Optional[str] = None
    aws_secret_arn: Optional[str] = None
    enabled: bool = True
    created_ts: Optional[datetime] = None
    created_by: Optional[str] = None
    updated_ts: Optional[datetime] = None
    updated_by: Optional[str] = None

    _parse_extra = field_validator("extra_properties", mode="before")(_parse_json_field)

    def prop(self, key: str, default: str | None = None) -> str | None:
        return self.extra_properties.get(key, default)


# --------------------------------------------------------------------------- #
# meta.validation_config (master)
# --------------------------------------------------------------------------- #
class ValidationConfig(_Model):
    validation_id: str
    execution_group: Optional[str] = None
    description: Optional[str] = None

    source_type: Optional[str] = None            # denormalized for readability
    source_connection: str
    source_schema: Optional[str] = None
    source_object: Optional[str] = None
    source_query: Optional[str] = None           # full override; wins over schema.object
    source_filter: Optional[str] = None          # WHERE fragment

    target_type: Optional[str] = None
    target_connection: str
    target_schema: Optional[str] = None
    target_object: Optional[str] = None
    target_query: Optional[str] = None
    target_filter: Optional[str] = None

    primary_key: Optional[str] = None            # csv column list
    comparison_key: Optional[str] = None         # csv; None -> primary_key
    column_mapping: dict = Field(default_factory=dict)   # {src_col: tgt_col}
    exclude_columns: Optional[str] = None        # csv

    validation_profile: str = Profiles.STANDARD
    validation_mode: str = ValidationModes.FULL
    tolerance_type: Optional[str] = None         # ABSOLUTE | PERCENT
    tolerance_value: Optional[float] = None
    sampling_type: Optional[str] = None          # NONE | RANDOM | TOP
    sampling_value: Optional[float] = None
    partition_column: Optional[str] = None
    partition_buckets: Optional[int] = None
    watermark_column: Optional[str] = None
    comparison_tier: str = ComparisonTiers.AUTO
    max_mismatch_samples: Optional[int] = None
    case_insensitive: bool = False
    parallel_flag: bool = True
    enabled: bool = True
    priority: int = 100
    execution_order: int = 100

    created_ts: Optional[datetime] = None
    created_by: Optional[str] = None
    updated_ts: Optional[datetime] = None
    updated_by: Optional[str] = None

    _parse_mapping = field_validator("column_mapping", mode="before")(_parse_json_field)

    # ---- convenience accessors ------------------------------------------- #
    @property
    def primary_key_columns(self) -> list[str]:
        return _csv(self.primary_key)

    @property
    def comparison_key_columns(self) -> list[str]:
        return _csv(self.comparison_key) or self.primary_key_columns

    @property
    def excluded_columns(self) -> list[str]:
        return _csv(self.exclude_columns)

    def map_column(self, source_col: str) -> str:
        """Source column name -> target column name (identity when unmapped)."""
        return self.column_mapping.get(source_col, source_col)


def _csv(value: str | None) -> list[str]:
    return [c.strip() for c in value.split(",") if c.strip()] if value else []


# --------------------------------------------------------------------------- #
# meta.validation_rules
# --------------------------------------------------------------------------- #
class ValidationRule(_Model):
    rule_id: str
    validation_id: str
    rule_type: str
    column_name: Optional[str] = None
    rule_expression: Optional[str] = None        # predicate | regex | domain csv | custom SQL
    severity: str = Severity.ERROR
    threshold: Optional[float] = None
    parameters: dict = Field(default_factory=dict)
    enabled: bool = True
    created_ts: Optional[datetime] = None
    updated_ts: Optional[datetime] = None

    _parse_params = field_validator("parameters", mode="before")(_parse_json_field)


# --------------------------------------------------------------------------- #
# meta.validation_profiles (profiles are data, not code)
# --------------------------------------------------------------------------- #
class ProfileEntry(_Model):
    profile_name: str
    rule_type: str
    rule_order: int = 100
    default_parameters: dict = Field(default_factory=dict)
    enabled: bool = True

    _parse_params = field_validator("default_parameters", mode="before")(_parse_json_field)


# --------------------------------------------------------------------------- #
# meta.watermarks
# --------------------------------------------------------------------------- #
class Watermark(_Model):
    validation_id: str
    watermark_column: Optional[str] = None
    last_value_str: Optional[str] = None
    last_value_ts: Optional[datetime] = None
    last_value_num: Optional[float] = None
    last_run_id: Optional[str] = None
    updated_ts: Optional[datetime] = None


# --------------------------------------------------------------------------- #
# Expanded execution unit (produced by ProfileExpander — not persisted)
# --------------------------------------------------------------------------- #
class RuleInstance(_Model):
    rule_type: str
    rule_id: Optional[str] = None                # set when sourced from validation_rules
    column_name: Optional[str] = None
    rule_expression: Optional[str] = None
    severity: str = Severity.ERROR
    threshold: Optional[float] = None
    parameters: dict = Field(default_factory=dict)
    rule_order: int = 100


# --------------------------------------------------------------------------- #
# Connector introspection results
# --------------------------------------------------------------------------- #
class ColumnDef(_Model):
    name: str
    native_type: str
    length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    nullable: Optional[bool] = None
    position: Optional[int] = None
    default_value: Optional[str] = None


class IndexDef(_Model):
    name: str
    columns: list[str] = Field(default_factory=list)
    unique: bool = False


class ConstraintDef(_Model):
    name: str
    constraint_type: str                          # PRIMARY KEY | UNIQUE | FOREIGN KEY | CHECK
    columns: list[str] = Field(default_factory=list)
    definition: Optional[str] = None


class TableStats(_Model):
    row_estimate: Optional[int] = None
    size_bytes: Optional[int] = None


# --------------------------------------------------------------------------- #
# Run request (engine entrypoint)
# --------------------------------------------------------------------------- #
class RunRequest(_Model):
    run_name: Optional[str] = None
    trigger_type: str = "MANUAL"                  # MANUAL | SCHEDULED | RERUN
    triggered_by: Optional[str] = None
    execution_groups: list[str] = Field(default_factory=list)
    validation_ids: list[str] = Field(default_factory=list)
    profile_override: Optional[str] = None
    parent_run_id: Optional[str] = None
    resume_run_id: Optional[str] = None           # resume: skip already-PASSED validations
