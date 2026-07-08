"""Audit & result domain models — rows for audit.* and results.* tables."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class RunStatus:
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    PARTIAL = "PARTIAL"
    CANCELLED = "CANCELLED"


class ValidationStatus:
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    PASSED = "PASSED"
    FAILED = "FAILED"
    WARNING = "WARNING"
    ERROR = "ERROR"
    TIMEOUT = "TIMEOUT"
    SKIPPED = "SKIPPED"


class RuleStatus:
    PASS = "PASS"
    FAIL = "FAIL"
    WARN = "WARN"
    ERROR = "ERROR"
    SKIP = "SKIP"


class MismatchTypes:
    MISSING_IN_TARGET = "MISSING_IN_TARGET"
    EXTRA_IN_TARGET = "EXTRA_IN_TARGET"
    COLUMN_MISMATCH = "COLUMN_MISMATCH"
    DUPLICATE_KEY = "DUPLICATE_KEY"


class _Model(BaseModel):
    model_config = ConfigDict(extra="ignore")


# audit.run_audit — one row per run
class RunAudit(_Model):
    run_id: str
    run_name: Optional[str] = None
    trigger_type: str = "MANUAL"
    parent_run_id: Optional[str] = None
    triggered_by: Optional[str] = None
    group_filter: Optional[str] = None
    validation_id_filter: Optional[str] = None
    profile_override: Optional[str] = None
    status: str = RunStatus.RUNNING
    start_ts: Optional[datetime] = None
    end_ts: Optional[datetime] = None
    duration_sec: Optional[float] = None
    total_validations: int = 0
    passed: int = 0
    failed: int = 0
    warnings: int = 0
    errors: int = 0
    skipped: int = 0
    config_snapshot: Optional[str] = None        # JSON of effective FrameworkSettings
    cluster_id: Optional[str] = None
    notebook_path: Optional[str] = None
    framework_version: Optional[str] = None


# audit.validation_run_audit — one row per validation attempt (checkpoint unit)
class ValidationRunAudit(_Model):
    execution_id: str
    run_id: str
    validation_id: str
    attempt: int = 1
    status: str = ValidationStatus.PENDING
    start_ts: Optional[datetime] = None
    end_ts: Optional[datetime] = None
    duration_sec: Optional[float] = None
    source_row_count: Optional[int] = None
    target_row_count: Optional[int] = None
    rows_compared: Optional[int] = None
    rows_matched: Optional[int] = None
    rows_mismatched: Optional[int] = None
    rows_missing: Optional[int] = None
    rows_extra: Optional[int] = None
    rules_total: int = 0
    rules_passed: int = 0
    rules_failed: int = 0
    comparison_tier_used: Optional[str] = None
    retry_count: int = 0
    error_message: Optional[str] = None
    error_stack: Optional[str] = None
    correlation_id: Optional[str] = None


# results.result_detail — one row per rule instance outcome
class RuleResult(_Model):
    result_id: str
    execution_id: str
    run_id: str
    validation_id: str
    rule_type: str
    rule_id: Optional[str] = None
    column_name: Optional[str] = None
    status: str = RuleStatus.PASS
    source_value: Optional[str] = None
    target_value: Optional[str] = None
    difference: Optional[str] = None
    difference_pct: Optional[float] = None
    tolerance_applied: Optional[str] = None
    details: dict = Field(default_factory=dict)  # serialized to JSON at write
    source_sql: Optional[str] = None
    target_sql: Optional[str] = None
    start_ts: Optional[datetime] = None
    end_ts: Optional[datetime] = None
    duration_sec: Optional[float] = None


# results.mismatch_detail — exception repository
class MismatchRecord(_Model):
    mismatch_id: str
    run_id: str
    execution_id: str
    validation_id: str
    mismatch_type: str
    key_values: dict = Field(default_factory=dict)   # {key_col: value}
    column_name: Optional[str] = None
    source_value: Optional[str] = None
    target_value: Optional[str] = None
    source_row: Optional[str] = None                 # JSON sample, truncated to 4KB
    target_row: Optional[str] = None
    bucket_path: Optional[str] = None                # drill-down lineage "17/342/9"
    captured_ts: Optional[datetime] = None


# results.profile_detail — data profiling metrics
class ProfileRecord(_Model):
    profile_id: str
    run_id: str
    execution_id: str
    validation_id: str
    side: str                                        # SOURCE | TARGET
    column_name: Optional[str] = None                # None for table-level metrics
    metric: str
    metric_value: Optional[str] = None
    details: dict = Field(default_factory=dict)
