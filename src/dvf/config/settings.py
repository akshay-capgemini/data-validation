"""Framework settings — pydantic models mirroring config/framework.yaml.

Precedence (later wins): framework.yaml < environments/{env}.yaml < DVF__* env vars
< explicit overrides. See loader.load_settings().
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class TierThresholds(BaseModel):
    small: int = 100_000
    medium: int = 50_000_000
    large: int = 1_000_000_000


class EngineSettings(BaseModel):
    max_parallel: int = 8
    query_timeout_s: int = 900
    retry_max: int = 2
    retry_backoff_s: int = 30
    tier_thresholds: TierThresholds = Field(default_factory=TierThresholds)
    bucket_count: int = 1024
    leaf_threshold: int = 5_000_000
    max_depth: int = 3
    small_tier_max_fetch: int = 250_000
    driver_diff_max_rows: int = 1_000_000   # beyond this, MEDIUM tier requires Spark
    max_mismatch_samples: int = 100
    in_list_batch_size: int = 500


class AuditSettings(BaseModel):
    batch_rows: int = 500
    flush_interval_s: float = 5.0


class CanonicalSettings(BaseModel):
    delimiter: str = "|~|"
    null_sentinel: str = "<NULL>"
    decimal_scale: int = 6
    ts_precision: int = 6


class EmailSettings(BaseModel):
    enabled: bool = False
    method: str = "ses"                      # ses | smtp
    region: str = "us-east-1"
    from_address: str = "dq-noreply@example.com"
    smtp_host: Optional[str] = None
    smtp_port: int = 25
    recipients: list[str] = Field(default_factory=list)
    recipients_by_group: dict[str, list[str]] = Field(default_factory=dict)


class ReportingSettings(BaseModel):
    formats: list[str] = Field(default_factory=lambda: ["html", "excel", "json"])
    output_path: str = "./dvf_reports"
    pdf_enabled: bool = False
    email: EmailSettings = Field(default_factory=EmailSettings)


class LoggingSettings(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    level: str = "INFO"
    json_format: bool = Field(default=True, alias="json")


class RetentionSettings(BaseModel):
    audit_days: int = 180
    results_days: int = 90
    reports_days: int = 90


class FrameworkSettings(BaseModel):
    """Effective framework configuration for a run."""

    model_config = ConfigDict(extra="ignore")

    environment: str = "local"
    catalog: str = "dvf_local"               # UC catalog (prod) / logical prefix (sqlite)
    secret_scope_default: str = ""
    engine: EngineSettings = Field(default_factory=EngineSettings)
    audit: AuditSettings = Field(default_factory=AuditSettings)
    canonical: CanonicalSettings = Field(default_factory=CanonicalSettings)
    reporting: ReportingSettings = Field(default_factory=ReportingSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    retention: RetentionSettings = Field(default_factory=RetentionSettings)

    def snapshot_json(self) -> str:
        """Serialized effective settings for run_audit.config_snapshot."""
        return self.model_dump_json()
