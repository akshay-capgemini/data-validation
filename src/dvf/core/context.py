"""RunContext — everything a validation task needs, wired once per run."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from dvf.audit.writer import AuditWriter
from dvf.config.settings import FrameworkSettings
from dvf.connectors.factory import ConnectorFactory
from dvf.metadata.backend import StorageBackend
from dvf.metadata.repository import MetadataRepository
from dvf.sqlgen.generator import SQLGenerator


@dataclass
class RunContext:
    run_id: str
    settings: FrameworkSettings
    backend: StorageBackend
    repo: MetadataRepository
    writer: AuditWriter
    factory: ConnectorFactory
    sqlgen: SQLGenerator
    spark: Any = None
    profile_override: str | None = None
    skip_validation_ids: set[str] = field(default_factory=set)
