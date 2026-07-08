"""MetadataRepository — typed access to meta.* tables over a StorageBackend."""

from __future__ import annotations

from typing import Iterable, Sequence

from dvf.core.exceptions import MetadataError
from dvf.metadata.backend import StorageBackend
from dvf.metadata.models import (
    ConnectionDef, ProfileEntry, ValidationConfig, ValidationRule, Watermark,
)
from dvf.utils.timeutil import utcnow


class MetadataRepository:
    def __init__(self, backend: StorageBackend):
        self.backend = backend

    # ------------------------------------------------------------------ #
    # Connections
    # ------------------------------------------------------------------ #
    def get_connection(self, name: str) -> ConnectionDef:
        rows = self.backend.read_rows("meta", "connections",
                                      where_eq={"connection_name": name})
        if not rows:
            raise MetadataError(f"Connection '{name}' is not registered")
        conn = ConnectionDef.from_row(rows[0])
        if not conn.enabled:
            raise MetadataError(f"Connection '{name}' is disabled")
        return conn

    def list_connections(self) -> list[ConnectionDef]:
        return [ConnectionDef.from_row(r)
                for r in self.backend.read_rows("meta", "connections")]

    def upsert_connections(self, conns: Iterable[ConnectionDef],
                           actor: str | None = None) -> None:
        now = utcnow()
        rows = []
        for c in conns:
            row = c.model_dump()
            row.setdefault("created_ts", now)
            row["created_by"] = row.get("created_by") or actor
            row["updated_ts"] = now
            row["updated_by"] = actor
            rows.append(row)
        self.backend.merge_rows("meta", "connections", rows)

    # ------------------------------------------------------------------ #
    # Validation configs
    # ------------------------------------------------------------------ #
    def load_validation_configs(self, groups: Sequence[str] | None = None,
                                validation_ids: Sequence[str] | None = None,
                                enabled_only: bool = True) -> list[ValidationConfig]:
        where_eq = {"enabled": True} if enabled_only else None
        where_in = {}
        if groups:
            where_in["execution_group"] = list(groups)
        if validation_ids:
            where_in["validation_id"] = list(validation_ids)
        rows = self.backend.read_rows("meta", "validation_config",
                                      where_eq=where_eq, where_in=where_in or None)
        configs = [ValidationConfig.from_row(r) for r in rows]
        configs.sort(key=lambda c: (c.priority, c.execution_order, c.validation_id))
        return configs

    def upsert_validation_configs(self, configs: Iterable[ValidationConfig],
                                  actor: str | None = None) -> None:
        now = utcnow()
        rows = []
        for c in configs:
            row = c.model_dump()
            row.setdefault("created_ts", now)
            row["created_by"] = row.get("created_by") or actor
            row["updated_ts"] = now
            row["updated_by"] = actor
            rows.append(row)
        self.backend.merge_rows("meta", "validation_config", rows)

    # ------------------------------------------------------------------ #
    # Rules & profiles
    # ------------------------------------------------------------------ #
    def get_rules(self, validation_id: str, enabled_only: bool = True) -> list[ValidationRule]:
        where_eq: dict = {"validation_id": validation_id}
        if enabled_only:
            where_eq["enabled"] = True
        rows = self.backend.read_rows("meta", "validation_rules", where_eq=where_eq)
        return [ValidationRule.from_row(r) for r in rows]

    def upsert_rules(self, rules: Iterable[ValidationRule]) -> None:
        now = utcnow()
        rows = []
        for r in rules:
            row = r.model_dump()
            row.setdefault("created_ts", now)
            row["updated_ts"] = now
            rows.append(row)
        self.backend.merge_rows("meta", "validation_rules", rows)

    def get_profile(self, profile_name: str) -> list[ProfileEntry]:
        rows = self.backend.read_rows("meta", "validation_profiles",
                                      where_eq={"profile_name": profile_name.upper(),
                                                "enabled": True})
        entries = [ProfileEntry.from_row(r) for r in rows]
        entries.sort(key=lambda e: e.rule_order)
        return entries

    # ------------------------------------------------------------------ #
    # Watermarks
    # ------------------------------------------------------------------ #
    def get_watermark(self, validation_id: str) -> Watermark | None:
        rows = self.backend.read_rows("meta", "watermarks",
                                      where_eq={"validation_id": validation_id})
        return Watermark.from_row(rows[0]) if rows else None

    def save_watermark(self, watermark: Watermark) -> None:
        row = watermark.model_dump()
        row["updated_ts"] = utcnow()
        self.backend.merge_rows("meta", "watermarks", [row])
