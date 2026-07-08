"""ObjectDiscovery — propose validation_config rows from live schemas.

Scans a source schema, matches objects on the target (name mapping hook),
detects primary keys, and emits DISABLED ValidationConfig candidates for human
review before activation. Discovery never overwrites an existing validation_id.
"""

from __future__ import annotations

from typing import Callable

from dvf.audit.logger import get_logger
from dvf.connectors.base import BaseConnector
from dvf.metadata.models import Profiles, ValidationConfig
from dvf.metadata.repository import MetadataRepository

_log = get_logger("metadata.discovery")


class ObjectDiscovery:
    def __init__(self, repo: MetadataRepository, factory):
        self.repo = repo
        self.factory = factory

    def discover(self, *,
                 source_connection: str, source_schema: str | None,
                 target_connection: str, target_schema: str | None,
                 execution_group: str,
                 id_prefix: str = "VAL",
                 profile: str = Profiles.STANDARD,
                 name_map: Callable[[str], str] | None = None,
                 include: Callable[[str], bool] | None = None,
                 ) -> list[ValidationConfig]:
        """Returns proposed configs (enabled=False). Objects missing on the
        target are skipped with a log line; existing validation_ids are skipped."""
        name_map = name_map or (lambda n: n)
        include = include or (lambda n: True)

        source = self.factory.create(self.repo.get_connection(source_connection))
        target = self.factory.create(self.repo.get_connection(target_connection))
        source.connect(), target.connect()
        try:
            existing = {c.validation_id for c in
                        self.repo.load_validation_configs(enabled_only=False)}
            proposals: list[ValidationConfig] = []
            objects = [o for o in source.list_objects(source_schema) if include(o)]
            _log.info("discovery: %d candidate object(s) in %s.%s",
                      len(objects), source_connection, source_schema or "")
            for obj in objects:
                target_obj = name_map(obj)
                if not target.table_exists(target_schema, target_obj):
                    _log.warning("discovery: %s missing on target as %s — skipped",
                                 obj, target_obj)
                    continue
                validation_id = f"{id_prefix}_{execution_group}_{obj}".upper()
                if validation_id in existing:
                    _log.info("discovery: %s already registered — skipped",
                              validation_id)
                    continue
                pk = source.get_primary_keys(source_schema, obj)
                proposals.append(ValidationConfig(
                    validation_id=validation_id,
                    execution_group=execution_group,
                    description=f"discovered: {obj} -> {target_obj}",
                    source_type=source.conn.connection_type,
                    source_connection=source_connection,
                    source_schema=source_schema,
                    source_object=obj,
                    target_type=target.conn.connection_type,
                    target_connection=target_connection,
                    target_schema=target_schema,
                    target_object=target_obj,
                    primary_key=",".join(pk) if pk else None,
                    validation_profile=profile,
                    enabled=False,               # human review before activation
                ))
            return proposals
        finally:
            source.disconnect(), target.disconnect()

    def register(self, proposals: list[ValidationConfig],
                 actor: str | None = None) -> int:
        if proposals:
            self.repo.upsert_validation_configs(proposals, actor=actor)
        return len(proposals)
