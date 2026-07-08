"""Test harness helpers — validator runner, config factory, flaky connector."""

from __future__ import annotations

from typing import ClassVar

from dvf.connectors.sqlite import SqliteConnector
from dvf.core.exceptions import ConnectionFailedError
from dvf.metadata.models import ConnectionDef, RuleInstance, ValidationConfig
from dvf.sqlgen.generator import SQLGenerator
from dvf.validators import ValidatorRegistry
from dvf.validators.base import ValidationContext


def make_config(**over) -> ValidationConfig:
    base = dict(validation_id="VAL_T1", source_connection="SRC",
                source_object="customers", target_connection="TGT",
                target_object="customers", validation_profile="QUICK",
                primary_key="id")
    return ValidationConfig(**{**base, **over})


class CapturingWriter:
    """Duck-typed AuditWriter capturing appended rows for assertions."""

    def __init__(self):
        self.rows: list[tuple[str, str, dict]] = []

    def append(self, schema: str, table: str, row) -> None:
        data = row.model_dump() if hasattr(row, "model_dump") else dict(row)
        self.rows.append((schema, table, data))

    def merge(self, schema: str, table: str, row) -> None:
        self.append(schema, table, row)

    def table_rows(self, table: str) -> list[dict]:
        return [r for _, t, r in self.rows if t == table]


def run_validator(settings, rule_type: str, src_path: str, tgt_path: str,
                  config: ValidationConfig | None = None,
                  rule: RuleInstance | None = None,
                  parameters: dict | None = None,
                  src_name: str = "SRC", tgt_name: str = "TGT",
                  src_props: dict | None = None, tgt_props: dict | None = None):
    """Execute one validator against two sqlite files; returns (results, ctx).
    Emitted mismatch/profile rows land in ctx.writer (a CapturingWriter)."""
    config = config or make_config()
    rule = rule or RuleInstance(rule_type=rule_type, parameters=parameters or {})
    src = SqliteConnector(ConnectionDef(connection_name=src_name,
                                        connection_type="SQLITE",
                                        database_name=src_path,
                                        extra_properties=src_props or {}))
    tgt = SqliteConnector(ConnectionDef(connection_name=tgt_name,
                                        connection_type="SQLITE",
                                        database_name=tgt_path,
                                        extra_properties=tgt_props or {}))
    src.connect(), tgt.connect()
    try:
        ctx = ValidationContext(
            config=config, rule=rule, source=src, target=tgt,
            sqlgen=SQLGenerator(settings), settings=settings,
            run_id="R_TEST", execution_id="E_TEST", writer=CapturingWriter())
        return ValidatorRegistry.create(rule_type, ctx).execute(), ctx
    finally:
        src.disconnect(), tgt.disconnect()


class FlakySqliteConnector(SqliteConnector):
    """Raises a transient ConnectionFailedError the first N connect() calls
    per connection_name — proves the engine's transient retry path."""

    fail_times: ClassVar[int] = 1
    attempts: ClassVar[dict[str, int]] = {}

    @classmethod
    def reset(cls, fail_times: int = 1) -> None:
        cls.fail_times = fail_times
        cls.attempts = {}

    def connect(self) -> None:
        name = self.conn.connection_name
        count = self.attempts.get(name, 0) + 1
        self.attempts[name] = count
        if count <= self.fail_times:
            raise ConnectionFailedError(
                f"flaky connector simulated outage (attempt {count})",
                connection_name=name)
        super().connect()
