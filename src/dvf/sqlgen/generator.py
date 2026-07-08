"""SQLGenerator — renders dialect-specific pushdown SQL from Jinja2 templates.

Statement *shapes* live in templates (dvf/dialects/templates, with per-dialect
override subdirectories); atomic *expressions* come from the SQLDialect object
exposed to templates as ``d``. Templates are goldens-tested as text.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

from jinja2 import ChoiceLoader, Environment, FileSystemLoader, StrictUndefined

from dvf.config.settings import FrameworkSettings
from dvf.dialects import CanonOptions, SQLDialect

_TEMPLATE_ROOT = Path(__file__).resolve().parent.parent / "dialects" / "templates"


@dataclass(frozen=True)
class ObjectRef:
    """A validated object rendered for FROM/WHERE composition."""
    from_clause: str                 # `"schema"."table"` or `(SELECT ...) dvf_q`
    where: str | None = None         # combined predicate (no WHERE keyword)

    def where_sql(self) -> str:
        return f"\nWHERE {self.where}" if self.where else ""


class SQLGenerator:
    """Stateless renderer; one instance shared per run."""

    def __init__(self, settings: FrameworkSettings):
        self._settings = settings
        self._envs: dict[str, Environment] = {}

    # ------------------------------------------------------------------ #
    def canon_options(self, case_insensitive: bool = False,
                      decimal_scale: int | None = None,
                      ts_precision: int | None = None) -> CanonOptions:
        c = self._settings.canonical
        return CanonOptions(
            delimiter=c.delimiter,
            null_sentinel=c.null_sentinel,
            decimal_scale=decimal_scale if decimal_scale is not None else c.decimal_scale,
            ts_precision=ts_precision if ts_precision is not None else c.ts_precision,
            case_insensitive=case_insensitive,
        )

    # ------------------------------------------------------------------ #
    def object_ref(self, dialect: SQLDialect, *,
                   schema: str | None, obj: str | None, query: str | None,
                   filter_sql: str | None = None,
                   extra_predicates: Sequence[str] = (),
                   suffix: str | None = None) -> ObjectRef:
        """Build the FROM/WHERE pair. A full query override wins over schema.object.
        suffix: table-level clause (TABLESAMPLE ...) — ignored for query overrides."""
        if query:
            from_clause = f"({query.strip().rstrip(';')}) dvf_q"
        elif obj:
            from_clause = dialect.qualify(schema, obj)
            if suffix:
                from_clause = f"{from_clause} {suffix}"
        else:
            raise ValueError("object_ref needs either a query or an object name")
        predicates = [p for p in ([filter_sql] if filter_sql else []) if p]
        predicates.extend(p for p in extra_predicates if p)
        where = " AND ".join(f"({p})" for p in predicates) if predicates else None
        return ObjectRef(from_clause=from_clause, where=where)

    # ------------------------------------------------------------------ #
    def render(self, dialect: SQLDialect, template_name: str, **context: Any) -> str:
        env = self._env_for(dialect)
        sql = env.get_template(template_name).render(d=dialect, **context)
        return "\n".join(line.rstrip() for line in sql.strip().splitlines() if line.strip())

    def _env_for(self, dialect: SQLDialect) -> Environment:
        env = self._envs.get(dialect.name)
        if env is None:
            loaders = []
            override_dir = _TEMPLATE_ROOT / dialect.name
            if override_dir.is_dir():
                loaders.append(FileSystemLoader(str(override_dir)))
            loaders.append(FileSystemLoader(str(_TEMPLATE_ROOT)))
            env = Environment(
                loader=ChoiceLoader(loaders),
                undefined=StrictUndefined,
                trim_blocks=True,
                lstrip_blocks=True,
                autoescape=False,
                keep_trailing_newline=False,
            )
            self._envs[dialect.name] = env
        return env
