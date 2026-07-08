"""ComparisonPlan — unified cross-engine column metadata for one validation.

Both sides must render byte-identical canonical strings, so per-column metadata
(decimal scale) is unified across the pair before any SQL is generated.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from dvf.core.exceptions import ComparisonError
from dvf.dialects import CanonOptions, TypeCat
from dvf.metadata.models import ColumnDef


@dataclass(frozen=True)
class ColumnPair:
    source: ColumnDef
    target: ColumnDef

    @property
    def name(self) -> str:                       # canonical (source) name
        return self.source.name


@dataclass
class ComparisonPlan:
    key_pairs: list[ColumnPair]
    value_pairs: list[ColumnPair]                # non-key shared columns
    canon: CanonOptions

    @property
    def key_names(self) -> list[str]:
        return [p.name for p in self.key_pairs]

    @property
    def value_names(self) -> list[str]:
        return [p.name for p in self.value_pairs]


def build_plan(validator) -> ComparisonPlan:
    """validator: the BaseValidator driving the comparison (gives config,
    shared-column resolution, sqlgen, settings)."""
    config = validator.config
    keys = [k.lower() for k in config.comparison_key_columns]
    if not keys:
        raise ComparisonError(
            f"Validation '{config.validation_id}' needs primary_key or "
            "comparison_key for record-level comparison")

    pairs = validator.shared_columns()
    if not pairs:
        raise ComparisonError(
            f"Validation '{config.validation_id}' has no shared columns to compare")

    src_dialect = validator.source.dialect
    key_pairs: list[ColumnPair] = []
    value_pairs: list[ColumnPair] = []
    seen_keys: set[str] = set()
    for s_col, t_col in pairs:
        s_col, t_col = _unify(s_col, t_col, src_dialect)
        pair = ColumnPair(s_col, t_col)
        if s_col.name.lower() in keys:
            key_pairs.append(pair)
            seen_keys.add(s_col.name.lower())
        else:
            value_pairs.append(pair)

    missing = [k for k in keys if k not in seen_keys]
    if missing:
        raise ComparisonError(
            f"Validation '{config.validation_id}': comparison key column(s) "
            f"{missing} not present in both sides (after mapping/excludes)")

    # keys keep configured order
    key_pairs.sort(key=lambda p: keys.index(p.name.lower()))

    ts_precision = validator.param("ts_precision")
    decimal_scale = validator.param("canon_scale")
    canon = validator.sqlgen.canon_options(
        case_insensitive=config.case_insensitive,
        decimal_scale=int(decimal_scale) if decimal_scale is not None else None,
        ts_precision=int(ts_precision) if ts_precision is not None else None)
    return ComparisonPlan(key_pairs=key_pairs, value_pairs=value_pairs, canon=canon)


def _unify(s_col: ColumnDef, t_col: ColumnDef, dialect) -> tuple[ColumnDef, ColumnDef]:
    """Unify decimal scale across the pair so both render at the same width."""
    if dialect.type_category(s_col.native_type) != TypeCat.DECIMAL:
        return s_col, t_col
    unified = max(s_col.scale or 0, t_col.scale or 0)
    if (s_col.scale or 0) == unified and (t_col.scale or 0) == unified:
        return s_col, t_col
    if unified == 0:
        return s_col, t_col
    return (s_col.model_copy(update={"scale": unified}),
            t_col.model_copy(update={"scale": unified}))
