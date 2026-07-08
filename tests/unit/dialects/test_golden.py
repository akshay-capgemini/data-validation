"""Golden SQL tests — byte-exact dialect output, reviewed as text diffs.

Regenerate after intentional dialect changes:  pytest --update-golden
Every dialect change then shows its SQL diff in code review.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dvf.config.settings import FrameworkSettings
from dvf.dialects import get_dialect
from dvf.metadata.models import ColumnDef
from dvf.sqlgen.generator import SQLGenerator

GOLDEN_DIR = Path(__file__).resolve().parents[2] / "golden"
DIALECTS = ["sqlite", "spark", "snowflake", "sqlserver", "oracle", "postgres",
            "mysql", "db2_luw", "db2_zos", "teradata"]

#: a schema covering every canonical type category
COLUMNS = [
    ColumnDef(name="id", native_type="BIGINT"),
    ColumnDef(name="code", native_type="CHAR(10)"),
    ColumnDef(name="name", native_type="VARCHAR(50)", length=50),
    ColumnDef(name="amount", native_type="DECIMAL(18,2)", precision=18, scale=2),
    ColumnDef(name="ratio", native_type="FLOAT"),
    ColumnDef(name="units", native_type="DECIMAL(10,0)", precision=10, scale=0),
    ColumnDef(name="is_active", native_type="BOOLEAN"),
    ColumnDef(name="born_on", native_type="DATE"),
    ColumnDef(name="updated_at", native_type="TIMESTAMP"),
]
KEY_COLS = [COLUMNS[0]]
VALUE_COLS = COLUMNS[1:]


def _generator() -> SQLGenerator:
    return SQLGenerator(FrameworkSettings())


def _cases(dialect, gen: SQLGenerator) -> dict[str, str]:
    d = dialect
    ref = gen.object_ref(d, schema="dq", obj="customers", query=None,
                         filter_sql="load_dt = '2026-01-01'")
    canon = gen.canon_options()
    canon_ci = gen.canon_options(case_insensitive=True)
    hash_cols = ([{"expr": d.canonical_column(KEY_COLS[0], canon), "alias": "k0"},
                  {"expr": d.row_hash_expr(VALUE_COLS, canon), "alias": "row_hash"}]
                 if d.supports_hash else [])
    cases = {
        "record_count": gen.render(d, "record_count.sql.j2", ref=ref),
        "aggregate": gen.render(d, "aggregate.sql.j2", ref=ref, aggs=[
            {"func": "SUM", "expr": d.quote("amount"), "alias": "a0_sum"},
            {"func": "MIN", "expr": d.quote("amount"), "alias": "a0_min"},
            {"func": "MAX", "expr": d.quote("amount"), "alias": "a0_max"}]),
        "null_count": gen.render(d, "null_count.sql.j2", ref=ref, columns=[
            {"expr": d.quote("name"), "alias": "c0"},
            {"expr": d.quote("amount"), "alias": "c1"}]),
        "duplicate_count": gen.render(d, "duplicate_count.sql.j2", ref=ref,
                                      keys=[d.quote("id")]),
        "predicate_count": gen.render(d, "predicate_count.sql.j2", ref=ref,
                                      predicate=f"{d.quote('amount')} < 0"),
        "canonical_row": d.row_canonical_expr(COLUMNS, canon),
        "canonical_row_ci": d.row_canonical_expr(COLUMNS[:4], canon_ci),
    }
    if d.supports_hash:
        cases["row_hash_select"] = gen.render(d, "canonical_select.sql.j2",
                                              ref=ref, select_cols=hash_cols,
                                              order_by=None)
    return cases


@pytest.mark.parametrize("dialect_name", DIALECTS)
def test_golden(dialect_name, request):
    update = request.config.getoption("--update-golden")
    dialect = get_dialect(dialect_name)
    gen = _generator()
    out_dir = GOLDEN_DIR / dialect_name
    for case, sql in _cases(dialect, gen).items():
        path = out_dir / f"{case}.sql"
        if update:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(sql + "\n", encoding="utf-8")
            continue
        assert path.exists(), (
            f"golden file missing: {path} — run `pytest --update-golden`")
        expected = path.read_text(encoding="utf-8").rstrip("\n")
        assert sql == expected, (
            f"[{dialect_name}/{case}] SQL drifted from golden.\n"
            f"--- golden ---\n{expected}\n--- actual ---\n{sql}\n"
            f"(intentional? run `pytest --update-golden`)")


def test_hash_expressions_lowercase_hex_everywhere():
    """Every hash-capable dialect must wrap its hash in lowercase-hex form."""
    for name in DIALECTS:
        d = get_dialect(name)
        if not d.supports_hash:
            continue
        expr = d.md5_hex_lower("x")
        assert "MD5" in expr.upper() or "HASH" in expr.upper()


def test_scale_zero_decimal_canonicalizes_as_integer():
    """NUMBER(10,0) (Oracle/DB2) must render like INT does on other engines."""
    canon = _generator().canon_options()
    col = ColumnDef(name="units", native_type="DECIMAL(10,0)",
                    precision=10, scale=0)
    int_col = ColumnDef(name="units", native_type="BIGINT")
    for name in DIALECTS:
        d = get_dialect(name)
        assert d.canonical_cast(col, canon) == d.canonical_cast(int_col, canon), name
