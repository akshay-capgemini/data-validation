"""Comparison engine — SMALL (cross + same-connection EXCEPT) and MEDIUM tiers
against planted diffs. Expectations from seed_reconciliation_pair:
missing=2 (ids 4,9), extra=2 (101,102), mismatched=2 (id 2 amount, id 7 name),
matched=6.
"""

from __future__ import annotations

import pytest

from dvf.audit.models import MismatchTypes, RuleStatus
from dvf.comparison.differ import diff_rows_driver
from fixtures.harness import make_config, run_validator
from fixtures.seeddata import seed_reconciliation_pair, seed_same_db_pair


@pytest.fixture
def recon_pair(tmp_path):
    src, tgt = str(tmp_path / "rs.db"), str(tmp_path / "rt.db")
    seed_reconciliation_pair(src, tgt, n=10)
    return src, tgt


def _assert_outcome(outcome: dict, tier: str):
    assert outcome["tier"] == tier
    assert outcome["rows_missing"] == 2
    assert outcome["rows_extra"] == 2
    assert outcome["rows_mismatched"] == 2
    assert outcome["rows_matched"] == 6


def _mismatch_rows(ctx):
    return ctx.writer.table_rows("mismatch_detail")


# --------------------------------------------------------------------------- #
def test_differ_unit():
    src = [("1", "h1"), ("2", "h2"), ("3", "h3")]
    tgt = [("1", "h1"), ("2", "hX"), ("4", "h4")]
    result, smap, tmap = diff_rows_driver(src, tgt, key_len=1, sample_cap=10)
    assert (result.matched, result.mismatched, result.missing, result.extra) == \
        (1, 1, 1, 1)
    assert result.mismatched_keys == [("2",)]
    assert result.missing_keys == [("3",)]
    assert result.extra_keys == [("4",)]
    assert smap[("2",)] == ("h2",) and tmap[("2",)] == ("hX",)


# --------------------------------------------------------------------------- #
def test_small_cross_engine(settings, recon_pair):
    results, ctx = run_validator(settings, "ROW_HASH_COMPARE", *recon_pair)
    r = results[0]
    assert r.status == RuleStatus.FAIL
    outcome = ctx.shared["comparison_outcome"]
    _assert_outcome(outcome, "SMALL")

    rows = _mismatch_rows(ctx)
    by_type: dict[str, list[dict]] = {}
    for m in rows:
        by_type.setdefault(m["mismatch_type"], []).append(m)
    missing_ids = {m["key_values"]["id"] for m in
                   by_type[MismatchTypes.MISSING_IN_TARGET]}
    extra_ids = {m["key_values"]["id"] for m in
                 by_type[MismatchTypes.EXTRA_IN_TARGET]}
    assert missing_ids == {"4", "9"}
    assert extra_ids == {"101", "102"}
    col_mismatch = by_type[MismatchTypes.COLUMN_MISMATCH]
    assert {(m["key_values"]["id"], m["column_name"]) for m in col_mismatch} == \
        {("2", "amount"), ("7", "name")}
    amount_row = next(m for m in col_mismatch if m["column_name"] == "amount")
    assert amount_row["source_value"] == "200.500000"
    assert amount_row["target_value"] == "250.500000"
    # exact column attribution in SMALL tier
    assert outcome["column_mismatch_counts"] == {"amount": 1, "name": 1}


def test_small_same_connection_except(settings, tmp_path):
    db = str(tmp_path / "same.db")
    seed_same_db_pair(db, n=10)
    cfg = make_config(source_connection="BOTH", target_connection="BOTH",
                      target_object="customers_tgt")
    results, ctx = run_validator(settings, "ROW_HASH_COMPARE", db, db,
                                 config=cfg, src_name="BOTH", tgt_name="BOTH")
    assert results[0].status == RuleStatus.FAIL
    outcome = ctx.shared["comparison_outcome"]
    _assert_outcome(outcome, "SMALL")
    assert "EXCEPT" in outcome["source_sql"]      # genuinely pushed down
    assert outcome["column_mismatch_counts"] == {"amount": 1, "name": 1}


def test_medium_tier_hash_compare(settings, recon_pair):
    cfg = make_config(comparison_tier="MEDIUM")
    results, ctx = run_validator(settings, "ROW_HASH_COMPARE", *recon_pair,
                                 config=cfg)
    assert results[0].status == RuleStatus.FAIL
    outcome = ctx.shared["comparison_outcome"]
    _assert_outcome(outcome, "MEDIUM")
    assert "row_hash" in outcome["source_sql"]

    rows = _mismatch_rows(ctx)
    col_rows = [m for m in rows
                if m["mismatch_type"] == MismatchTypes.COLUMN_MISMATCH
                and m["column_name"]]
    # sample fetch re-reads full rows by key and attributes exact columns
    assert {(m["key_values"]["id"], m["column_name"]) for m in col_rows} == \
        {("2", "amount"), ("7", "name")}
    assert all(m["source_row"] for m in col_rows)


def test_outcome_cached_across_rule_types(settings, recon_pair):
    results, ctx = run_validator(settings, "MISSING_RECORDS", *recon_pair)
    assert results[0].status == RuleStatus.FAIL
    assert results[0].target_value == "2"
    first = ctx.shared["comparison_outcome"]

    # second rule type on the SAME ctx must not recompute
    from dvf.validators import ValidatorRegistry
    from dvf.metadata.models import RuleInstance
    ctx.rule = RuleInstance(rule_type="EXTRA_RECORDS")
    validator = ValidatorRegistry.create("EXTRA_RECORDS", ctx)
    extra_results = validator.execute()
    assert extra_results[0].target_value == "2"
    assert ctx.shared["comparison_outcome"] is first


def test_column_compare_reports_offending_columns(settings, recon_pair):
    results, _ = run_validator(settings, "COLUMN_COMPARE", *recon_pair)
    summary = results[0]
    assert summary.status == RuleStatus.FAIL
    per_col = {r.column_name for r in results[1:]}
    assert per_col == {"amount", "name"}


def test_identical_pair_passes_all_reconciliation(settings, tmp_path):
    from fixtures.seeddata import seed_identical_pair
    src, tgt = str(tmp_path / "is.db"), str(tmp_path / "it.db")
    seed_identical_pair(src, tgt, n=10)
    for rule_type in ("ROW_HASH_COMPARE", "MISSING_RECORDS", "EXTRA_RECORDS",
                      "COLUMN_COMPARE"):
        results, _ = run_validator(settings, rule_type, src, tgt)
        assert results[0].status == RuleStatus.PASS, rule_type


def test_missing_comparison_key_errors_cleanly(settings, recon_pair):
    from dvf.core.exceptions import ComparisonError
    cfg = make_config(primary_key=None, comparison_key=None)
    with pytest.raises(ComparisonError, match="comparison_key"):
        run_validator(settings, "ROW_HASH_COMPARE", *recon_pair, config=cfg)
