"""LARGE/XLARGE drill-down — bucket signatures find every planted diff, with
tiny buckets/leaves forcing real recursion. Plus the no-hash fallbacks."""

from __future__ import annotations

import pytest

from dvf.audit.models import RuleStatus
from dvf.dialects.sqlite import SqliteDialect
from fixtures.harness import make_config, run_validator
from fixtures.seeddata import (
    LARGE_PAIR_EXPECT, seed_large_pair, seed_reconciliation_pair,
)

NOHASH = {"supports_hash": "false"}


@pytest.fixture
def large_pair(tmp_path):
    src, tgt = str(tmp_path / "ls.db"), str(tmp_path / "lt.db")
    seed_large_pair(src, tgt, n=200)
    return src, tgt


def _drill_settings(settings, leaf=10, depth=3):
    settings.engine.leaf_threshold = leaf
    settings.engine.max_depth = depth
    return settings


def _assert_counts(outcome):
    assert outcome["rows_missing"] == LARGE_PAIR_EXPECT["missing"]
    assert outcome["rows_extra"] == LARGE_PAIR_EXPECT["extra"]
    assert outcome["rows_mismatched"] == LARGE_PAIR_EXPECT["mismatched"]
    assert outcome["rows_matched"] == LARGE_PAIR_EXPECT["matched"]


def test_large_tier_single_level(settings, large_pair):
    cfg = make_config(comparison_tier="LARGE", partition_buckets=4)
    results, ctx = run_validator(_drill_settings(settings), "ROW_HASH_COMPARE",
                                 *large_pair, config=cfg)
    assert results[0].status == RuleStatus.FAIL
    outcome = ctx.shared["comparison_outcome"]
    assert outcome["tier"] == "LARGE"
    _assert_counts(outcome)
    assert "bucket_id" in outcome["source_sql"]

    mismatch_rows = ctx.writer.table_rows("mismatch_detail")
    assert mismatch_rows, "samples must be emitted"
    # LARGE = depth 0 only: single-segment bucket paths
    assert all(m["bucket_path"] is not None and "/" not in m["bucket_path"]
               for m in mismatch_rows)


def test_xlarge_recursion_with_paths(settings, large_pair):
    cfg = make_config(comparison_tier="XLARGE", partition_buckets=4)
    results, ctx = run_validator(_drill_settings(settings, leaf=10, depth=3),
                                 "ROW_HASH_COMPARE", *large_pair, config=cfg)
    assert results[0].status == RuleStatus.FAIL
    outcome = ctx.shared["comparison_outcome"]
    assert outcome["tier"] == "XLARGE"
    _assert_counts(outcome)

    mismatch_rows = ctx.writer.table_rows("mismatch_detail")
    # 200 rows / 4 buckets = 50 > leaf(10) -> forced multi-level drill-down
    assert any("/" in (m["bucket_path"] or "") for m in mismatch_rows)
    # every planted key surfaced with column attribution for modified rows
    col_pairs = {(m["key_values"]["id"], m["column_name"])
                 for m in mismatch_rows
                 if m["mismatch_type"] == "COLUMN_MISMATCH" and m["column_name"]}
    assert col_pairs == {("26", "amount"), ("90", "name"),
                         ("154", "email"), ("198", "amount")}


def test_xlarge_aggregate_signatures_no_slices(settings, large_pair, monkeypatch):
    """DB2/Teradata path: no hash-slice sums -> typed aggregate signatures."""
    monkeypatch.setattr(SqliteDialect, "supports_hash_slices", False)
    cfg = make_config(comparison_tier="XLARGE", partition_buckets=4)
    results, ctx = run_validator(_drill_settings(settings), "ROW_HASH_COMPARE",
                                 *large_pair, config=cfg)
    assert results[0].status == RuleStatus.FAIL
    outcome = ctx.shared["comparison_outcome"]
    _assert_counts(outcome)
    assert "SUM(CAST" in outcome["source_sql"]    # aggregate signatures used


def test_medium_no_hash_fallback_client_side(settings, tmp_path):
    """z/OS/Teradata path: canonical strings fetched, hashed client-side."""
    src, tgt = str(tmp_path / "ns.db"), str(tmp_path / "nt.db")
    seed_reconciliation_pair(src, tgt, n=10)
    cfg = make_config(comparison_tier="MEDIUM")
    results, ctx = run_validator(settings, "ROW_HASH_COMPARE", src, tgt,
                                 config=cfg, src_props=NOHASH, tgt_props=NOHASH)
    assert results[0].status == RuleStatus.FAIL
    outcome = ctx.shared["comparison_outcome"]
    assert outcome["rows_missing"] == 2
    assert outcome["rows_extra"] == 2
    assert outcome["rows_mismatched"] == 2
    # no dvf_md5() pushdown — the canonical concat itself is fetched
    assert "dvf_md5" not in outcome["source_sql"]
    assert "COALESCE" in outcome["source_sql"]


def test_drilldown_nonnumeric_key_without_hash_errors(settings, large_pair,
                                                      monkeypatch):
    from dvf.core.exceptions import ComparisonError
    cfg = make_config(comparison_tier="LARGE", primary_key="email",
                      partition_buckets=4)
    with pytest.raises(ComparisonError, match="numeric"):
        run_validator(_drill_settings(settings), "ROW_HASH_COMPARE", *large_pair,
                      config=cfg, src_props=NOHASH, tgt_props=NOHASH)


def test_checksum_validator(settings, tmp_path):
    from fixtures.seeddata import seed_identical_pair
    src, tgt = str(tmp_path / "cs.db"), str(tmp_path / "ct.db")
    seed_identical_pair(src, tgt, n=10)
    results, _ = run_validator(settings, "CHECKSUM", src, tgt)
    assert results[0].status == RuleStatus.PASS

    src2, tgt2 = str(tmp_path / "cs2.db"), str(tmp_path / "ct2.db")
    seed_reconciliation_pair(src2, tgt2, n=10)
    results, _ = run_validator(settings, "CHECKSUM", src2, tgt2)
    assert results[0].status == RuleStatus.FAIL
    assert "dvf_hex_to_int" in results[0].source_sql

    # no-slice dialects skip with guidance
    results, _ = run_validator(settings, "CHECKSUM", src, tgt,
                               src_props=NOHASH, tgt_props=NOHASH)
    assert results[0].status == RuleStatus.SKIP


def test_partition_count_validator(settings, tmp_path):
    src, tgt = str(tmp_path / "ps.db"), str(tmp_path / "pt.db")
    seed_reconciliation_pair(src, tgt, n=10)
    cfg = make_config(partition_column="created_ts")
    results, _ = run_validator(settings, "PARTITION_COUNT", src, tgt, config=cfg)
    summary = results[0]
    assert summary.status == RuleStatus.FAIL
    issues = summary.details["issues"]
    only_src = {i["partition"] for i in issues if i.get("issue") == "only in source"}
    only_tgt = [i for i in issues if i.get("issue") == "only in target"]
    assert len(only_src) == 2                     # ids 4 & 9 each own a ts partition
    assert len(only_tgt) == 1                     # 101+102 share '2026-01-02 00:00:00'
    assert only_tgt[0]["target"] == 2

    cfg = make_config(partition_column="is_active")       # balanced partitions
    results, _ = run_validator(settings, "PARTITION_COUNT", src, tgt, config=cfg)
    assert results[0].status == RuleStatus.PASS
