"""Phase-1 structural validators against seeded sqlite pairs."""

from __future__ import annotations

import pytest

from dvf.audit.models import RuleStatus
from dvf.validators.base import ValidationContext
from fixtures.harness import make_config, run_validator
from fixtures.seeddata import (
    seed_count_mismatch_pair, seed_identical_pair, seed_schema_drift_pair,
)


@pytest.fixture
def identical_pair(tmp_path):
    src, tgt = str(tmp_path / "s.db"), str(tmp_path / "t.db")
    seed_identical_pair(src, tgt, n=10)
    return src, tgt


# --------------------------------------------------------------------------- #
def test_connection_pass(settings, identical_pair):
    results, _ = run_validator(settings, "CONNECTION", *identical_pair)
    assert [r.status for r in results] == [RuleStatus.PASS]


def test_object_exists_pass_and_fail(settings, identical_pair, tmp_path):
    results, _ = run_validator(settings, "OBJECT_EXISTS", *identical_pair)
    assert results[0].status == RuleStatus.PASS

    cfg = make_config(target_object="no_such_table")
    results, _ = run_validator(settings, "OBJECT_EXISTS", *identical_pair, config=cfg)
    assert results[0].status == RuleStatus.FAIL
    assert results[0].target_value == "MISSING"


def test_schema_compare_identical(settings, identical_pair):
    results, _ = run_validator(settings, "SCHEMA_COMPARE", *identical_pair)
    assert results[0].status == RuleStatus.PASS


def test_schema_compare_drift(settings, tmp_path):
    src, tgt = str(tmp_path / "s2.db"), str(tmp_path / "t2.db")
    seed_schema_drift_pair(src, tgt)
    results, _ = run_validator(settings, "SCHEMA_COMPARE", src, tgt)
    r = results[0]
    assert r.status == RuleStatus.FAIL
    assert r.details["missing_in_target"] == ["email"]
    assert r.details["extra_in_target"] == ["load_dt"]


def test_schema_compare_respects_excludes_and_mapping(settings, tmp_path):
    src, tgt = str(tmp_path / "s3.db"), str(tmp_path / "t3.db")
    seed_schema_drift_pair(src, tgt)
    cfg = make_config(exclude_columns="email,load_dt")
    results, _ = run_validator(settings, "SCHEMA_COMPARE", src, tgt, config=cfg)
    assert results[0].status == RuleStatus.PASS


def test_column_count(settings, tmp_path):
    src, tgt = str(tmp_path / "s4.db"), str(tmp_path / "t4.db")
    seed_schema_drift_pair(src, tgt)
    results, _ = run_validator(settings, "COLUMN_COUNT", src, tgt)
    assert results[0].status == RuleStatus.PASS   # 6 == 6 (drift swaps a column)

    cfg = make_config(exclude_columns="load_dt")  # now 6 vs 5
    results, _ = run_validator(settings, "COLUMN_COUNT", src, tgt, config=cfg)
    assert results[0].status == RuleStatus.FAIL


def test_record_count_pass_and_shared_cache(settings, identical_pair):
    results, ctx = run_validator(settings, "RECORD_COUNT", *identical_pair)
    r = results[0]
    assert r.status == RuleStatus.PASS
    assert r.source_value == "10" and r.target_value == "10"
    assert ctx.shared[ValidationContext.KEY_SOURCE_COUNT] == 10
    assert "SELECT COUNT(*)" in r.source_sql


def test_record_count_fail_with_difference(settings, tmp_path):
    src, tgt = str(tmp_path / "s5.db"), str(tmp_path / "t5.db")
    seed_count_mismatch_pair(src, tgt, n=10, missing_in_target=2)
    results, _ = run_validator(settings, "RECORD_COUNT", src, tgt)
    r = results[0]
    assert r.status == RuleStatus.FAIL
    assert (r.source_value, r.target_value, r.difference) == ("10", "8", "-2")
    assert r.difference_pct == pytest.approx(-20.0)


def test_record_count_tolerance_percent(settings, tmp_path):
    src, tgt = str(tmp_path / "s6.db"), str(tmp_path / "t6.db")
    seed_count_mismatch_pair(src, tgt, n=10, missing_in_target=2)
    cfg = make_config(tolerance_type="PERCENT", tolerance_value=25.0)
    results, _ = run_validator(settings, "RECORD_COUNT", src, tgt, config=cfg)
    assert results[0].status == RuleStatus.PASS
    assert results[0].tolerance_applied == "PERCENT 25.0"


def test_record_count_with_filter(settings, identical_pair):
    cfg = make_config(source_filter="is_active = 1", target_filter="is_active = 1")
    results, _ = run_validator(settings, "RECORD_COUNT", *identical_pair, config=cfg)
    assert results[0].status == RuleStatus.PASS
    assert results[0].source_value == "5"
    assert "WHERE" in results[0].source_sql
