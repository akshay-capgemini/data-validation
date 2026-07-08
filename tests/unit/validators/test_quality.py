"""Quality/business validators against the defect-seeded sqlite pair.

Planted defects (see seed_quality_defect_pair): email NULL x2, blank name x1,
duplicate email, amount drift +50 on id 2, is_active=2 on id 8,
'invalid-email' on id 9.
"""

from __future__ import annotations

import sqlite3

import pytest

from dvf.audit.models import RuleStatus
from dvf.metadata.models import RuleInstance
from fixtures.harness import make_config, run_validator
from fixtures.seeddata import (
    seed_identical_pair, seed_orders, seed_quality_defect_pair,
    seed_type_drift_pair,
)


@pytest.fixture
def defect_pair(tmp_path):
    src, tgt = str(tmp_path / "qs.db"), str(tmp_path / "qt.db")
    seed_quality_defect_pair(src, tgt, n=10)
    return src, tgt


def _summary(results):
    return results[0]


# ---------------------------------------------------------------------- #
# NULL / BLANK parity
# ---------------------------------------------------------------------- #
def test_null_check_parity_finds_email(settings, defect_pair):
    results, _ = run_validator(settings, "NULL_CHECK", *defect_pair)
    assert _summary(results).status == RuleStatus.FAIL
    mismatched = {m["column"]: m for m in
                  _summary(results).details["mismatched_columns"]}
    assert mismatched["email"]["source"] == 0
    assert mismatched["email"]["target"] == 2
    per_col = [r for r in results[1:] if r.column_name == "email"]
    assert per_col and per_col[0].difference == "2"


def test_null_check_threshold_mode(settings, defect_pair):
    rule = RuleInstance(rule_type="NULL_CHECK", column_name="email", threshold=2)
    results, _ = run_validator(settings, "NULL_CHECK", *defect_pair, rule=rule)
    assert results[0].status == RuleStatus.PASS
    rule = RuleInstance(rule_type="NULL_CHECK", column_name="email", threshold=1)
    results, _ = run_validator(settings, "NULL_CHECK", *defect_pair, rule=rule)
    assert results[0].status == RuleStatus.FAIL


def test_blank_check_finds_name(settings, defect_pair):
    results, _ = run_validator(settings, "BLANK_CHECK", *defect_pair)
    assert _summary(results).status == RuleStatus.FAIL
    mismatched = {m["column"] for m in
                  _summary(results).details["mismatched_columns"]}
    assert mismatched == {"name"}


# ---------------------------------------------------------------------- #
# Keys & duplicates
# ---------------------------------------------------------------------- #
def test_duplicate_check_on_pk_passes(settings, defect_pair):
    results, _ = run_validator(settings, "DUPLICATE_CHECK", *defect_pair)
    assert results[0].status == RuleStatus.PASS


def test_duplicate_check_on_email_fails(settings, defect_pair):
    rule = RuleInstance(rule_type="DUPLICATE_CHECK", rule_expression="email")
    results, _ = run_validator(settings, "DUPLICATE_CHECK", *defect_pair, rule=rule)
    r = results[0]
    assert r.status == RuleStatus.FAIL
    # 2 duplicate groups on target: the planted email pair (ids 6,7) AND the
    # two NULL emails (ids 3,5) — GROUP BY treats NULLs as one group.
    assert r.details["target_duplicate_keys"] == 2
    assert r.details["source_duplicate_keys"] == 0


def test_pk_check_both_sides(settings, defect_pair):
    results, _ = run_validator(settings, "PK_CHECK", *defect_pair)
    assert len(results) == 2
    assert all(r.status == RuleStatus.PASS for r in results)


def test_unique_key_check_target(settings, defect_pair):
    rule = RuleInstance(rule_type="UNIQUE_KEY_CHECK", rule_expression="email")
    results, _ = run_validator(settings, "UNIQUE_KEY_CHECK", *defect_pair, rule=rule)
    assert results[0].status == RuleStatus.FAIL
    rule = RuleInstance(rule_type="UNIQUE_KEY_CHECK", rule_expression="email",
                        parameters={"side": "source"})
    results, _ = run_validator(settings, "UNIQUE_KEY_CHECK", *defect_pair, rule=rule)
    assert results[0].status == RuleStatus.PASS


def test_fk_check_finds_orphan(settings, defect_pair, tmp_path):
    src, tgt = defect_pair
    seed_orders(tgt, with_orphan=True)
    cfg = make_config(target_object="orders")
    rule = RuleInstance(rule_type="FK_CHECK", rule_expression="customer_id",
                        parameters={"ref_object": "customers", "ref_columns": "id"})
    results, _ = run_validator(settings, "FK_CHECK", src, tgt, config=cfg, rule=rule)
    r = results[0]
    assert r.status == RuleStatus.FAIL
    assert r.target_value == "1"                  # exactly the planted orphan


def test_mandatory_check(settings, defect_pair):
    rule = RuleInstance(rule_type="MANDATORY_CHECK", rule_expression="email,name")
    results, _ = run_validator(settings, "MANDATORY_CHECK", *defect_pair, rule=rule)
    assert results[0].status == RuleStatus.FAIL
    assert results[0].details["violations"] == {"email": 2, "name": 1}


# ---------------------------------------------------------------------- #
# Domain / pattern / dates
# ---------------------------------------------------------------------- #
def test_domain_check(settings, defect_pair):
    rule = RuleInstance(rule_type="DOMAIN_CHECK", column_name="is_active",
                        rule_expression="0,1")
    results, _ = run_validator(settings, "DOMAIN_CHECK", *defect_pair, rule=rule)
    assert results[0].status == RuleStatus.FAIL   # is_active=2 on id 8
    assert results[0].target_value == "1"


def test_pattern_check(settings, defect_pair):
    rule = RuleInstance(rule_type="PATTERN_CHECK", column_name="email",
                        rule_expression=r"^[^@]+@[^@]+\.[a-z]+$")
    results, _ = run_validator(settings, "PATTERN_CHECK", *defect_pair, rule=rule)
    assert results[0].status == RuleStatus.FAIL   # 'invalid-email' on id 9
    assert results[0].target_value == "1"


def test_timestamp_check_parity_and_bounds(settings, tmp_path):
    src, tgt = str(tmp_path / "ts.db"), str(tmp_path / "tt.db")
    seed_identical_pair(src, tgt, n=5)
    results, _ = run_validator(settings, "TIMESTAMP_CHECK", src, tgt)
    assert all(r.status == RuleStatus.PASS for r in results)

    rule = RuleInstance(rule_type="TIMESTAMP_CHECK", column_name="created_ts",
                        parameters={"max": "2026-01-01 08:32:00"})
    results, _ = run_validator(settings, "TIMESTAMP_CHECK", src, tgt, rule=rule)
    assert results[0].status == RuleStatus.FAIL   # ids 3..5 exceed the bound


# ---------------------------------------------------------------------- #
# Aggregates
# ---------------------------------------------------------------------- #
def test_aggregate_finds_sum_drift(settings, defect_pair):
    rule = RuleInstance(rule_type="AGGREGATE", parameters={"functions": "SUM"})
    results, _ = run_validator(settings, "AGGREGATE", *defect_pair, rule=rule)
    summary = results[0]
    assert summary.status == RuleStatus.FAIL
    drifted = [r for r in results[1:] if r.column_name == "amount"]
    assert drifted and drifted[0].difference == "SUM differs"


def test_aggregate_within_tolerance(settings, defect_pair):
    cfg = make_config(tolerance_type="ABSOLUTE", tolerance_value=100.0)
    rule = RuleInstance(rule_type="AGGREGATE", parameters={"functions": "SUM"})
    results, _ = run_validator(settings, "AGGREGATE", *defect_pair,
                               config=cfg, rule=rule)
    assert results[0].status == RuleStatus.PASS


# ---------------------------------------------------------------------- #
# Schema attribute validators
# ---------------------------------------------------------------------- #
def test_data_type_and_length_drift(settings, tmp_path):
    src, tgt = str(tmp_path / "td.db"), str(tmp_path / "td2.db")
    seed_type_drift_pair(src, tgt)

    results, _ = run_validator(settings, "DATA_TYPE", src, tgt)
    issues = {i["column"] for i in results[0].details["issues"]}
    assert results[0].status == RuleStatus.FAIL
    assert "amount" in issues                     # DECIMAL -> STRING

    results, _ = run_validator(settings, "LENGTH", src, tgt)
    issues = {i["column"] for i in results[0].details["issues"]}
    assert results[0].status == RuleStatus.FAIL
    assert "name" in issues                       # 50 -> 30


def test_precision_scale_pass_on_identical(settings, tmp_path):
    src, tgt = str(tmp_path / "ps.db"), str(tmp_path / "pt.db")
    seed_identical_pair(src, tgt, n=3)
    for rule_type in ("PRECISION", "SCALE", "DEFAULT_VALUE_CHECK",
                      "CONSTRAINT_CHECK"):
        results, _ = run_validator(settings, rule_type, src, tgt)
        assert results[0].status == RuleStatus.PASS, rule_type


def test_index_check_missing_in_target(settings, tmp_path):
    src, tgt = str(tmp_path / "ix.db"), str(tmp_path / "iy.db")
    seed_identical_pair(src, tgt, n=3)
    db = sqlite3.connect(src)
    db.execute("CREATE INDEX idx_email ON customers(email)")
    db.commit(), db.close()
    results, _ = run_validator(settings, "INDEX_CHECK", src, tgt)
    assert results[0].status == RuleStatus.FAIL
    assert results[0].details["missing_in_target"][0]["columns"] == ["email"]


# ---------------------------------------------------------------------- #
# Business rule / custom SQL
# ---------------------------------------------------------------------- #
def test_business_rule_expect_mode(settings, defect_pair):
    rule = RuleInstance(rule_type="BUSINESS_RULE", rule_expression="amount < 1000")
    results, _ = run_validator(settings, "BUSINESS_RULE", *defect_pair, rule=rule)
    assert results[0].status == RuleStatus.FAIL   # id 10 amount 1000.5
    assert results[0].target_value == "1"

    rule = RuleInstance(rule_type="BUSINESS_RULE", rule_expression="amount < 1000",
                        threshold=1)
    results, _ = run_validator(settings, "BUSINESS_RULE", *defect_pair, rule=rule)
    assert results[0].status == RuleStatus.PASS


def test_custom_sql_expected_and_compare(settings, defect_pair):
    rule = RuleInstance(rule_type="CUSTOM_SQL",
                        rule_expression="SELECT COUNT(*) FROM customers",
                        parameters={"expected": "10"})
    results, _ = run_validator(settings, "CUSTOM_SQL", *defect_pair, rule=rule)
    assert results[0].status == RuleStatus.PASS

    rule = RuleInstance(
        rule_type="CUSTOM_SQL",
        rule_expression="SELECT SUM(amount) FROM customers",
        parameters={"source_sql": "SELECT SUM(amount) FROM customers"})
    results, _ = run_validator(settings, "CUSTOM_SQL", *defect_pair, rule=rule)
    assert results[0].status == RuleStatus.FAIL   # +50 drift

    cfg = make_config(tolerance_type="ABSOLUTE", tolerance_value=60.0)
    results, _ = run_validator(settings, "CUSTOM_SQL", *defect_pair,
                               config=cfg, rule=rule)
    assert results[0].status == RuleStatus.PASS


def test_warning_severity_produces_warn(settings, defect_pair):
    rule = RuleInstance(rule_type="DOMAIN_CHECK", column_name="is_active",
                        rule_expression="0,1", severity="WARNING")
    results, _ = run_validator(settings, "DOMAIN_CHECK", *defect_pair, rule=rule)
    assert results[0].status == RuleStatus.WARN
