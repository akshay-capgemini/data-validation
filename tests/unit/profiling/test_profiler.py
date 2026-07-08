"""DataProfiler + DATA_PROFILE validator — exact stats on seeded data."""

from __future__ import annotations

import math

import pytest

from dvf.audit.models import RuleStatus
from dvf.metadata.models import RuleInstance
from dvf.profiling.profiler import ProfileMetrics
from fixtures.harness import run_validator
from fixtures.seeddata import base_rows, seed_identical_pair, seed_quality_defect_pair


def test_data_profile_identical_pair(settings, tmp_path):
    src, tgt = str(tmp_path / "pfs.db"), str(tmp_path / "pft.db")
    seed_identical_pair(src, tgt, n=10)
    results, ctx = run_validator(settings, "DATA_PROFILE", src, tgt)
    assert results[0].status == RuleStatus.PASS

    records = ctx.writer.table_rows("profile_detail")
    assert records, "profile metrics must be persisted"
    by_key = {(r["side"], r["column_name"], r["metric"]): r["metric_value"]
              for r in records}
    assert by_key[("SOURCE", None, ProfileMetrics.ROW_COUNT)] == "10"
    assert by_key[("SOURCE", "email", ProfileMetrics.NULL_COUNT)] == "0"
    assert by_key[("SOURCE", "email", ProfileMetrics.DISTINCT_COUNT)] == "10"

    # numeric moments: amounts are 100.5*i + ... — verify AVG/STDDEV math
    amounts = [r[3] for r in base_rows(10)]
    mean = sum(amounts) / len(amounts)
    variance = sum(a * a for a in amounts) / len(amounts) - mean * mean
    assert float(by_key[("SOURCE", "amount", ProfileMetrics.AVG)]) == \
        pytest.approx(mean, rel=1e-6)
    assert float(by_key[("SOURCE", "amount", ProfileMetrics.STDDEV)]) == \
        pytest.approx(math.sqrt(variance), rel=1e-6)

    # top values captured for leading columns
    top = by_key.get(("SOURCE", "id", ProfileMetrics.TOP_VALUES))
    assert top and '"count": 1' in top


def test_data_profile_detects_drift(settings, tmp_path):
    src, tgt = str(tmp_path / "pds.db"), str(tmp_path / "pdt.db")
    seed_quality_defect_pair(src, tgt, n=10)      # target: 2 NULL emails etc.
    results, _ = run_validator(settings, "DATA_PROFILE", src, tgt)
    r = results[0]
    assert r.status == RuleStatus.FAIL
    assert any("email.NULL_COUNT: 0 vs 2" in d for d in r.details["drift"])


def test_data_profile_compare_off(settings, tmp_path):
    src, tgt = str(tmp_path / "pos.db"), str(tmp_path / "pot.db")
    seed_quality_defect_pair(src, tgt, n=10)
    rule = RuleInstance(rule_type="DATA_PROFILE", parameters={"compare": "false"})
    results, _ = run_validator(settings, "DATA_PROFILE", src, tgt, rule=rule)
    assert results[0].status == RuleStatus.PASS   # profiling only, no assertion
