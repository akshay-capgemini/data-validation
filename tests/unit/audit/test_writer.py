"""AuditWriter — ordering, batching, merge-after-append, flush semantics."""

from __future__ import annotations

import time

from dvf.audit.writer import AuditWriter


def _writer(backend, **kw) -> AuditWriter:
    w = AuditWriter(backend, batch_rows=kw.get("batch_rows", 100),
                    flush_interval_s=kw.get("flush_interval_s", 0.2))
    w.start()
    return w


def test_append_merge_order_preserved(backend):
    w = _writer(backend)
    w.merge("audit", "run_audit", {"run_id": "R1", "status": "RUNNING",
                                   "total_validations": 3})
    for i in range(3):
        w.append("results", "result_detail", {
            "result_id": f"RES{i}", "execution_id": "E1", "run_id": "R1",
            "validation_id": "V1", "rule_type": "RECORD_COUNT", "status": "PASS"})
    w.merge("audit", "run_audit", {"run_id": "R1", "status": "SUCCEEDED"})
    w.stop()

    assert w.errors == []
    run = backend.read_rows("audit", "run_audit", where_eq={"run_id": "R1"})[0]
    assert run["status"] == "SUCCEEDED"           # finalize merged after RUNNING
    assert run["total_validations"] == 3          # partial merge preserved it
    results = backend.read_rows("results", "result_detail", where_eq={"run_id": "R1"})
    assert len(results) == 3


def test_interval_flush_without_stop(backend):
    w = _writer(backend, flush_interval_s=0.1)
    w.append("results", "result_detail", {
        "result_id": "RESX", "execution_id": "E1", "run_id": "R2",
        "validation_id": "V1", "rule_type": "RECORD_COUNT", "status": "PASS"})
    deadline = time.time() + 5
    while time.time() < deadline:
        if backend.read_rows("results", "result_detail", where_eq={"run_id": "R2"}):
            break
        time.sleep(0.05)
    assert backend.read_rows("results", "result_detail", where_eq={"run_id": "R2"})
    w.stop()


def test_batch_threshold_triggers_drain(backend):
    w = _writer(backend, batch_rows=5, flush_interval_s=30)  # interval too long to matter
    for i in range(12):
        w.append("results", "result_detail", {
            "result_id": f"B{i}", "execution_id": "E1", "run_id": "R3",
            "validation_id": "V1", "rule_type": "RECORD_COUNT", "status": "PASS"})
    deadline = time.time() + 5
    while time.time() < deadline:
        if len(backend.read_rows("results", "result_detail",
                                 where_eq={"run_id": "R3"})) >= 10:
            break
        time.sleep(0.05)
    assert len(backend.read_rows("results", "result_detail",
                                 where_eq={"run_id": "R3"})) >= 10
    w.stop()
    assert len(backend.read_rows("results", "result_detail",
                                 where_eq={"run_id": "R3"})) == 12


def test_flush_blocks_until_persisted(backend):
    w = _writer(backend, flush_interval_s=10)     # only explicit flush drains
    w.append("results", "result_detail", {
        "result_id": "F1", "execution_id": "E1", "run_id": "R4",
        "validation_id": "V1", "rule_type": "RECORD_COUNT", "status": "PASS"})
    w.flush()
    assert backend.read_rows("results", "result_detail", where_eq={"run_id": "R4"})
    w.stop()


def test_bad_write_recorded_not_raised(backend):
    w = _writer(backend)
    w.append("results", "result_detail", {"result_id": None})  # violates PK NOT NULL? no —
    # sqlite PKs allow NULL unless INTEGER PK; use a genuinely bad table instead:
    w.append("results", "no_such_table", {"x": 1})
    w.stop()
    assert any("no_such_table" in str(e) or "Unknown framework table" in str(e)
               for e in w.errors)
