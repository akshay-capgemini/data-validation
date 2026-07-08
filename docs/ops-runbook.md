# Operations Runbook

## Daily triage flow

1. **Job alert / email arrives** — subject carries status + failed count.
2. Open the run in the HTML report or:
   ```sql
   SELECT * FROM dvf_prod.audit.v_execution_history
   WHERE run_id = '<run_id>' ORDER BY validation_status DESC;
   ```
3. Classify per validation:

| Status | Meaning | Action |
|---|---|---|
| FAILED | data genuinely differs | inspect `results.mismatch_detail` / rule rows; route to data owner |
| ERROR | execution problem (connectivity, SQL, config) | read error_message/error_stack; fix; rerun |
| TIMEOUT | pushdown query exceeded `engine.query_timeout_s` | raise timeout, switch tier (XLARGE), or add INCREMENTAL mode |
| WARNING | only WARNING-severity rules failed | review; tighten severity when it matters |
| SKIPPED | resume skip or unrunnable config | check error_message (e.g. unknown profile) |

4. **Root-cause a FAILED reconciliation**
   ```sql
   SELECT mismatch_type, column_name, key_values, source_value, target_value,
          bucket_path
   FROM dvf_prod.results.mismatch_detail
   WHERE run_id = '<run_id>' AND validation_id = '<vid>' LIMIT 200;
   ```
   * `MISSING_IN_TARGET` → load gap (source_row holds the full canonical row)
   * `EXTRA_IN_TARGET` → duplicates/orphans in target
   * `COLUMN_MISMATCH` → transformation drift; `column_name` + both values
   * `bucket_path` narrows huge tables to the diverging slice
   * The **exact pushdown SQL** for every rule is in
     `results.result_detail.source_sql/target_sql` — reproducible manually.

5. **Fix, then rerun failures only** — notebook 04 (`rerun_failed`,
   `run_id=<failed run>`). Interrupted run (cluster died)? Use `resume`.

## Transient errors & retry

Retry only fires on transient classification (connection failures, deadlocks,
lock timeouts — SQLSTATE 08xxx/40001/…, vendor codes 1205/-911/…). Each attempt
is its own audit row (`attempt` 1..n, new execution_id). Persistent ERROR after
retries ⇒ real environmental issue: check secrets, firewall, driver jars.

Data FAILURES never retry — rerunning without a fix reproduces the failure.

## Log tracing

Every log line is JSON with `correlation_id = run_id:validation_id:execution_id`.
Driver-log search by any component of the triple isolates one validation's
whole story (SQL, retries, timings).

## Watermarks (INCREMENTAL mode)

* Advance only after PASSED/WARNING — a failed increment re-validates next run.
* Skewed/late data? Reset: notebook 08 `reset_watermark` → next run does a full
  scan and re-baselines.
* Inspect: `SELECT * FROM dvf_prod.meta.watermarks`.

## Retention

Notebook 11 (schedule monthly): deletes audit/result rows past
`retention.*` days (framework.yaml) then VACUUMs. Always dry-run first
(default) and eyeball the counts.

## Common issues

| Symptom | Likely cause / fix |
|---|---|
| `No connector registered for type` | typo in connection_type; extension not registered |
| `Secret lookup failed` | scope/key mismatch; cluster lacks scope ACL |
| `JDBC control path needs the cluster JVM` | driver jar missing from cluster libs, or running off-cluster |
| `MEDIUM tier over N rows needs Spark` | driver-only session on huge table — run on a cluster or raise tier |
| `cannot bucket a non-numeric comparison key` | LARGE/XLARGE on no-hash engine without numeric key — add numeric key or use MEDIUM |
| hash mismatch storm on every row | canonicalization mismatch — check `ts_precision`, `case_insensitive`, collations; compare `source_sql` vs `target_sql` canonical exprs |
| counts equal but FULL fails | that is the point — row content drifted; see mismatch_detail |
| audit rows missing after crash | writer drains on stop; hard-kill may lose the last micro-batch — resume re-executes anything not PASSED |
