# User Guide

## 1. One-time setup
1. Ask an admin to deploy DVF (docs/deployment-guide.md) and run
   `00_bootstrap_metadata` for your environment.
2. Sanity-check the workspace with `09_integration_smoke_test` — it must print
   `SMOKE TEST PASSED`.

## 2. Register a connection (notebook 01)
Add a `ConnectionDef` with **secret references only** (Databricks secret scope
or AWS Secrets Manager ARN). Verify it appears in the displayed table.
Details per engine: docs/metadata-reference.md.

## 3. Configure validations (notebook 02 or 12)
* Hand-configure: edit the `CONFIGS` list or point the widget at a CSV/JSON of
  validation_config rows on S3/Volumes.
* Bulk-onboard: run `12_object_discovery` — it scans the source schema,
  matches target objects, detects primary keys, and registers **disabled**
  candidates. Review, then enable via `08_metadata_maintenance`.

Choose a profile:
| Goal | Profile |
|---|---|
| connectivity + shape + counts (fast) | QUICK |
| + types, nulls, blanks, duplicates, PK, aggregates | STANDARD |
| + full record-level reconciliation (missing/extra/column diffs) | FULL |
| migration cutover evidence (adds length/precision/scale/defaults/constraints/indexes) | MIGRATION |
| STANDARD + your business rules | REGRESSION |
| daily delta validation via watermark | INCREMENTAL |
| only your custom rules | CUSTOM |

Column-specific checks (domain, regex, mandatory, FK, business rules, custom
SQL) go in `meta.validation_rules` — see the reference doc.

## 4. Run (notebook 03)
Widgets: environment, execution groups or validation IDs, optional profile
override. The notebook prints the run summary, shows per-validation outcomes,
renders the HTML report inline, and writes the configured formats to the S3
report path.

Statuses: **PASSED** · **WARNING** (only WARNING-severity failures) ·
**FAILED** (data mismatch) · **ERROR** (execution problem) · **TIMEOUT** ·
**SKIPPED**.

## 5. Read the results
* **HTML report** — summary cards; per-validation accordion with every rule,
  values, differences, tolerance, duration; mismatch samples; the exact
  pushdown SQL per rule (details/SQL toggle).
* **Excel** — Summary / Validations / RuleResults / Mismatches sheets with
  status coloring and filters.
* **Mismatch repository** — `results.mismatch_detail`: keys, offending column,
  source/target canonical values, full row payloads, drill-down `bucket_path`.
* **Dashboard** — notebook 07: trends, failure hot-spots, DQ score by group.

## 6. Fix & rerun (notebook 04)
* `rerun_failed` — new run covering only the validations whose latest attempt
  failed/errored/timed out (`parent_run_id` links it to the original).
* `resume` — after an interrupted run: everything not yet PASSED re-executes.

## 7. Schedule (notebook 10)
Create a Databricks Job on `10_scheduled_driver` with your groups; it emails
the summary (per-group recipients in framework.yaml) and can fail the job task
on FAILED runs for alerting. Add `11_retention_purge` monthly.

## 8. Performance knobs
| Symptom | Knob |
|---|---|
| runs serialized | `engine.max_parallel` (framework.yaml), per-validation `priority`/`execution_order` |
| big table slow | `comparison_tier=XLARGE`, `partition_buckets`, INCREMENTAL mode |
| huge mismatch floods | `max_mismatch_samples`, thresholds on reconciliation rules |
| flaky network | `engine.retry_max`, `retry_backoff_s` (transient-only) |
| timestamps differ by precision | rule parameter `ts_precision` (e.g. 3 for SQL Server DATETIME) |
| trailing spaces / case collations | canonicalization handles RTRIM; set `case_insensitive=true` for CI collations |
