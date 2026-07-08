# DVF — Enterprise Data Validation & Reconciliation Framework

Metadata-driven data validation and reconciliation for **AWS Databricks**.
Validates data between **any supported source and any supported target** using
**pushdown SQL** — Databricks orchestrates; the source/target engines do the
work; only counts, aggregates, hashes, and bounded mismatch samples ever move.

**Not** an ETL framework. **Not** a migration tool. Validation only —
comparable in scope to Informatica DVO / QuerySurge / Datagaps, fully open and
metadata-driven: thousands of objects validated without touching a notebook.

## Supported systems

| Family | Systems |
|---|---|
| RDBMS (JDBC pushdown) | SQL Server, Oracle, PostgreSQL, MySQL, DB2 LUW, DB2 z/OS, Teradata |
| Cloud DW | Snowflake (native connector control path + Spark bulk path) |
| Lakehouse | Delta / Unity Catalog tables & paths |
| Files (Spark) | CSV/TXT/pipe, fixed-width, Parquet, JSON, XML, Excel |
| Mainframe | COBOL copybook (Cobrix), sequential, VSAM dumps, GDG (latest-generation) |

Any connector ↔ any connector: SQL Server → Snowflake, DB2 z/OS → Snowflake,
mainframe file → Snowflake, ODS → Core (Snowflake ↔ Snowflake), CSV → any DB…

## How it works

```
metadata (Delta) ──► ValidationEngine ──► dialect SQL ──► pushdown on source/target
                          │                                       │
                          ▼                                       ▼
                    AuditWriter ◄── counts / hashes / signatures / samples
                          │
                          ▼
        audit & results Delta tables ──► HTML / Excel / CSV / JSON / PDF / email
```

* **35+ validation types** — schema/type/length/precision/scale, counts, nulls,
  blanks, duplicates, PK/UK/FK, mandatory, domain, regex, date/timestamp,
  aggregates, checksum, row-hash reconciliation (missing/extra/column diffs),
  partition counts, business rules, custom SQL, data profiling.
* **Profiles as data** — QUICK / STANDARD / FULL / MIGRATION / REGRESSION /
  INCREMENTAL / CUSTOM bundle rule types per validation (`meta.validation_profiles`).
* **Comparison tiers** — SMALL (pushdown EXCEPT / exact fetch), MEDIUM (row-hash
  join), LARGE (bucket signatures), XLARGE (recursive drill-down: equal buckets
  are proven equal without moving a row; only diverging lineage expands).
* **Cross-engine canonicalization** — every engine renders each value to a
  byte-identical canonical string (NULL sentinels, RTRIM, fixed-scale decimals,
  ISO temporal formats, lowercase-hex MD5), so hashes computed on SQL Server
  and Snowflake are directly comparable. No-hash engines (DB2 z/OS, Teradata)
  degrade gracefully (client-side hashing / aggregate signatures).
* **Operations** — parallel execution with priorities & barriers, transient-only
  retry, resume interrupted runs, rerun-failed, watermark-driven incremental
  validation, sampling, full audit trail with correlation IDs, exception
  repository with drill-down lineage, retention purge, DQ-score dashboard.

## Repository layout

```
config/            framework.yaml + environments/{dev,qa,prod}.yaml
src/dvf/           the framework package (installable wheel)
  core/            engine, planner, task, rerun
  connectors/      base + jdbc family + snowflake + delta + files + sqlite(test)
  dialects/        SQL dialects + Jinja2 statement templates
  sqlgen/          SQL generator + canonicalization options
  validators/      35+ rule types (registry-driven)
  comparison/      tiered comparison + drill-down + differ
  profiling/       pushdown data profiler
  metadata/        Delta/sqlite storage backends, repository, DDL, discovery
  audit/           structured logging + micro-batching audit writer
  reporting/       html / excel / csv / json / pdf / email
notebooks/         13 thin orchestration notebooks (00–12)
tests/             pytest suite (106 tests, no external systems needed)
docs/              architecture, metadata reference, guides, runbooks
```

## Quick start

**Local (no cluster, no external systems):**
```bash
pip install -e ".[dev]"
pytest                       # full suite incl. sqlite end-to-end pipeline
```

**On Databricks:**
1. Build & install the wheel (`python -m build`), add cluster libraries
   (JDBC jars, Cobrix — see docs/deployment-guide.md).
2. Run `notebooks/00_bootstrap_metadata` — creates catalog schemas, tables,
   profiles.
3. Run `notebooks/09_integration_smoke_test` — proves the pipeline end-to-end
   with seeded Delta tables (no external systems).
4. Register connections (`01`) and validations (`02`, or discover with `12`).
5. Run `03_run_validations`; review the HTML/Excel report; rerun failures
   with `04`.

## Documentation

| Doc | Contents |
|---|---|
| [docs/architecture.md](docs/architecture.md) | layers, components, sequence diagrams, tier algorithms |
| [docs/metadata-reference.md](docs/metadata-reference.md) | every table/column/enum + worked examples |
| [docs/user-guide.md](docs/user-guide.md) | register → configure → run → read reports |
| [docs/connector-dev-guide.md](docs/connector-dev-guide.md) | add a new system with zero engine changes |
| [docs/ops-runbook.md](docs/ops-runbook.md) | rerun/resume, triage, retention, correlation IDs |
| [docs/deployment-guide.md](docs/deployment-guide.md) | wheel, cluster libs, UC grants, secrets, DAB jobs |
| [docs/testing-guide.md](docs/testing-guide.md) | pytest layout, golden SQL workflow, smoke test |
