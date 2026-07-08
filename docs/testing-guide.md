# Testing Guide

## Philosophy
The entire framework is provable **without any external system**: a
`SqliteConnector`/`SqliteDialect` pair (with registered md5/hex UDFs) plus the
`SqliteBackend` storage let the full pipeline — engine, every validator, every
comparison tier including recursive drill-down and the no-hash fallbacks — run
in pytest. Cluster-only behavior (Spark reads, JDBC, Cobrix) is covered by
notebook `09_integration_smoke_test`, the authoritative on-workspace gate.

## Layout
```
tests/
├── conftest.py                  # settings/backend/repo fixtures, --update-golden
├── fixtures/
│   ├── seeddata.py              # every planted defect enumerated & asserted exactly
│   └── harness.py               # run_validator(), CapturingWriter, FlakySqliteConnector
├── unit/
│   ├── config/ metadata/ audit/ core/       # settings, backends, writer, planner,
│   │                                        # retry/rerun, modes (watermark/sampling)
│   ├── dialects/test_golden.py              # byte-exact SQL for 10 dialects
│   ├── validators/                          # structural + quality + business
│   ├── comparison/                          # differ, SMALL/MEDIUM, drill-down
│   ├── profiling/ reporting/ connectors/
├── golden/{dialect}/<case>.sql              # committed dialect SQL
└── integration/
    ├── test_e2e_sqlite.py                   # bootstrap→register→run→audit→HTML
    └── test_e2e_full_profile.py             # counts equal, rows drifted → caught
```

## Running
```bash
pytest                        # full suite (~7s, no network, Windows-safe)
pytest tests/integration      # just the end-to-end gates
pytest -k drilldown -v        # focused
```

## Golden SQL workflow
1. Change a dialect (or add one — see connector-dev-guide).
2. `pytest tests/unit/dialects/test_golden.py` fails showing the SQL diff.
3. Intentional? `pytest --update-golden` regenerates; the `.sql` files land in
   your commit and reviewers see exactly what SQL changed per engine.

## Planted-defect convention
Seeders in `fixtures/seeddata.py` document every defect they plant
(ids, columns, expected counts) and tests assert **exact** outcomes — never
"some mismatches were found". Follow the same discipline for new fixtures:
if a test can't state the exact expected number, the fixture is underspecified.

## On-cluster smoke test
`notebooks/09_integration_smoke_test`: seeds two Delta tables (1,000 rows,
counts equal, 7 planted row diffs), runs a FULL profile, asserts exact
missing/extra/mismatched counts and mismatch_detail contents, then renders
reports. Run it after every deployment and DBR upgrade.

## Windows note
The suite is pure-Python + sqlite — no winutils/Spark needed locally. Tests
that would need a local SparkSession are intentionally kept OUT of the default
suite; Spark behavior is validated on-cluster via notebook 09.
