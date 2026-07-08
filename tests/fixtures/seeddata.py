"""Seed data helpers — sqlite source/target pairs with deliberate, known defects.

Every planted defect is enumerated here so tests can assert *exact* outcomes:
counts, mismatched keys, drifted columns.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta

CUSTOMERS_DDL = """
CREATE TABLE customers (
    id          INTEGER PRIMARY KEY,
    name        VARCHAR(50),
    email       VARCHAR(100),
    amount      DECIMAL(18,2),
    is_active   BOOLEAN,
    created_ts  TIMESTAMP
)"""

_BASE_TS = datetime(2026, 1, 1, 8, 30, 0)


def base_rows(n: int = 10) -> list[tuple]:
    """Deterministic customer rows: id 1..n."""
    rows = []
    for i in range(1, n + 1):
        rows.append((
            i,
            f"Customer {i:03d}",
            f"cust{i:03d}@example.com",
            round(100.0 * i + 0.5, 2),
            1 if i % 2 == 0 else 0,
            (_BASE_TS + timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M:%S"),
        ))
    return rows


def create_customers_db(path: str, rows: list[tuple], ddl: str = CUSTOMERS_DDL,
                        table: str = "customers") -> None:
    db = sqlite3.connect(path)
    try:
        db.execute(ddl)
        db.executemany(
            f"INSERT INTO {table} (id, name, email, amount, is_active, created_ts) "
            "VALUES (?, ?, ?, ?, ?, ?)", rows)
        db.commit()
    finally:
        db.close()


def seed_identical_pair(src_path: str, tgt_path: str, n: int = 10) -> None:
    rows = base_rows(n)
    create_customers_db(src_path, rows)
    create_customers_db(tgt_path, rows)


def seed_count_mismatch_pair(src_path: str, tgt_path: str, n: int = 10,
                             missing_in_target: int = 2) -> None:
    """Target is missing the LAST `missing_in_target` ids (n-1, n, ...)."""
    rows = base_rows(n)
    create_customers_db(src_path, rows)
    create_customers_db(tgt_path, rows[:-missing_in_target])


def seed_quality_defect_pair(src_path: str, tgt_path: str, n: int = 10) -> None:
    """Target plants known quality defects against a clean source:

    * id 3 & id 5: email NULL                (null parity: email +2 on target)
    * id 4: name all-spaces                  (blank parity: name +1)
    * id 7: email duplicated from id 6       (duplicate on email)
    * id 2: amount +50.0                     (aggregate SUM drift)
    * id 8: is_active = 2                    (domain violation for {0,1})
    * id 9: email 'invalid-email'            (pattern violation)
    """
    rows = base_rows(n)
    create_customers_db(src_path, rows)
    t = [list(r) for r in rows]
    t[2][2] = None
    t[4][2] = None
    t[3][1] = "   "
    t[6][2] = t[5][2]
    t[1][3] = round(t[1][3] + 50.0, 2)
    t[7][4] = 2
    t[8][2] = "invalid-email"
    create_customers_db(tgt_path, [tuple(r) for r in t])


def seed_type_drift_pair(src_path: str, tgt_path: str, n: int = 5) -> None:
    """Target narrows name to VARCHAR(30) and stores amount as VARCHAR(20):
    DATA_TYPE (amount: DECIMAL->STRING) and LENGTH (name: 50->30) defects."""
    rows = base_rows(n)
    create_customers_db(src_path, rows)
    ddl = """
    CREATE TABLE customers (
        id          INTEGER PRIMARY KEY,
        name        VARCHAR(30),
        email       VARCHAR(100),
        amount      VARCHAR(20),
        is_active   BOOLEAN,
        created_ts  TIMESTAMP
    )"""
    create_customers_db(tgt_path, rows, ddl=ddl)


def seed_reconciliation_pair(src_path: str, tgt_path: str, n: int = 10) -> None:
    """Planted record-level diffs with UNCHANGED row counts (10 = 10):

    * ids 4 and 9 deleted from target            -> 2 MISSING_IN_TARGET
    * ids 101, 102 inserted into target          -> 2 EXTRA_IN_TARGET
    * id 2: amount +50.0, id 7: name changed     -> 2 COLUMN_MISMATCH
    * matched = n - 4                            -> 6
    (Record counts match on both sides — only hash comparison catches this.)
    """
    rows = base_rows(n)
    create_customers_db(src_path, rows)
    t = [list(r) for r in rows if r[0] not in (4, 9)]
    for r in t:
        if r[0] == 2:
            r[3] = round(r[3] + 50.0, 2)
        if r[0] == 7:
            r[1] = "Renamed Customer"
    t.append((101, "Extra One", "extra1@example.com", 11.0, 1,
              "2026-01-02 00:00:00"))
    t.append((102, "Extra Two", "extra2@example.com", 22.0, 0,
              "2026-01-02 00:00:00"))
    create_customers_db(tgt_path, [tuple(r) for r in t])


def seed_same_db_pair(path: str, n: int = 10) -> None:
    """customers + customers_tgt inside ONE sqlite file (same-connection SMALL
    tier / pushdown EXCEPT path) with the same planted diffs as
    seed_reconciliation_pair."""
    rows = base_rows(n)
    create_customers_db(path, rows)
    t = [list(r) for r in rows if r[0] not in (4, 9)]
    for r in t:
        if r[0] == 2:
            r[3] = round(r[3] + 50.0, 2)
        if r[0] == 7:
            r[1] = "Renamed Customer"
    t.append((101, "Extra One", "extra1@example.com", 11.0, 1,
              "2026-01-02 00:00:00"))
    t.append((102, "Extra Two", "extra2@example.com", 22.0, 0,
              "2026-01-02 00:00:00"))
    db = sqlite3.connect(path)
    try:
        db.execute(CUSTOMERS_DDL.replace("customers", "customers_tgt"))
        db.executemany(
            "INSERT INTO customers_tgt (id, name, email, amount, is_active, "
            "created_ts) VALUES (?, ?, ?, ?, ?, ?)", [tuple(r) for r in t])
        db.commit()
    finally:
        db.close()


def seed_large_pair(src_path: str, tgt_path: str, n: int = 200) -> None:
    """Big-ish pair with diffs SPREAD ACROSS BUCKETS for drill-down tests:

    * missing in target : ids 13, 77, 141          (3)
    * extra in target   : ids 1001, 1002           (2)
    * modified in target: ids 26 (+9.99 amount), 90 (name), 154 (email),
                          198 (amount sign)        (4)
    * matched           : n - 7 = 193
    """
    rows = base_rows(n)
    create_customers_db(src_path, rows)
    t = [list(r) for r in rows if r[0] not in (13, 77, 141)]
    for r in t:
        if r[0] == 26:
            r[3] = round(r[3] + 9.99, 2)
        elif r[0] == 90:
            r[1] = "Bucket Drift"
        elif r[0] == 154:
            r[2] = "drift154@example.com"
        elif r[0] == 198:
            r[3] = -r[3]
    t.append((1001, "Extra A", "xa@example.com", 1.0, 1, "2026-01-03 00:00:00"))
    t.append((1002, "Extra B", "xb@example.com", 2.0, 0, "2026-01-03 00:00:00"))
    create_customers_db(tgt_path, [tuple(r) for r in t])


LARGE_PAIR_EXPECT = {"missing": 3, "extra": 2, "mismatched": 4, "matched": 193}


def seed_orders(path: str, with_orphan: bool = True) -> None:
    """orders.customer_id references customers.id; optionally one orphan (999)."""
    db = sqlite3.connect(path)
    try:
        db.execute("CREATE TABLE orders (order_id INTEGER PRIMARY KEY, "
                   "customer_id INTEGER, total DECIMAL(18,2))")
        rows = [(1, 1, 10.0), (2, 2, 20.0), (3, 3, 30.0)]
        if with_orphan:
            rows.append((4, 999, 40.0))
        db.executemany("INSERT INTO orders VALUES (?, ?, ?)", rows)
        db.commit()
    finally:
        db.close()


def seed_schema_drift_pair(src_path: str, tgt_path: str, n: int = 5) -> None:
    """Target lacks 'email' and adds 'load_dt' — schema/column-count defects."""
    rows = base_rows(n)
    create_customers_db(src_path, rows)
    drift_ddl = """
    CREATE TABLE customers (
        id          INTEGER PRIMARY KEY,
        name        VARCHAR(50),
        amount      DECIMAL(18,2),
        is_active   BOOLEAN,
        created_ts  TIMESTAMP,
        load_dt     DATE
    )"""
    db = sqlite3.connect(tgt_path)
    try:
        db.execute(drift_ddl)
        db.executemany(
            "INSERT INTO customers (id, name, amount, is_active, created_ts, load_dt) "
            "VALUES (?, ?, ?, ?, ?, '2026-01-02')",
            [(r[0], r[1], r[3], r[4], r[5]) for r in rows])
        db.commit()
    finally:
        db.close()
