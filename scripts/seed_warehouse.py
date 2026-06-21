#!/usr/bin/env python3
"""Build a local demo 'warehouse' (DuckDB) so the data-level probe has something
real to read — no external/cloud warehouse needed for the demo.

Three tables, each in a state that *corroborates* one demo DAG's diagnosis:

  events_daily  -> STALE: latest partition is yesterday (today's is missing).
                   Confirms `failing_report_sensor` ("report not ready").
  dim_users     -> HEALTHY & fresh. Confirms `failing_timeout` is infra-level,
                   not a data problem (safe to just rerun).
  events        -> HEALTHY (has the columns). Confirms `failing_bad_sql` is a
                   code/query bug, not a data problem (needs a human).

Run: ./.venv/bin/python scripts/seed_warehouse.py
"""

from __future__ import annotations

import datetime as dt
import os
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = os.environ.get("PM_WAREHOUSE", str(ROOT / "airflow" / "data" / "warehouse.duckdb"))


def main() -> int:
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    today = dt.date.today()
    con = duckdb.connect(DB_PATH)

    # events_daily: dates today-7 .. today-1  => latest partition is STALE.
    con.execute(
        "CREATE TABLE events_daily (event_date DATE, user_id INTEGER, n INTEGER)"
    )
    rows = []
    for d in range(7, 0, -1):
        day = today - dt.timedelta(days=d)
        for uid in range(1, 21):
            rows.append((day, uid, uid * d))
    con.executemany("INSERT INTO events_daily VALUES (?, ?, ?)", rows)

    # dim_users: fresh dimension loaded today.
    con.execute(
        "CREATE TABLE dim_users (user_id INTEGER, name VARCHAR, loaded_date DATE)"
    )
    con.executemany(
        "INSERT INTO dim_users VALUES (?, ?, ?)",
        [(uid, f"user_{uid}", today) for uid in range(1, 101)],
    )

    # events: fresh fact with the columns the (buggy) query references.
    con.execute(
        "CREATE TABLE events (event_date DATE, user_id INTEGER, amount DOUBLE)"
    )
    con.executemany(
        "INSERT INTO events VALUES (?, ?, ?)",
        [(today, uid, uid * 1.5) for uid in range(1, 51)],
    )

    print(f"seeded {DB_PATH}")
    for tbl, fcol in (("events_daily", "event_date"), ("dim_users", "loaded_date"), ("events", "event_date")):
        rc = con.execute(f"SELECT count(*) FROM {tbl}").fetchone()[0]
        fmax = con.execute(f"SELECT max({fcol}) FROM {tbl}").fetchone()[0]
        print(f"  {tbl:<14} rows={rc:<5} max({fcol})={fmax}")
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
