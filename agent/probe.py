"""Data-level evidence for a failed DAG.

Log-pattern diagnosis can't tell "report genuinely not ready" from "upstream
wrote zero rows" or "schema drifted". Probing the actual table behind the DAG
turns that guess into evidence: row count, column count, partition freshness.

The probe reads a warehouse over SQL (here a local DuckDB warehouse seeded by
scripts/seed_warehouse.py). In a real deployment you'd point the same interface
at your own warehouse — the ProbeResult shape stays identical.
"""

from __future__ import annotations

import datetime as dt
import os
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent

# Which table (and freshness column) backs each demo DAG.
DAG_TABLE: dict[str, dict[str, str]] = {
    "failing_report_sensor": {"table": "events_daily", "freshness_col": "event_date"},
    "failing_timeout": {"table": "dim_users", "freshness_col": "loaded_date"},
    "failing_bad_sql": {"table": "events", "freshness_col": "event_date"},
}


def _warehouse_path() -> str:
    return os.environ.get(
        "PM_WAREHOUSE", str(_ROOT / "airflow" / "data" / "warehouse.duckdb")
    )


def _interpret(result: dict[str, Any]) -> dict[str, Any]:
    """Add a human/agent-readable verdict + staleness signal."""
    if not result.get("reachable"):
        result["verdict"] = "table unreachable — data dependency likely broken"
        return result
    note = []
    fmax = result.get("freshness_max")
    if fmax:
        try:
            d = dt.date.fromisoformat(str(fmax)[:10])
            days = (dt.date.today() - d).days
            result["days_stale"] = days
            result["stale"] = days >= 1
            note.append(
                f"latest partition {fmax} ({days} day(s) old)"
                if days >= 1
                else f"fresh (latest {fmax})"
            )
        except ValueError:
            pass
    if result.get("row_count") == 0:
        note.append("table is EMPTY (0 rows)")
    elif result.get("row_count") is not None:
        note.append(f"{result['row_count']} rows")
    result["verdict"] = "; ".join(note) or "reachable"
    return result


def _probe_warehouse(table: str, freshness_col: str | None) -> dict[str, Any]:
    import duckdb

    path = _warehouse_path()
    if not os.path.exists(path):
        return {
            "reachable": False,
            "table": table,
            "error": f"warehouse not found at {path}; run scripts/seed_warehouse.py",
        }
    con = duckdb.connect(path, read_only=True)
    try:
        rc = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        cc = len(con.execute(f"SELECT * FROM {table} LIMIT 0").description)
        out: dict[str, Any] = {
            "reachable": True,
            "table": table,
            "row_count": int(rc),
            "column_count": cc,
        }
        if freshness_col:
            fmax = con.execute(f"SELECT max({freshness_col}) FROM {table}").fetchone()[0]
            out["freshness_col"] = freshness_col
            out["freshness_max"] = str(fmax) if fmax is not None else None
        return out
    except Exception as e:  # noqa: BLE001 - surface as evidence
        return {"reachable": False, "table": table, "error": str(e)[:300]}
    finally:
        con.close()


def probe_dag(
    dag_id: str,
    table: str | None = None,
    freshness_col: str | None = None,
) -> dict[str, Any]:
    """Probe the table behind a DAG and return data-level evidence."""
    cfg = DAG_TABLE.get(dag_id, {})
    table = table or cfg.get("table")
    freshness_col = freshness_col or cfg.get("freshness_col")
    if not table:
        return {"reachable": False, "error": f"no table mapping for dag '{dag_id}'"}

    return _interpret(_probe_warehouse(table, freshness_col))
