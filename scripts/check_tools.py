#!/usr/bin/env python3
"""Exercise the agent tools directly against the live local Airflow (no LLM).

Proves the perceive -> diagnose -> gate -> act -> verify plumbing works:
  1. list recent failures (expect the 3 demo DAGs)
  2. baseline-diagnose each
  3. dry-run + execute rerun on a TRANSIENT failure, then verify it succeeds
  4. attempt execute rerun on the SQL failure -> gate refuses

Run: ./.venv/bin/python scripts/check_tools.py
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from agent import tools  # noqa: E402


def _p(title: str, obj) -> None:
    print(f"\n=== {title} ===")
    print(json.dumps(obj, indent=2)[:1200])


def main() -> int:
    failures = tools.list_recent_failures(since_minutes=180)
    _p("list_recent_failures", failures)
    by_dag = {f["dag_id"]: f for f in failures["failures"]}

    for dag_id, f in by_dag.items():
        d = tools.run_baseline_diagnosis(f["dag_id"], f["run_id"], f["task_id"], f["try_number"])
        print(f"  - {dag_id:<24} -> {d['category']:<24} auto_fixable={d['auto_fixable']}")

    # --- transient path: timeout should auto-fix ---
    t = by_dag.get("failing_timeout")
    if t:
        _p("rerun_task (dry run) failing_timeout",
           tools.rerun_task(t["dag_id"], t["run_id"], t["task_id"], t["try_number"], execute=False))
        _p("rerun_task (execute) failing_timeout",
           tools.rerun_task(t["dag_id"], t["run_id"], t["task_id"], t["try_number"], execute=True))
        print("\n  polling for recovery…")
        final = None
        for i in range(24):
            st = tools.get_run_status(t["dag_id"], t["run_id"], t["task_id"])
            final = st.get("state")
            print(f"   [{i}] state={final}")
            if final in ("success", "failed"):
                break
            time.sleep(5)
        print(f"  -> final state: {final}  ({'RECOVERED ✅' if final == 'success' else 'not recovered'})")

    # --- non-transient path: bad SQL must be refused ---
    b = by_dag.get("failing_bad_sql")
    if b:
        _p("rerun_task (execute) failing_bad_sql -> expect refusal",
           tools.rerun_task(b["dag_id"], b["run_id"], b["task_id"], b["try_number"], execute=True))

    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
