#!/usr/bin/env python3
"""Run the PipelineMedic agent against a failed Airflow task.

Examples:
    # auto-pick: run the agent on every recent failure
    ./.venv/bin/python scripts/run_agent.py --all

    # target one DAG's most recent failure
    ./.venv/bin/python scripts/run_agent.py --dag-id failing_report_sensor

Uses local Ollama by default; set PM_LLM_* env vars for Qwen Cloud and
PM_AIRFLOW_* for a non-default Airflow.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from agent import tools  # noqa: E402
from agent.loop import run_agent  # noqa: E402


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dag-id", help="Target this DAG's most recent failure.")
    ap.add_argument("--all", action="store_true", help="Run on all recent failures.")
    ap.add_argument("--since-minutes", type=int, default=180)
    ap.add_argument("--max-steps", type=int, default=8)
    args = ap.parse_args(argv)

    found = tools.list_recent_failures(since_minutes=args.since_minutes)["failures"]
    if not found:
        print("No recent failures found. Trigger the demo DAGs first "
              "(see airflow/README.md).")
        return 1

    if args.dag_id:
        targets = [f for f in found if f["dag_id"] == args.dag_id]
        if not targets:
            print(f"No recent failure for dag_id={args.dag_id}. "
                  f"Available: {[f['dag_id'] for f in found]}")
            return 1
    elif args.all:
        targets = found
    else:
        targets = found[:1]

    for i, task in enumerate(targets, 1):
        print("\n" + "=" * 72)
        print(f"AGENT RUN {i}/{len(targets)}: {task['dag_id']}.{task['task_id']}")
        print("=" * 72)
        run_agent(task, max_steps=args.max_steps, verbose=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
