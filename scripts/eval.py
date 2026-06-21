#!/usr/bin/env python3
"""Evaluate the diagnosis layer against a labeled failure-log set.

Reports three things that matter for an SRE autopilot:
  - category accuracy          (did we name the right root cause?)
  - auto-fixable accuracy       (transient vs needs-human)
  - SAFETY  (UNSAFE count)      (non-transient wrongly marked auto-fixable -> 0!)

The last one is the gate's guarantee: PipelineMedic must never auto-rerun a
code/SQL/schema/permission failure. This run exits non-zero if that ever happens.

Run: ./.venv/bin/python scripts/eval.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import airflow_ops  # noqa: E402

CASES = ROOT / "eval" / "cases.jsonl"


def main() -> int:
    cases = [json.loads(line) for line in CASES.read_text().splitlines() if line.strip()]

    cat_ok = fix_ok = 0
    unsafe = []   # non-transient predicted auto-fixable (dangerous)
    missed = []   # transient predicted needs-human (safe but less automation)
    rows = []

    for c in cases:
        d = airflow_ops.diagnose(c["log"], is_prod=False)
        cat_match = d.category == c["expected_category"]
        fix_match = d.auto_fixable == c["expected_auto_fixable"]
        cat_ok += cat_match
        fix_ok += fix_match
        if d.auto_fixable and not c["expected_auto_fixable"]:
            unsafe.append(c["id"])
        if not d.auto_fixable and c["expected_auto_fixable"]:
            missed.append(c["id"])
        flag = "ok " if cat_match else "CAT"
        if not fix_match:
            flag = "FIX"
        rows.append((flag, c["id"], c["expected_category"], d.category))

    n = len(cases)
    print(f"{'':4} {'case':<24} {'expected':<24} {'predicted':<24}")
    print("-" * 80)
    for flag, cid, exp, pred in rows:
        mark = "✓" if flag == "ok " else "✗"
        print(f"{mark:<4} {cid:<24} {exp:<24} {pred:<24}")

    print("\n" + "=" * 50)
    print(f"cases:                 {n}")
    print(f"category accuracy:     {cat_ok}/{n}  ({cat_ok / n:.0%})")
    print(f"auto-fixable accuracy: {fix_ok}/{n}  ({fix_ok / n:.0%})")
    print(f"UNSAFE (auto-fix a needs-human failure): {len(unsafe)}  {unsafe or ''}")
    print(f"conservative misses (transient -> human): {len(missed)}  {missed or ''}")
    print("=" * 50)

    if unsafe:
        print("\n❌ SAFETY FAILURE: the gate would auto-rerun a non-transient failure.")
        return 1
    print("\n✅ SAFE: no non-transient failure was ever marked auto-fixable.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
