"""Incident store + periodic scan/diagnose, with optional auto-fix.

This is the always-on side of PipelineMedic: every N minutes it pulls recent
Airflow failures, diagnoses each (deterministic baseline), and keeps an in-memory
incident board the Web UI renders. Remediation is gated:

  - transient / auto-fixable  -> "needs_review" (one-click approve in the UI),
                                  or auto-fixed if auto_fix is ON
  - everything else           -> "needs_human" (escalate; never auto-rerun)
"""

from __future__ import annotations

import datetime as _dt
import os
import threading
import time
from typing import Any

from . import tools

_LOCK = threading.Lock()
_SCAN_LOCK = threading.Lock()  # serializes scans; never held during UI reads

_STATE: dict[str, Any] = {
    "incidents": {},          # key -> incident dict
    "last_scan": None,        # iso timestamp
    "auto_fix": os.environ.get("PM_AUTO_FIX", "0") in ("1", "true", "True"),
    "scan_interval": int(os.environ.get("PM_SCAN_INTERVAL", "600")),
    "events": [],             # recent activity feed (newest first)
}

# Active (not-yet-resolved) statuses still get re-diagnosed on each scan.
_RESOLVED = {"recovered", "escalated"}


def _now_hms() -> str:
    return _dt.datetime.now().strftime("%H:%M:%S")


def _now_iso() -> str:
    return _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _key(f: dict) -> str:
    return f"{f['dag_id']}::{f['run_id']}::{f['task_id']}::{f.get('try_number', 1)}"


def _event(msg: str) -> None:
    with _LOCK:
        _STATE["events"].insert(0, {"ts": _now_hms(), "msg": msg})
        del _STATE["events"][50:]


def get_state() -> dict[str, Any]:
    with _LOCK:
        incidents = sorted(
            _STATE["incidents"].values(),
            key=lambda x: (x["status"] in _RESOLVED, not x["auto_fixable"], x["dag_id"]),
        )
        return {
            "incidents": incidents,
            "last_scan": _STATE["last_scan"],
            "auto_fix": _STATE["auto_fix"],
            "scan_interval": _STATE["scan_interval"],
            "events": list(_STATE["events"])[:30],
        }


def set_auto_fix(value: bool) -> None:
    with _LOCK:
        _STATE["auto_fix"] = bool(value)
    _event(f"auto-fix turned {'ON' if value else 'OFF'}")


def get_interval() -> int:
    with _LOCK:
        return _STATE["scan_interval"]


def _upsert(incident: dict) -> None:
    with _LOCK:
        _STATE["incidents"][incident["key"]] = incident


def _get(key: str) -> dict | None:
    with _LOCK:
        return _STATE["incidents"].get(key)


def scan_once() -> dict[str, Any]:
    """One scan round: list failures, diagnose new/active ones, maybe auto-fix."""
    if not _SCAN_LOCK.acquire(blocking=False):
        return get_state()
    try:
        try:
            found = tools.list_recent_failures(since_minutes=180)["failures"]
        except Exception as e:  # noqa: BLE001
            _event(f"scan error: {type(e).__name__}: {e}")
            with _LOCK:
                _STATE["last_scan"] = _now_iso()
            return get_state()

        current_keys = {_key(f) for f in found}
        # Prune unresolved incidents whose failed run no longer exists (fixed
        # externally or deleted). Keep recovered/escalated ones on the board.
        with _LOCK:
            for k, inc in list(_STATE["incidents"].items()):
                if k not in current_keys and inc["status"] not in _RESOLVED:
                    del _STATE["incidents"][k]

        new_count = 0
        for f in found:
            key = _key(f)
            existing = _get(key)
            if existing and existing["status"] in _RESOLVED:
                continue  # already handled; don't re-diagnose

            d = tools.run_baseline_diagnosis(
                f["dag_id"], f["run_id"], f["task_id"], f.get("try_number", 1)
            )
            incident = existing or {
                "key": key,
                "history": [],
                "first_seen": _now_iso(),
            }
            incident.update(
                {
                    "dag_id": f["dag_id"],
                    "run_id": f["run_id"],
                    "task_id": f["task_id"],
                    "try_number": f.get("try_number", 1),
                    "category": d["category"],
                    "transient": d["transient"],
                    "auto_fixable": d["auto_fixable"],
                    "summary": d["summary"],
                    "recommended_action": d["recommended_action"],
                    "evidence": d["evidence"],
                    "last_seen": _now_iso(),
                }
            )
            if not existing:
                incident["status"] = "needs_review" if d["auto_fixable"] else "needs_human"
                new_count += 1
                _event(
                    f"NEW {f['dag_id']}.{f['task_id']} -> {d['category']} "
                    f"({'auto-fixable' if d['auto_fixable'] else 'needs human'})"
                )
            _upsert(incident)

        with _LOCK:
            _STATE["last_scan"] = _now_iso()
            auto = _STATE["auto_fix"]

        if new_count == 0:
            _event("scan: no new failures")

        if auto:
            for inc in list(get_state()["incidents"]):
                if inc["auto_fixable"] and inc["status"] == "needs_review":
                    _event(f"auto-fix: remediating {inc['dag_id']}.{inc['task_id']}")
                    approve(inc["dag_id"], inc["run_id"], inc["task_id"], inc["try_number"])

        return get_state()
    finally:
        _SCAN_LOCK.release()


def approve(
    dag_id: str,
    run_id: str,
    task_id: str,
    try_number: int = 1,
    *,
    verify_seconds: int = 75,
) -> dict[str, Any]:
    """Gated remediation: clear+rerun a task, then verify recovery."""
    key = f"{dag_id}::{run_id}::{task_id}::{try_number}"
    incident = _get(key)

    res = tools.rerun_task(dag_id, run_id, task_id, try_number, execute=True)

    if res.get("action") == "refused":
        status, note = "needs_human", f"refused: {res.get('reason', '')}"
    elif res.get("action") == "cleared":
        _event(f"rerun cleared {dag_id}.{task_id}; verifying…")
        final = None
        deadline = time.time() + verify_seconds
        while time.time() < deadline:
            try:
                final = tools.get_run_status(dag_id, run_id, task_id).get("state")
            except Exception:  # noqa: BLE001
                final = None
            if final in ("success", "failed"):
                break
            time.sleep(5)
        if final == "success":
            status, note = "recovered", "rerun succeeded"
            _event(f"✅ recovered {dag_id}.{task_id}")
        else:
            status, note = "rerun_failed", f"rerun ended in state={final}"
            _event(f"⚠️ rerun did not recover {dag_id}.{task_id} (state={final})")
    else:
        status, note = "needs_review", f"unexpected: {res.get('action')}"

    if incident is None:
        incident = {"key": key, "dag_id": dag_id, "run_id": run_id,
                    "task_id": task_id, "try_number": try_number, "history": []}
    incident["status"] = status
    incident.setdefault("history", []).append({"ts": _now_iso(), "note": note})
    _upsert(incident)
    return incident


def escalate(dag_id: str, run_id: str, task_id: str, try_number: int = 1) -> dict[str, Any]:
    """Mark an incident as handed off to a human (closes it on the board)."""
    key = f"{dag_id}::{run_id}::{task_id}::{try_number}"
    incident = _get(key)
    if incident is None:
        return {"error": "unknown incident"}
    incident["status"] = "escalated"
    incident.setdefault("history", []).append(
        {"ts": _now_iso(), "note": "escalated to human (PR/ticket)"}
    )
    _upsert(incident)
    _event(f"escalated {dag_id}.{task_id} to human")
    return incident
