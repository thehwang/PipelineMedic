"""Function-calling tools that expose Airflow triage/remediation to the LLM.

Each tool is a plain Python function returning a JSON-serializable dict. The
OpenAI-compatible tool schemas are in ``TOOL_SCHEMAS`` and dispatch happens via
``dispatch()``. The agent loop lets Qwen *reason*, but the safety gate (only
auto-rerun transient failures) is enforced here in code — never by the model.
"""

from __future__ import annotations

import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any, Callable

# airflow_ops.py lives at the project root, one level above this package.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import airflow_ops  # noqa: E402

from . import probe  # noqa: E402

# How much log tail to hand back to the model / diagnosis.
_LOG_TAIL_CHARS = 4000


def _client() -> "airflow_ops.AirflowClient":
    return airflow_ops.AirflowClient.from_env()


def _is_prod(client: "airflow_ops.AirflowClient") -> bool:
    return airflow_ops._looks_prod(client.base, client.base)


# --------------------------------------------------------------------------- #
# Tools
# --------------------------------------------------------------------------- #
def list_recent_failures(since_minutes: int = 60) -> dict[str, Any]:
    """Perceive: recently failed task instances."""
    client = _client()
    failures = client.list_failed_task_instances(since_minutes)
    items = [
        {
            "dag_id": ti.get("dag_id"),
            "run_id": ti.get("dag_run_id"),
            "task_id": ti.get("task_id"),
            "try_number": ti.get("try_number", 1),
        }
        for ti in failures
    ]
    return {"failures": items, "count": len(items)}


def get_task_log(
    dag_id: str,
    run_id: str,
    task_id: str,
    try_number: int = 1,
) -> dict[str, Any]:
    """Evidence: fetch the tail of a failed task's log."""
    client = _client()
    log = client.logs(dag_id, run_id, task_id, try_number)
    return {"log_tail": log[-_LOG_TAIL_CHARS:]}


def run_baseline_diagnosis(
    dag_id: str,
    run_id: str,
    task_id: str,
    try_number: int = 1,
) -> dict[str, Any]:
    """Deterministic regex baseline diagnosis (a strong prior for the model)."""
    client = _client()
    log = client.logs(dag_id, run_id, task_id, try_number)
    d = airflow_ops.diagnose(log, _is_prod(client))
    return asdict(d)


def rerun_task(
    dag_id: str,
    run_id: str,
    task_id: str,
    try_number: int = 1,
    execute: bool = False,
) -> dict[str, Any]:
    """Act (gated): clear a failed task so the scheduler reruns it.

    Safety gate enforced here: an *execute* rerun is refused unless the failure
    is in the transient auto-fix allowlist. ``execute=False`` is a dry run.
    """
    client = _client()
    log = client.logs(dag_id, run_id, task_id, try_number)
    d = airflow_ops.diagnose(log, _is_prod(client))

    if execute and not d.auto_fixable:
        return {
            "action": "refused",
            "reason": (
                f"Failure category '{d.category}' is not auto-fixable; "
                "escalate to a human (open a PR/ticket) instead of rerunning."
            ),
            "diagnosis": asdict(d),
        }

    result = client.clear_task(
        dag_id, run_id, task_id, dry_run=not execute
    )
    return {
        "action": "cleared" if execute else "dry_run",
        "diagnosis": asdict(d),
        "result": result,
    }


def get_run_status(dag_id: str, run_id: str, task_id: str) -> dict[str, Any]:
    """Verify: current state of the task instance after acting."""
    client = _client()
    ti = client.task_instance(dag_id, run_id, task_id)
    return {"state": ti.get("state"), "try_number": ti.get("try_number")}


def probe_data(
    dag_id: str,
    table: str | None = None,
    freshness_col: str | None = None,
) -> dict[str, Any]:
    """Data-level evidence: probe the table behind the DAG (rows/freshness/schema)."""
    return probe.probe_dag(dag_id, table, freshness_col)


# --------------------------------------------------------------------------- #
# Registry + OpenAI tool schemas
# --------------------------------------------------------------------------- #
TOOL_REGISTRY: dict[str, Callable[..., dict]] = {
    "list_recent_failures": list_recent_failures,
    "get_task_log": get_task_log,
    "run_baseline_diagnosis": run_baseline_diagnosis,
    "rerun_task": rerun_task,
    "get_run_status": get_run_status,
    "probe_data": probe_data,
}


def _ti_props(extra: dict | None = None) -> dict:
    props = {
        "dag_id": {"type": "string"},
        "run_id": {"type": "string", "description": "the dag_run_id"},
        "task_id": {"type": "string"},
        "try_number": {"type": "integer", "default": 1},
    }
    if extra:
        props.update(extra)
    return props


TOOL_SCHEMAS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "list_recent_failures",
            "description": "List Airflow task instances that failed recently.",
            "parameters": {
                "type": "object",
                "properties": {
                    "since_minutes": {
                        "type": "integer",
                        "description": "look-back window in minutes",
                        "default": 60,
                    }
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_task_log",
            "description": "Fetch the tail of a failed task instance's log.",
            "parameters": {
                "type": "object",
                "properties": _ti_props(),
                "required": ["dag_id", "run_id", "task_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_baseline_diagnosis",
            "description": (
                "Run the deterministic regex baseline diagnosis on a task's log. "
                "Returns category, whether it is transient/auto-fixable, and "
                "recommended action. Use as a prior, then reason about it."
            ),
            "parameters": {
                "type": "object",
                "properties": _ti_props(),
                "required": ["dag_id", "run_id", "task_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "rerun_task",
            "description": (
                "Clear a failed task so the scheduler reruns it. Set execute=true "
                "to actually act (default false = dry run). Non-transient failures "
                "are refused by the safety gate."
            ),
            "parameters": {
                "type": "object",
                "properties": _ti_props(
                    {"execute": {"type": "boolean", "default": False}}
                ),
                "required": ["dag_id", "run_id", "task_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_run_status",
            "description": "Get the current state of a task instance (to verify a fix).",
            "parameters": {
                "type": "object",
                "properties": {
                    "dag_id": {"type": "string"},
                    "run_id": {"type": "string", "description": "the dag_run_id"},
                    "task_id": {"type": "string"},
                },
                "required": ["dag_id", "run_id", "task_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "probe_data",
            "description": (
                "Inspect the data table behind the DAG for data-level evidence: "
                "row count, column count, and partition freshness. Use it to tell "
                "'report genuinely not ready / stale partition' from 'data is fine, "
                "the problem is code'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "dag_id": {"type": "string"},
                    "table": {"type": "string", "description": "optional override"},
                    "freshness_col": {"type": "string", "description": "optional override"},
                },
                "required": ["dag_id"],
            },
        },
    },
]


def dispatch(name: str, arguments: str | dict) -> str:
    """Run a tool by name with JSON (string or dict) args; return a JSON string."""
    fn = TOOL_REGISTRY.get(name)
    if fn is None:
        return json.dumps({"error": f"unknown tool: {name}"})
    try:
        args = json.loads(arguments) if isinstance(arguments, str) else dict(arguments)
    except (json.JSONDecodeError, TypeError) as e:
        return json.dumps({"error": f"bad arguments for {name}: {e}"})
    try:
        return json.dumps(fn(**args))
    except Exception as e:  # noqa: BLE001 - surface tool errors back to the model
        return json.dumps({"error": f"{type(e).__name__}: {e}"})
