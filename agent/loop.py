"""Minimal agent loop: Qwen reasons, tools act, the code gate keeps it safe.

Perceive -> (LLM) reason -> tool calls -> gate (inside rerun_task) -> verify.
The same loop runs on local Ollama or Qwen Cloud — only env vars change.
"""

from __future__ import annotations

import json
from typing import Any, Optional

from . import tools
from .llm import LLMClient

SYSTEM_PROMPT = """You are PipelineMedic, an autonomous SRE agent for Apache Airflow.
A task has failed. Investigate and remediate it using the provided tools.

Follow this process:
1. Call get_task_log to read the failure evidence.
2. Call run_baseline_diagnosis to get a deterministic prior (category + whether it is transient/auto-fixable).
3. Call probe_data to check the table behind the DAG (rows / freshness / schema). Use it to corroborate:
   a stale or empty partition supports "report not ready" (transient); healthy fresh data means the
   failure is infra-level (timeout) or a code bug (SQL), not a data problem.
4. Reason briefly about the root cause, combining the log signature and the data evidence.
5. If the failure is transient/auto-fixable (timeout, connection reset, rate limit, sensor-not-ready, worker lost, OOM, upstream 5xx):
   call rerun_task with execute=true, then call get_run_status to verify recovery.
6. If it is NOT auto-fixable (SQL/syntax error, schema mismatch, permission, not-found, import error, data-quality):
   do NOT rerun. Recommend escalation (open a fix PR / ticket) and stop.

Only call one logical step at a time. When finished, output a short final report:
root cause, action taken, and outcome. Do not call any more tools after the final report."""


# Identifier args each tool accepts, so we can backfill them from the (single,
# known) task context when a weak model forgets to pass them.
_TOOL_IDENT_KEYS: dict[str, tuple[str, ...]] = {
    "get_task_log": ("dag_id", "run_id", "task_id", "try_number"),
    "run_baseline_diagnosis": ("dag_id", "run_id", "task_id", "try_number"),
    "rerun_task": ("dag_id", "run_id", "task_id", "try_number"),
    "get_run_status": ("dag_id", "run_id", "task_id"),
    "probe_data": ("dag_id",),
}


def _fill_defaults(name: str, raw_args: str | dict, task: dict[str, Any]) -> dict[str, Any]:
    """Backfill missing identifier args from the task context."""
    try:
        args = json.loads(raw_args) if isinstance(raw_args, str) else dict(raw_args or {})
    except (json.JSONDecodeError, TypeError):
        args = {}
    for key in _TOOL_IDENT_KEYS.get(name, ()):  # only inject keys the tool accepts
        if key in task and not args.get(key):
            args[key] = task[key]
    return args


def _assistant_to_dict(msg: Any) -> dict[str, Any]:
    d: dict[str, Any] = {"role": "assistant", "content": msg.content or ""}
    if getattr(msg, "tool_calls", None):
        d["tool_calls"] = [
            {
                "id": tc.id,
                "type": "function",
                "function": {
                    "name": tc.function.name,
                    "arguments": tc.function.arguments,
                },
            }
            for tc in msg.tool_calls
        ]
    return d


def _baseline(task: dict[str, Any]) -> dict[str, Any] | None:
    """Deterministic prior so the loop knows the expected terminal action."""
    try:
        return tools.run_baseline_diagnosis(
            task["dag_id"], task["run_id"], task["task_id"], task.get("try_number", 1)
        )
    except Exception:  # noqa: BLE001 - baseline is best-effort
        return None


def _executed_fix(trace: list[dict[str, Any]]) -> bool:
    """True if a rerun was actually executed (task cleared) during this run."""
    for t in trace:
        if t["tool"] != "rerun_task":
            continue
        try:
            res = json.loads(t["result"])
        except (json.JSONDecodeError, TypeError):
            continue
        if res.get("action") == "cleared":
            return True
    return False


_NUDGE = (
    "You diagnosed a TRANSIENT / auto-fixable failure but have NOT executed the "
    "remediation yet. Call rerun_task with execute=true for this task now, then "
    "call get_run_status to verify it recovered. Do not write a final report "
    "until the rerun has been executed and verified."
)


def run_agent(
    task: dict[str, Any],
    *,
    llm: Optional[LLMClient] = None,
    max_steps: int = 10,
    max_nudges: int = 2,
    verbose: bool = True,
) -> dict[str, Any]:
    llm = llm or LLMClient()
    base = _baseline(task)
    auto_fixable = bool(base and base.get("auto_fixable"))

    messages: list[dict[str, Any]] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {
            "role": "user",
            "content": "A task failed. Investigate and remediate it.\n"
            + json.dumps(task, indent=2),
        },
    ]
    trace: list[dict[str, Any]] = []
    nudges = 0

    for step in range(max_steps):
        resp = llm.chat_raw(messages, tools=tools.TOOL_SCHEMAS, temperature=0)
        msg = resp.choices[0].message
        messages.append(_assistant_to_dict(msg))

        if not getattr(msg, "tool_calls", None):
            # Follow-through guard: don't let a weak model stop at a dry run on a
            # transient failure. Nudge it to actually execute + verify.
            if auto_fixable and not _executed_fix(trace) and nudges < max_nudges:
                nudges += 1
                if verbose:
                    print(f"[nudge {nudges}] transient but not yet remediated; pushing to execute")
                messages.append({"role": "user", "content": _NUDGE})
                continue
            if verbose and msg.content:
                print(f"\n[final report]\n{msg.content}")
            return {
                "final": msg.content or "",
                "trace": trace,
                "steps": step + 1,
                "remediated": _executed_fix(trace),
                "nudges": nudges,
            }

        for tc in msg.tool_calls:
            name = tc.function.name
            args = _fill_defaults(name, tc.function.arguments, task)
            if verbose:
                print(f"[step {step + 1}] tool: {name}({json.dumps(args)})")
            result = tools.dispatch(name, args)
            if verbose:
                print(f"           -> {result[:280]}")
            trace.append({"tool": name, "args": json.dumps(args), "result": result})
            messages.append(
                {"role": "tool", "tool_call_id": tc.id, "content": result}
            )

    return {
        "final": "(max steps reached)",
        "trace": trace,
        "steps": max_steps,
        "remediated": _executed_fix(trace),
        "nudges": nudges,
    }
