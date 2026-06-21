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
3. Reason briefly about the root cause.
4. If the failure is transient/auto-fixable (timeout, connection reset, rate limit, sensor-not-ready, worker lost, OOM, upstream 5xx):
   call rerun_task with execute=true, then call get_run_status to verify recovery.
5. If it is NOT auto-fixable (SQL/syntax error, schema mismatch, permission, not-found, import error, data-quality):
   do NOT rerun. Recommend escalation (open a fix PR / ticket) and stop.

Only call one logical step at a time. When finished, output a short final report:
root cause, action taken, and outcome. Do not call any more tools after the final report."""


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


def run_agent(
    task: dict[str, Any],
    *,
    llm: Optional[LLMClient] = None,
    max_steps: int = 8,
    verbose: bool = True,
) -> dict[str, Any]:
    llm = llm or LLMClient()
    messages: list[dict[str, Any]] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {
            "role": "user",
            "content": "A task failed. Investigate and remediate it.\n"
            + json.dumps(task, indent=2),
        },
    ]
    trace: list[dict[str, Any]] = []

    for step in range(max_steps):
        resp = llm.chat_raw(messages, tools=tools.TOOL_SCHEMAS, temperature=0)
        msg = resp.choices[0].message
        messages.append(_assistant_to_dict(msg))

        if not getattr(msg, "tool_calls", None):
            if verbose and msg.content:
                print(f"\n[final report]\n{msg.content}")
            return {"final": msg.content or "", "trace": trace, "steps": step + 1}

        for tc in msg.tool_calls:
            name = tc.function.name
            raw_args = tc.function.arguments
            if verbose:
                print(f"[step {step + 1}] tool: {name}({raw_args})")
            result = tools.dispatch(name, raw_args)
            if verbose:
                print(f"           -> {result[:280]}")
            trace.append({"tool": name, "args": raw_args, "result": result})
            messages.append(
                {"role": "tool", "tool_call_id": tc.id, "content": result}
            )

    return {"final": "(max steps reached)", "trace": trace, "steps": max_steps}
