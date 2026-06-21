#!/usr/bin/env python3
"""Verify the configured LLM provider is reachable and supports tool-calling.

Run from the project root with the venv:
    ./.venv/bin/python scripts/check_llm.py

Uses local Ollama by default; set PM_LLM_* env vars to point at Qwen Cloud.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from agent.config import get_llm_settings  # noqa: E402
from agent.llm import LLMClient  # noqa: E402


def main() -> int:
    s = get_llm_settings()
    print(
        "PipelineMedic LLM check\n"
        f"  base_url: {s.base_url}\n"
        f"  model:    {s.model}\n"
        f"  api_key:  {s.masked_key()}\n"
        f"  mode:     {'LOCAL (Ollama)' if s.is_local else 'REMOTE (cloud)'}\n"
    )

    client = LLMClient(s)

    print("[1/2] basic chat …")
    try:
        out = client.chat(
            [{"role": "user", "content": "Reply with exactly: OK"}],
            temperature=0,
        )
    except Exception as e:  # noqa: BLE001
        print(f"  ✗ chat failed: {e}")
        return 1
    print("  ->", out.strip()[:120])

    print("[2/2] function-calling probe …")
    tools = [
        {
            "type": "function",
            "function": {
                "name": "report_diagnosis",
                "description": "Report the diagnosed category of an Airflow task failure.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "category": {
                            "type": "string",
                            "description": "short root-cause category, e.g. timeout",
                        },
                        "transient": {
                            "type": "boolean",
                            "description": "true if safe to auto-retry",
                        },
                    },
                    "required": ["category", "transient"],
                },
            },
        }
    ]
    try:
        resp = client.chat_raw(
            [
                {
                    "role": "user",
                    "content": (
                        "An Airflow task failed with 'Sensor has timed out'. "
                        "Call report_diagnosis with your best diagnosis."
                    ),
                }
            ],
            tools=tools,
            temperature=0,
        )
    except Exception as e:  # noqa: BLE001
        print(f"  ✗ tool-calling request failed: {e}")
        return 1

    msg = resp.choices[0].message
    if getattr(msg, "tool_calls", None):
        tc = msg.tool_calls[0]
        print(f"  -> tool call: {tc.function.name}({tc.function.arguments})")
        print("\n✅ chat + function-calling both work.")
        return 0

    print("  -> no tool_calls; content:", (msg.content or "")[:200])
    print(
        "\n⚠️  Chat works but the model did not emit a tool call. "
        "Small (3B) models are unreliable at this; final quality is validated "
        "on qwen-max in the cloud."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
