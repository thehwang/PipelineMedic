#!/usr/bin/env python3
"""Start the PipelineMedic Web UI + API (with the periodic scan loop).

    ./.venv/bin/python scripts/serve.py            # http://localhost:8000
    PM_SCAN_INTERVAL=120 ./.venv/bin/python scripts/serve.py   # scan every 2 min

Env:
    PM_SCAN_INTERVAL  seconds between scans (default 600 = 10 min)
    PM_AUTO_FIX       1 to auto-remediate transient failures (default 0)
    PM_PORT           HTTP port (default 8000)
    plus PM_LLM_* / PM_AIRFLOW_* (see .env.example)
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import uvicorn  # noqa: E402


def main() -> int:
    port = int(os.environ.get("PM_PORT", "8000"))
    uvicorn.run("agent.web:app", host="0.0.0.0", port=port, log_level="info")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
