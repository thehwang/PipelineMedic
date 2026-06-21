# PipelineMedic — autonomous SRE agent for Airflow

An LLM agent that triages and auto-remediates Apache Airflow failures:
**perceive a failed task → read its log → diagnose the root cause → if the
failure is transient, clear & rerun it; if not, escalate to a human.**

The agent reasons with **Qwen** (local Ollama in dev, Qwen Cloud / DashScope for
the final run — same code, just env vars), but the **safety gate and diagnosis
are deterministic Python**, so it never auto-fixes a code/SQL/schema bug no
matter what the model says.

```
recent failures ──► [perceive] ──► [Qwen reason] ──► tool calls
                                          │
                          ┌───────────────┴───────────────┐
                   transient?                         not transient?
                  clear + rerun (gated)            refuse → escalate (PR/ticket)
                          │
                     [verify state]
```

## Quickstart (local demo — no cloud, no prod access)

```bash
# 0) Python deps (one-time)
python3 -m venv .venv && ./.venv/bin/pip install -r requirements.txt

# 1) Verify the LLM provider (defaults to local Ollama qwen2.5:3b)
./.venv/bin/python scripts/check_llm.py

# 2) Bring up the local Airflow + 3 demo DAGs, then trigger them (see airflow/README.md)
cd airflow && docker compose up -d && cd ..
#   …unpause + trigger failing_timeout / failing_report_sensor / failing_bad_sql

# 3) Drive the tools directly (no LLM) — proves perceive→diagnose→gate→act→verify
./.venv/bin/python scripts/check_tools.py

# 4) Run the full Qwen-driven agent on the live failures
./.venv/bin/python scripts/run_agent.py --all

# 5) Seed the local demo warehouse (so the data-level probe has real data)
./.venv/bin/python scripts/seed_warehouse.py

# 6) …or open the dashboard: failure board + diagnosis + data evidence +
#    one-click approve/rerun, with a background scan every 10 min
./.venv/bin/python scripts/serve.py        # http://localhost:8000

# 7) Score the diagnosis layer (accuracy + the SAFETY gate metric)
./.venv/bin/python scripts/eval.py
```

Switch to Qwen Cloud for the final run by setting `PM_LLM_*` (see `.env.example`);
no code changes.

## Repo layout

| Path | What |
|------|------|
| `agent/config.py` | provider-agnostic LLM settings (env-driven) |
| `agent/llm.py` | thin OpenAI-compatible client (Ollama / Qwen Cloud) |
| `agent/tools.py` | function-calling tools + the safety gate |
| `agent/loop.py` | the agent loop (perceive → reason → act → verify) |
| `agent/scanner.py` | periodic scan + diagnose + incident store (optional auto-fix) |
| `agent/probe.py` | data-level evidence (local DuckDB warehouse) |
| `agent/web.py` | dashboard + API + background 10-min scan loop |
| `airflow/` | local Airflow (docker compose) + 3 failing demo DAGs |
| `eval/cases.jsonl` | labeled failure-log set for the diagnosis eval |
| `scripts/` | `check_llm`, `check_tools`, `run_agent`, `serve`, `seed_warehouse`, `eval` |
| `airflow_ops.py` | Airflow REST client + regex diagnosis (used by tools) |

## Dashboard & periodic scan

`scripts/serve.py` runs a small FastAPI app at <http://localhost:8000>:

- A background loop scans Airflow every `PM_SCAN_INTERVAL` seconds (default 600 =
  **10 min**), diagnoses each failure, and keeps a live incident board.
- Each incident shows category, root-cause summary, and the log evidence.
  - **transient / auto-fixable** → one-click **Approve & Rerun** (clears the task,
    reruns it, and verifies recovery), or
  - **not auto-fixable** → **Escalate to human** (the gate refuses any rerun).
- **Run agent (LLM)** runs the full Qwen reasoning loop on that incident.
- **auto-fix toggle** (or `PM_AUTO_FIX=1`): remediate transient failures
  automatically on each scan; risky ones still wait for human approval (HITL).

This replaces the production Slack approval card with an in-app approval flow, so
the whole demo runs with no external services.

## Data-level evidence

Log patterns alone can't tell "report genuinely not ready" from "upstream wrote
zero rows" or "schema drifted". `probe_data` inspects the table behind the DAG
(row count, freshness, schema) and attaches a verdict to each incident:

| DAG | data verdict | corroborates |
|-----|--------------|--------------|
| `failing_report_sensor` | latest partition is 1 day stale | report genuinely not ready (transient) |
| `failing_timeout` | data fresh & healthy | infra blip, not a data problem (safe rerun) |
| `failing_bad_sql` | data fresh & healthy | the bug is in the query code (needs human) |

The probe reads a warehouse over SQL — here a local DuckDB warehouse seeded by
`scripts/seed_warehouse.py`. In a real deployment you point the same interface at
your own warehouse; the evidence shape is unchanged.

## Evaluation & safety

`scripts/eval.py` scores the diagnosis layer against `eval/cases.jsonl` (labeled
synthetic + anonymized failure logs):

- **category accuracy** — did we name the right root cause?
- **auto-fixable accuracy** — transient vs needs-human
- **UNSAFE count** — non-transient failures wrongly marked auto-fixable; this is
  the gate's core guarantee and **must be 0** (the script exits non-zero otherwise)

Current run: 95% category, 100% auto-fixable, **0 unsafe**. The lone category miss
still lands on the needs-human side — so even an imperfect label stays safe.

---

## Production path (any Airflow + an alert channel)

The same `airflow_ops.py` powers a runbook in production:
**read the failure → diagnose → propose a fix → on human approval, clear & rerun.**

```
failure alert ──trigger──► [Automation 1: Diagnose]
   parse dag/task/run/web_base → pull logs (REST) → classify root cause
   → reply: cause + recommended action + "reply `approve` to auto-fix"
                              │
                       human replies `approve`
                              ▼
                ──trigger──► [Automation 2: Remediate]
   re-derive identifiers → airflow_ops rerun --execute
   → poll task state → reply with the result
```

Approval is a *separate* trigger, which guarantees "act only after consent".

### Alternative input: poll Airflow directly (no alert dependency)

Instead of (or alongside) an alert-triggered diagnose, a cron job can pull
failures straight from the Airflow REST API:

```
cron (every 10m) ──► airflow_ops.py scan --diagnose --state-file ...
   → for each NEW failed task: surface it (+ ticket if not auto-fixable)
   → auto-fix stays gated behind the `approve` step (Automation 2)
```

This makes the alert channel an *output* only, so monitoring works even when the
failure-callback message is missing or delayed. See `automations/03_scan_cron.md`.

## Components

- `airflow_ops.py` — the enabling CLI the agent calls. Subcommands:
  - `parse` — failure text → structured JSON (`dag_id`, `task_id`, `run_id`, `airflow_uri`, `is_prod`)
  - `logs` — fetch task-instance logs via the Airflow REST API
  - `diagnose` — fetch logs + classify (transient vs. needs-human)
  - `scan` — poll Airflow for recent failed tasks
  - `rerun` — clear the failed task so the scheduler reruns it (**DRY-RUN by default**)
- `agent/probe.py` — **data-level** evidence via DuckDB against the demo
  warehouse (`scripts/seed_warehouse.py`). Checks the table behind a failed DAG:
  reachable? row count? freshness (max of a date column)? Turns "report not
  ready" vs "upstream wrote 0 rows" from a guess into evidence. To point it at a
  real warehouse, extend `_probe_warehouse` with your own connection.

## Why this works with minimal config

A failure-callback message includes a **Log Url** with the Airflow web host;
`airflow_ops.py` uses that host directly as the REST base, so no extra
environment lookup is needed in the common path. Otherwise set
`PM_AIRFLOW_BASE_URL` (defaults to the local demo at `http://localhost:8080`).

## Auth

HTTP basic auth by default (the local demo uses `admin/admin`). Config is
env-driven:

```bash
PM_AIRFLOW_BASE_URL=http://localhost:8080
PM_AIRFLOW_AUTH=basic          # or: token
PM_AIRFLOW_USERNAME=admin
PM_AIRFLOW_PASSWORD=admin
# PM_AIRFLOW_TOKEN=...          # when PM_AIRFLOW_AUTH=token (behind a proxy)
```

Install deps:

```bash
pip install -r requirements.txt   # openai, python-dotenv, requests, duckdb, fastapi, uvicorn
```

## Usage

```bash
# 1) Parse a failure message (no network/auth needed)
python airflow_ops.py parse --text "$FAILURE_MSG"

# 2) Diagnose (pulls logs via the Airflow REST API)
python airflow_ops.py diagnose --text "$FAILURE_MSG"

# 3) Preview the fix — DRY-RUN, changes nothing
python airflow_ops.py rerun --text "$FAILURE_MSG"

# 4) Actually clear & rerun (prod requires --yes-prod), wait up to 10 min
python airflow_ops.py rerun --text "$FAILURE_MSG" --execute --yes-prod --wait-seconds 600
```

Pass the failure message via `--text`, `--file <path>`, or pipe it on stdin.

## Safety model

- `rerun` is **dry-run unless `--execute`**.
- Acting on a **prod** host additionally requires `--yes-prod`.
- `rerun` re-diagnoses first and **refuses** to auto-rerun anything outside the
  transient allowlist (timeouts, sensor/report-not-ready, connection resets,
  rate limits, upstream 5xx, worker-lost, OOM). Code/SQL/schema/data-quality/auth
  errors are never auto-fixed — they should become a PR/Jira instead.
- `--force-category` exists for operators who want to override the gate manually.

## Auto-fix allowlist (what Automation 2 is allowed to clear+rerun)

| Category | Auto-fix | Default handling |
|---|---|---|
| sensor report-not-ready / poke timeout | ✅ | clear + rerun |
| timeout / connection reset / 5xx / rate-limit | ✅ | clear + rerun |
| worker lost / OOM | ✅ | clear + rerun (one shot) |
| auth/permission, not-found, SQL, schema, data-quality, import | ❌ | open PR / Jira, notify owner |
| unknown signature | ❌ | escalate to human |

## To finalize (outside this repo)

1. Point `PM_AIRFLOW_*` at your Airflow (or keep the local demo defaults).
2. Pick an alert/output channel for the diagnose + remediate automations.
3. Create the two automations in the **Agents Window** (Automations editor) using
   the prompts in `automations/`.
