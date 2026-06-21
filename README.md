# ETL Ops AI — Composer 2 failure triage & auto-remediation

AI-assisted runbook for Airflow (Cloud Composer 2) failures posted to a Slack
ops channel. The flow: **read the failure → diagnose root cause → propose a fix →
on human approval, clear & rerun the task automatically.**

```
Slack failure msg ──trigger──► [Automation 1: Diagnose]
   parse dag/task/run/web_base → pull logs (REST) → classify root cause
   → reply in thread: cause + recommended action + "reply `approve` to auto-fix"
                              │
                       human replies `approve`
                              ▼
                ──trigger──► [Automation 2: Remediate]
   re-derive identifiers from thread → airflow_ops rerun --execute
   → poll task state → reply with the result
```

Two automations (not one) because each Cursor Automation trigger is stateless;
making approval a *separate* trigger is what guarantees "act only after consent".

### Alternative input: poll Composer directly (no Slack dependency)

Instead of (or alongside) the Slack-triggered diagnose, a cron automation can
pull failures straight from the Airflow REST API:

```
cron (every 10m) ──► airflow_ops.py scan --diagnose --state-file ...
   → for each NEW failed task: post alert to Slack (+ Jira if not auto-fixable)
   → auto-fix stays gated behind the `approve` reply (Automation 2)
```

This makes Slack an *output* channel only, so monitoring works even when the
Slack failure-callback message is missing or delayed. See `automations/03_scan_cron.md`.

## Components

- `airflow_ops.py` — the enabling CLI the agent calls. Subcommands:
  - `parse` — Slack failure text → structured JSON (`dag_id`, `task_id`, `run_id`, `airflow_uri`, `is_prod`)
  - `logs` — fetch task-instance logs via the Airflow REST API
  - `diagnose` — fetch logs + classify (transient vs. needs-human)
  - `scan` — poll Composer for recent failed tasks (Slack-independent input)
  - `rerun` — clear the failed task so the scheduler reruns it (**DRY-RUN by default**)
- `data_probe.py` — **data-level** evidence via DuckDB + `iceberg_scan` on `gs://`
  (borrowed from the `pq` tool). Checks the table behind a failed DAG: reachable?
  row count? freshness (max of a date column)? schema readable? Turns
  "report not ready" vs "upstream wrote 0 rows" vs "schema drift" from a guess
  into evidence.

### Borrowing from `pq` (jq for Parquet)

`pq` already reads `gs://` parquet via DuckDB httpfs with GCS auth from
`PQ_GCS_BEARER_TOKEN` / `PQ_GCS_HMAC_*`. `data_probe.py` reuses that exact env
contract but targets **Iceberg** tables with
`iceberg_scan('gs://…/<schema>/<table>', allow_moved_paths => true)`.

```bash
export PQ_GCS_BEARER_TOKEN=$(gcloud auth print-access-token)
python data_probe.py --bucket my-data-lake-iceberg \
    --schema analytics --table events_daily_stg \
    --freshness-col event_date
```

For plain parquet (not Iceberg), just shell out to `pq` directly — e.g.
`pq diff a.parquet b.parquet` as a schema-drift CI gate (non-zero on drift),
or `pq stats` / `pq count` for quick profiling.

The diagnose automation can attach a `data_probe` result to its Slack summary so
reviewers see the *data* state alongside the log signature before approving a rerun.

## Why this works with minimal config

The Composer failure-callback message already includes a **Log Url** that contains
the Composer 2 web host (`https://<id>-dot-<region>.composer.googleusercontent.com`).
`airflow_ops.py` uses that host directly as the Airflow REST base, so no extra
environment lookup is needed in the common path.

## Auth & IAM (Composer 2)

Composer 2's Airflow web server accepts a standard Google OAuth access token — no
IAP client-id token dance (that was Composer 1). The runner just needs ADC for a
principal with access to the environment:

```bash
# Service account used by the Cursor cloud agent (recommended for prod):
#   roles/composer.user            on the Composer environment's project
# Local testing with your own creds:
gcloud auth application-default login
```

Install deps:

```bash
pip install google-auth requests duckdb
```

## Usage

```bash
# 1) Parse a failure message (no network/auth needed)
python airflow_ops.py parse --file sample_failure.txt

# 2) Diagnose (pulls logs via REST; needs ADC)
python airflow_ops.py diagnose --file sample_failure.txt

# 3) Preview the fix — DRY-RUN, changes nothing
python airflow_ops.py rerun --file sample_failure.txt

# 4) Actually clear & rerun (prod requires --yes-prod), wait up to 10 min
python airflow_ops.py rerun --file sample_failure.txt --execute --yes-prod --wait-seconds 600
```

Pipe from stdin instead of `--file` by passing the message on `--text` or via a pipe.

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

1. Authenticate the Slack integration and pick the ops channel.
2. Make sure the Cursor cloud agent has ADC for a `roles/composer.user` principal.
3. Create the two automations in the **Agents Window** (Automations editor) using
   the prompts in `automations/`.
