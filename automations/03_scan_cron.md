# Automation 3 — Scan Airflow on a schedule (alert-independent input)

**Trigger:** Cron, e.g. every 10 minutes (`*/10 * * * *`).

**Tools:** Terminal (run `airflow_ops.py`), Post to channel (notify), optionally a ticket tool.

**Why:** Pulls failures straight from the Airflow REST API instead of relying on a
failure callback as the input. The alert channel is then only an *output*.
Robust even if the failure-callback message is missing/delayed.

**Prompt:**

```
Every run, scan Airflow for task failures in the last 15 minutes and act:

1. Run (dedup against the state file so each failure alerts once):
     python /path/to/PipelineMedic/airflow_ops.py scan \
       --airflow-uri <airflow web base> \
       --since-minutes 15 --diagnose --state-file /path/to/.scan_seen.json
   (Or set PM_AIRFLOW_BASE_URL instead of passing --airflow-uri.)

2. For each item in new_failures:
   - Post a concise alert to the ops channel: dag/task/run, diagnosis.summary,
     category, and recommended_action.
   - If diagnosis.auto_fixable is true: add "Reply `approve` in thread to auto-fix"
     (Automation 2 handles the approval + rerun). Do NOT rerun here.
   - If auto_fixable is false: create a ticket (summary = dag/task + category,
     description = evidence + run id + log url) and link it in the alert.

3. If new_failures is empty, do nothing (stay quiet).
```

**Notes**
- `--state-file` prevents duplicate alerts across runs; keep it on a persistent path.
- Keep the rerun (write) action gated behind human approval (Automation 2) even
  though the scan can diagnose unattended.
