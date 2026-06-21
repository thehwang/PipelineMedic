# Automation 3 — Scan Composer on a schedule (Slack-independent input)

**Trigger:** Cron, e.g. every 10 minutes (`*/10 * * * *`).

**Tools:** Terminal (run `airflow_ops.py`), Post to Slack (notify), optionally Use MCP (Jira).

**Why:** Pulls failures straight from Composer's Airflow REST API instead of
relying on a Slack failure callback as the input. Slack is then only an *output*
channel. Robust even if the failure-callback message is missing/delayed.

**Prompt:**

```
Every run, scan Cloud Composer for task failures in the last 15 minutes and act:

1. Run (dedup against the state file so each failure alerts once):
     python /path/to/PipelineMedic/airflow_ops.py scan \
       --project <PROJECT> --location us-east4 --environment <ENV> \
       --since-minutes 15 --diagnose --state-file /path/to/.scan_seen.json
   (Or pass --airflow-uri <composer web base> instead of project/location/env.)

2. For each item in new_failures:
   - Post a concise alert to the ops Slack channel: dag/task/run, diagnosis.summary,
     category, and recommended_action.
   - If diagnosis.auto_fixable is true: add "Reply `approve` in thread to auto-fix"
     (Automation 2 handles the approval + rerun). Do NOT rerun here.
   - If auto_fixable is false: create a Jira ticket (summary = dag/task + category,
     description = evidence + run id + log url) and link it in the Slack alert.

3. If new_failures is empty, do nothing (stay quiet).
```

**Notes**
- `--state-file` prevents duplicate alerts across runs; keep it on a persistent path.
- Keep the rerun (write) action gated behind human approval (Automation 2) even
  though the scan can diagnose unattended.
