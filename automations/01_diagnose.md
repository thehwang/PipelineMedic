# Automation 1 — Diagnose Airflow failure

**Trigger:** New message in the ops alert channel (the Airflow failure-callback channel).

**Tools:** Read channel, Post to channel, Terminal (to run `airflow_ops.py`).

**Gate (do nothing unless matched):** The message contains both `Job Failed` and a
`Log Url:` pointing at the Airflow web host.

**Prompt:**

```
You triage Airflow failures posted to this ops channel.

A new message arrived. If it is NOT an Airflow failure callback (must contain
"Job Failed" and a "Log Url:" Airflow web link), stop silently.

Otherwise:
1. Save the full message text to a temp file and run:
     python /path/to/PipelineMedic/airflow_ops.py diagnose --file <tmp>
2. Read the JSON output. It contains `failure` (dag_id/task_id/run_id/is_prod)
   and `diagnosis` (category, transient, auto_fixable, summary, recommended_action, evidence).
3. Reply IN THE TRIGGERING THREAD with a concise summary:
   - DAG / task / run id
   - Root cause (diagnosis.summary) and the category
   - The 2-4 line log evidence
   - Recommended action
   - If diagnosis.auto_fixable is true: end with
     "✅ This looks safe to auto-fix (clear + rerun). Reply `approve` to proceed."
   - If false: end with
     "⚠️ Not safe to auto-rerun. Suggested: open a fix PR / Jira and ping the owner."
4. Do NOT clear or rerun anything in this automation. Only diagnose and propose.
```
