# Automation 2 — Remediate on approval

**Trigger:** New message in the same Slack ops channel (a thread reply).

**Tools:** Read Slack, Post to Slack, Use MCP server (Slack), Terminal (to run `airflow_ops.py`).

**Gate:** The new message is a thread reply whose text is (or starts with) `approve`,
AND the thread's parent/earlier messages contain a Composer failure callback that a
previous run marked auto-fixable.

**Prompt:**

```
This is the approval step for an Airflow failure auto-fix.

1. Confirm the triggering message is an approval (text is "approve", case-insensitive)
   posted as a reply in a thread. If not, stop silently.
2. Read the thread history. Find the original Composer failure-callback message
   (contains "Job Failed" + composer.googleusercontent.com "Log Url:").
   If none, reply "No failure message found in this thread to act on." and stop.
3. Save that original failure text to a temp file and run a dry-run first:
     python /path/to/PipelineMedic/airflow_ops.py rerun --file <tmp>
   If it refuses (action == "refused" because the failure is not auto-fixable),
   reply with the reason and stop — do NOT force it.
4. If the dry-run is clean, execute the fix (add --yes-prod only when is_prod):
     python /path/to/PipelineMedic/airflow_ops.py rerun --file <tmp> --execute --yes-prod --wait-seconds 600
5. Reply in the thread with the outcome: cleared/refused, and final task state if known.
   Mention @ the on-call if the rerun ends in `failed`.
```
