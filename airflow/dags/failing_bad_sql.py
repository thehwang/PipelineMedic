"""Non-transient failure: a SQL syntax error that a rerun can NOT fix.

Maps to PipelineMedic's `sql_error` signature (NOT auto-fixable). The agent must
refuse to auto-rerun and instead propose a code fix (PR) / open a ticket for a
human. This DAG always fails until the SQL is corrected.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

_BAD_SQL = "SELCT user_id, count(*) FROM events GROUP BY user_id"


def run_query() -> None:
    # Simulates the warehouse rejecting a malformed query.
    raise RuntimeError(
        "SQL compilation error: syntax error at or near 'SELCT' (line 1). "
        f"query: {_BAD_SQL}"
    )


with DAG(
    dag_id="failing_bad_sql",
    description="SQL syntax error; needs a human fix (not auto-fixable).",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["pipelinemedic", "demo", "needs-human"],
) as dag:
    PythonOperator(
        task_id="aggregate_events",
        python_callable=run_query,
        retries=0,
    )
