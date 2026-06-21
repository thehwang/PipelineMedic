"""Transient failure: a connection/read timeout that recovers on rerun.

Maps to PipelineMedic's `timeout` signature (auto-fixable). First run fails with a
timeout error; after the agent clears+reruns the task, it succeeds — demonstrating
safe automatic remediation.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

_MARKER = "/tmp/pm_timeout_recovered"


def flaky_upstream_call() -> None:
    if not os.path.exists(_MARKER):
        open(_MARKER, "w").close()
        raise RuntimeError(
            "Read timed out. Connection to upstream service timed out after 30s "
            "(deadline exceeded)."
        )
    print("Upstream responded on retry; data fetched successfully.")


with DAG(
    dag_id="failing_timeout",
    description="Transient timeout that recovers on rerun (auto-fixable).",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["pipelinemedic", "demo", "transient"],
) as dag:
    PythonOperator(
        task_id="fetch_from_upstream",
        python_callable=flaky_upstream_call,
        retries=0,
    )
