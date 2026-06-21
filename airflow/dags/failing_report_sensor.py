"""Transient failure: an external report is not ready yet, recovers on rerun.

Maps to PipelineMedic's `sensor_report_not_ready` signature (auto-fixable). First
run fails because the report is PENDING; after a clear+rerun the report is ready.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

_MARKER = "/tmp/pm_report_ready"


def check_report_status() -> None:
    if not os.path.exists(_MARKER):
        open(_MARKER, "w").close()
        raise AirflowException(
            "Sensor has timed out waiting for the daily report. "
            "Report status = PENDING (in_progress); not ready yet."
        )
    print("Report status = READY; downstream can proceed.")


with DAG(
    dag_id="failing_report_sensor",
    description="External report not ready; recovers on rerun (auto-fixable).",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["pipelinemedic", "demo", "transient"],
) as dag:
    PythonOperator(
        task_id="check_report_status",
        python_callable=check_report_status,
        retries=0,
    )
