"""Airflow rendition of the cdl_ingest Glue workflow — the 'original' orchestrator.

Mirrors the reference CDL chain: config load -> batch open -> zone hops -> batch close,
with an S3 sensor gating the run on file arrival and an SNS-style notify at the end.
The sfglue converter reads this (pasted source or via the Airflow REST API) and emits
the equivalent Databricks Job.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

BUCKET = "cdl-demo-495688866359"

default_args = {
    "owner": "cdl",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


def notify_completion(**_):
    print("cdl_ingest pipeline completed")


with DAG(
    dag_id="cdl_ingest_airflow",
    description="CDL medaffairs ingestion: landing -> raw -> curated -> publish",
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["cdl", "veeva_crm"],
) as dag:

    wait_for_files = S3KeySensor(
        task_id="wait_for_landing_files",
        bucket_key=f"s3://{BUCKET}/landing/veeva_crm/*/*.xlsx",
        wildcard_match=True,
        timeout=60 * 60,
        poke_interval=300,
    )

    load_config = GlueJobOperator(task_id="load_config", job_name="load_confiq")
    batch_open = GlueJobOperator(task_id="batch_open", job_name="parent_batch_open")
    landing_to_raw = GlueJobOperator(task_id="landing_to_raw", job_name="landing_to_raw")
    raw_to_curated = GlueJobOperator(task_id="raw_to_curated", job_name="raw_to_curated")
    curated_to_publish = GlueJobOperator(
        task_id="curated_to_publish", job_name="curated_to_publish"
    )
    batch_close = GlueJobOperator(task_id="batch_close", job_name="parent_batch_close")

    notify = PythonOperator(task_id="notify_team", python_callable=notify_completion)

    (wait_for_files >> load_config >> batch_open >> landing_to_raw
     >> raw_to_curated >> curated_to_publish >> batch_close >> notify)
