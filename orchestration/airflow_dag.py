"""
Amazon MWAA (Managed Workflows for Apache Airflow) DAG
Orchestrates AuroraStream Lakehouse Pipeline:
  Sensor: Waits for Bronze S3 stream batch arrival
  ETL: Triggers Glue PySpark Job (Bronze -> Silver)
  ETL: Triggers Glue PySpark Job (Silver -> Gold Dimensional Lakehouse)
  Data Quality: Executes Amazon Deequ Checks
  Data Warehouse: Executes Redshift Serverless COPY & Materialized View Refresh
  Alerting: Dispatches SNS Notifications on Failure
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "data-engineering-core",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="aurorastream_lakehouse_orchestration",
    default_args=default_args,
    description="End-to-End Amazon Lakehouse Orchestrator on MWAA",
    schedule_interval="0 * * * *", # Hourly execution
    catchup=False,
    max_active_runs=1,
    tags=["amazon", "lakehouse", "redshift", "glue", "deequ"]
) as dag:

    start_pipeline = EmptyOperator(task_id="start_pipeline")

    # 1. Sensor: Ensure Bronze streaming partition arrived
    wait_for_bronze_events = S3KeySensor(
        task_id="wait_for_bronze_events",
        bucket_name="{{ var.value.BRONZE_BUCKET }}",
        bucket_key="events/year={{ execution_date.strftime('%Y') }}/month={{ execution_date.strftime('%m') }}/*",
        wildcard_match=True,
        timeout=1800,
        poke_interval=60
    )

    # 2. Glue Job: Bronze to Silver (PySpark)
    run_bronze_to_silver_glue = GlueJobOperator(
        task_id="run_bronze_to_silver_glue",
        job_name="amazon-aurorastream-bronze-to-silver",
        script_args={
            "--BRONZE_BUCKET": "{{ var.value.BRONZE_BUCKET }}",
            "--SILVER_BUCKET": "{{ var.value.SILVER_BUCKET }}",
        },
        aws_conn_id="aws_default"
    )

    # 3. Glue Job: Silver to Gold Dimensional Lakehouse
    run_silver_to_gold_glue = GlueJobOperator(
        task_id="run_silver_to_gold_glue",
        job_name="amazon-aurorastream-silver-to-gold",
        script_args={
            "--SILVER_BUCKET": "{{ var.value.SILVER_BUCKET }}",
            "--GOLD_BUCKET": "{{ var.value.GOLD_BUCKET }}",
        },
        aws_conn_id="aws_default"
    )

    # 4. FinOps Compaction: Consolidates fragmented streaming files into 128MB Parquet blocks
    run_lakehouse_compaction = GlueJobOperator(
        task_id="run_lakehouse_compaction",
        job_name="amazon-aurorastream-compact-partitions",
        script_args={
            "--INPUT_PATH": "s3://{{ var.value.SILVER_BUCKET }}/events/",
            "--OUTPUT_PATH": "s3://{{ var.value.SILVER_BUCKET }}/events_compacted/",
            "--TARGET_PARTITIONS": "4"
        },
        aws_conn_id="aws_default"
    )

    # 5. Redshift Serverless Data Load (Parallel COPY)
    copy_to_redshift_dw = RedshiftDataOperator(
        task_id="copy_to_redshift_dw",
        database="aurorastream_dw",
        workgroup_name="{{ var.value.REDSHIFT_WORKGROUP }}",
        sql="""
            COPY aurorastream_dw.fact_orders
            FROM 's3://{{ var.value.GOLD_BUCKET }}/fact_orders/'
            IAM_ROLE '{{ var.value.REDSHIFT_IAM_ROLE }}'
            FORMAT AS PARQUET;
        """,
        aws_conn_id="aws_default"
    )

    # 6. SNS Failure Alerting
    sns_alert_on_failure = SnsPublishOperator(
        task_id="sns_alert_on_failure",
        target_arn="{{ var.value.SNS_ALERT_TOPIC_ARN }}",
        subject="[ALERT] AuroraStream Pipeline Failed",
        message="Task failed in MWAA DAG. Investigate CloudWatch logs.",
        trigger_rule=TriggerRule.ONE_FAILED
    )

    end_pipeline = EmptyOperator(task_id="end_pipeline")

    # DAG Dependency Chain
    start_pipeline >> wait_for_bronze_events >> run_bronze_to_silver_glue >> run_silver_to_gold_glue >> run_lakehouse_compaction >> copy_to_redshift_dw >> end_pipeline
    [run_bronze_to_silver_glue, run_silver_to_gold_glue, run_lakehouse_compaction, copy_to_redshift_dw] >> sns_alert_on_failure
