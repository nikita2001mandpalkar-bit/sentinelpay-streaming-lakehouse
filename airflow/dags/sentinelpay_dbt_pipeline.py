from datetime import datetime, timedelta
import os
import subprocess

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.ai_rca.handler import generate_rca_from_log

DBT_PROJECT_ROOT = "/opt/sentinelpay/dbt/sentinelpay_dbt"
DBT_PROFILES_DIR = "/opt/sentinelpay/dbt/sentinelpay_dbt/profiles"


def run_dbt_command(command_args):
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = DBT_PROFILES_DIR

    subprocess.run(
        ["dbt"] + command_args,
        cwd=DBT_PROJECT_ROOT,
        check=True,
        env=env,
    )


def dag_failure_handler(context):
    dag_id = context["dag"].dag_id
    task_instance = context["task_instance"]
    task_id = task_instance.task_id
    run_id = context["run_id"]

    log_path = getattr(task_instance, "log_filepath", "")
    if not log_path:
        log_path = (
            f"/opt/airflow/logs/"
            f"dag_id={dag_id}/"
            f"run_id={run_id}/"
            f"task_id={task_id}/"
            f"attempt={task_instance.try_number}.log"
        )

    print("=" * 60)
    print("SentinelPay dbt pipeline failed")
    print(f"DAG    : {dag_id}")
    print(f"Task   : {task_id}")
    print(f"Run ID : {run_id}")
    print(f"Log    : {log_path}")
    print("=" * 60)

    rca_output = generate_rca_from_log(
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        log_path=log_path,
    )
    print(rca_output)


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=20),
    "on_failure_callback": dag_failure_handler,
}


with DAG(
    dag_id="sentinelpay_dbt_pipeline",
    description="Production-style dbt pipeline for SentinelPay transformations and marts.",
    start_date=datetime(2026, 8, 26),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=45),
    default_args=default_args,
    tags=["sentinelpay", "dbt", "transformation", "production"],
) as dag:

    dbt_debug = PythonOperator(
        task_id="dbt_debug",
        python_callable=run_dbt_command,
        op_kwargs={"command_args": ["debug"]},
    )

    dbt_run_staging_core = PythonOperator(
        task_id="dbt_run_staging_core",
        python_callable=run_dbt_command,
        op_kwargs={
            "command_args": [
                "run",
                "--select",
                "stg_event_payment",
                "stg_event_refund",
                "--threads",
                "1",
            ]
        },
    )

    dbt_run_staging_support = PythonOperator(
        task_id="dbt_run_staging_support",
        python_callable=run_dbt_command,
        op_kwargs={
            "command_args": [
                "run",
                "--select",
                "stg_support_ticket",
                "--threads",
                "1",
            ]
        },
    )

    dbt_build_marts_core = PythonOperator(
        task_id="dbt_build_marts_core",
        python_callable=run_dbt_command,
        op_kwargs={
            "command_args": [
                "run",
                "--select",
                "+fact_transactions",
                "+fact_refunds",
                "+dim_customer",
                "+dim_merchant",
                "+dim_wallet",
                "+dim_date",
                "--threads",
                "1",
            ]
        },
    )

    dbt_build_marts_summary = PythonOperator(
        task_id="dbt_build_marts_summary",
        python_callable=run_dbt_command,
        op_kwargs={
            "command_args": [
                "run",
                "--select",
                "+mart_payment_summary",
                "+mart_refund_summary",
                "+mart_support_ticket_summary",
                "--threads",
                "1",
            ]
        },
    )

    dbt_debug >> dbt_run_staging_core >> dbt_run_staging_support >> dbt_build_marts_core >> dbt_build_marts_summary
