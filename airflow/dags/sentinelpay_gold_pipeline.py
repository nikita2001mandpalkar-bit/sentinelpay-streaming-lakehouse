from datetime import datetime, timedelta
import subprocess

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = "/opt/sentinelpay"


def run_command(module_name: str) -> None:
    subprocess.run(
        ["python", "-m", module_name],
        cwd=PROJECT_ROOT,
        check=True,
    )


def task_failure_handler(context) -> None:
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]

    print("=" * 60)
    print("Gold pipeline task failed")
    print(f"DAG    : {dag_id}")
    print(f"Task   : {task_id}")
    print(f"Run ID : {run_id}")
    print("=" * 60)


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": task_failure_handler,
}


with DAG(
    dag_id="sentinelpay_gold_pipeline",
    description="Run Gold layer business summary transformations.",
    start_date=datetime(2026, 8, 26),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=20),
    default_args=default_args,
    tags=["sentinelpay", "gold", "analytics"],
) as dag:

    gold_event_payment = PythonOperator(
        task_id="gold_event_payment",
        python_callable=run_command,
        op_kwargs={
            "module_name": "src.transformations.gold.gold_event_payment",
        },
        execution_timeout=timedelta(minutes=8),
    )

    gold_event_refund = PythonOperator(
        task_id="gold_event_refund",
        python_callable=run_command,
        op_kwargs={
            "module_name": "src.transformations.gold.gold_event_refund",
        },
        execution_timeout=timedelta(minutes=8),
    )

    gold_reconciliation = PythonOperator(
        task_id="gold_reconciliation",
        python_callable=run_command,
        op_kwargs={
            "module_name": "src.transformations.gold.gold_reconciliation",
        },
        execution_timeout=timedelta(minutes=8),
    )

    gold_dim_merchant_scd = PythonOperator(
        task_id="gold_dim_merchant_scd",
        python_callable=run_command,
        op_kwargs={
            "module_name": "src.transformations.gold.gold_dim_merchant_scd",
        },
        execution_timeout=timedelta(minutes=8),
    )

    gold_support_ticket = PythonOperator(
        task_id="gold_support_ticket",
        python_callable=run_command,
        op_kwargs={
            "module_name": "src.transformations.gold.gold_support_ticket",
        },
        execution_timeout=timedelta(minutes=8),
    )

    (
        gold_event_payment
        >> gold_event_refund
        >> gold_reconciliation
        >> gold_dim_merchant_scd
        >> gold_support_ticket
    )
