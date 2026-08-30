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
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id = context["run_id"]

    print(
        f"Failure callback triggered for "
        f"DAG={dag_id}, TASK={task_id}, RUN={run_id}"
    )


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": task_failure_handler,
}


with DAG(
    dag_id="sentinelpay_quality_gate",
    description="Run Silver layer quality validations before downstream processing.",
    start_date=datetime(2026, 8, 24),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    default_args=default_args,
    tags=["sentinelpay", "quality", "silver"],
) as dag:

    validate_event_payment = PythonOperator(
        task_id="validate_event_payment",
        python_callable=run_command,
        op_kwargs={
            "module_name": "src.quality.runner.validate_event_payment",
        },
        execution_timeout=timedelta(minutes=10),
    )

    validate_event_refund = PythonOperator(
        task_id="validate_event_refund",
        python_callable=run_command,
        op_kwargs={
            "module_name": "src.quality.runner.validate_event_refund",
        },
        execution_timeout=timedelta(minutes=10),
    )

    validate_support_ticket = PythonOperator(
        task_id="validate_support_ticket",
        python_callable=run_command,
        op_kwargs={
            "module_name": "src.quality.runner.validate_support_ticket",
        },
        execution_timeout=timedelta(minutes=10),
    )

    validate_bank_accounts = PythonOperator(
        task_id="validate_bank_accounts",
        python_callable=run_command,
        op_kwargs={
            "module_name": "src.quality.runner.validate_bank_accounts",
        },
        execution_timeout=timedelta(minutes=10),
    )

    validate_devices = PythonOperator(
        task_id="validate_devices",
        python_callable=run_command,
        op_kwargs={
            "module_name": "src.quality.runner.validate_devices",
        },
        execution_timeout=timedelta(minutes=10),
    )

    validate_settlements = PythonOperator(
        task_id="validate_settlements",
        python_callable=run_command,
        op_kwargs={
            "module_name": "src.quality.runner.validate_settlements",
        },
        execution_timeout=timedelta(minutes=10),
    )

    validate_event_payment >> validate_event_refund >> validate_support_ticket >> validate_bank_accounts >> validate_devices >> validate_settlements