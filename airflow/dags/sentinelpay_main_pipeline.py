from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


def dag_failure_handler(context) -> None:
    dag_id = context["dag"].dag_id
    task_instance = context["task_instance"]
    task_id = task_instance.task_id
    run_id = context["run_id"]
    log_path = getattr(task_instance, "log_filepath", "")

    print("=" * 60)
    print("SentinelPay main pipeline failed")
    print(f"DAG    : {dag_id}")
    print(f"Task   : {task_id}")
    print(f"Run ID : {run_id}")
    print(f"Log    : {log_path}")
    print("=" * 60)


default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=20),
    "on_failure_callback": dag_failure_handler,
}


with DAG(
    dag_id="sentinelpay_main_pipeline",
    description="Production-style orchestrator for SentinelPay quality, gold, and dbt pipelines.",
    start_date=datetime(2026, 8, 30),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=90),
    default_args=default_args,
    tags=["sentinelpay", "main", "production", "orchestration"],
) as dag:
    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    run_quality_gate = TriggerDagRunOperator(
        task_id="run_quality_gate",
        trigger_dag_id="sentinelpay_quality_gate",
        trigger_run_id="{{ dag_run.run_id }}__quality_gate",
        conf={
            "parent_dag_id": "{{ dag.dag_id }}",
            "parent_run_id": "{{ dag_run.run_id }}",
            "triggered_by_task_id": "{{ task.task_id }}",
        },
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
        fail_when_dag_is_paused=True,
    )

    run_gold_pipeline = TriggerDagRunOperator(
        task_id="run_gold_pipeline",
        trigger_dag_id="sentinelpay_gold_pipeline",
        trigger_run_id="{{ dag_run.run_id }}__gold_pipeline",
        conf={
            "parent_dag_id": "{{ dag.dag_id }}",
            "parent_run_id": "{{ dag_run.run_id }}",
            "triggered_by_task_id": "{{ task.task_id }}",
        },
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
        fail_when_dag_is_paused=True,
    )

    run_dbt_pipeline = TriggerDagRunOperator(
        task_id="run_dbt_pipeline",
        trigger_dag_id="sentinelpay_dbt_pipeline",
        trigger_run_id="{{ dag_run.run_id }}__dbt_pipeline",
        conf={
            "parent_dag_id": "{{ dag.dag_id }}",
            "parent_run_id": "{{ dag_run.run_id }}",
            "triggered_by_task_id": "{{ task.task_id }}",
        },
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
        fail_when_dag_is_paused=True,
    )

    start >> run_quality_gate >> run_gold_pipeline >> run_dbt_pipeline >> finish