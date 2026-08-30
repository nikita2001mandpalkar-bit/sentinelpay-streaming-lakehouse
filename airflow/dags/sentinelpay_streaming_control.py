from datetime import datetime, timedelta
import os
import subprocess
import time

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

PROJECT_ROOT = "/opt/sentinelpay"
STREAM_LOG_DIR = "/opt/airflow/logs/streaming_control"
PROCESS_STARTUP_TIMEOUT_SECONDS = 20

BRONZE_STREAM_JOBS = [
    {
        "task_id": "start_bronze_event_data",
        "module_name": "src.streaming.bronze_event_data",
    },
    {
        "task_id": "start_bronze_log_data",
        "module_name": "src.streaming.bronze_log_data",
    },
]

SILVER_STREAM_JOBS = [
    {
        "task_id": "start_silver_event_payment",
        "module_name": "src.transformations.silver_event_payment",
    },
    {
        "task_id": "start_silver_event_refund",
        "module_name": "src.transformations.silver_event_refund",
    },
    {
        "task_id": "start_silver_support_ticket",
        "module_name": "src.transformations.silver_support_ticket",
    },
    {
        "task_id": "start_silver_logs",
        "module_name": "src.transformations.silver_logs",
    },
]

ALL_STREAM_MODULES = [
    job["module_name"]
    for job in BRONZE_STREAM_JOBS + SILVER_STREAM_JOBS
]


def dag_failure_handler(context) -> None:
    dag_id = context["dag"].dag_id
    task_instance = context["task_instance"]
    task_id = task_instance.task_id
    run_id = context["run_id"]
    log_path = getattr(task_instance, "log_filepath", "")

    print("=" * 60)
    print("SentinelPay streaming control DAG failed")
    print(f"DAG    : {dag_id}")
    print(f"Task   : {task_id}")
    print(f"Run ID : {run_id}")
    print(f"Log    : {log_path}")
    print("=" * 60)


def get_running_pids(module_name: str) -> list[str]:
    result = subprocess.run(
        ["pgrep", "-f", f"python -m {module_name}"],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0 or not result.stdout.strip():
        return []

    return [pid.strip() for pid in result.stdout.splitlines() if pid.strip()]


def read_log_tail(log_file: str, lines: int = 50) -> str:
    if not os.path.exists(log_file):
        return f"log file not found: {log_file}"

    result = subprocess.run(
        ["tail", "-n", str(lines), log_file],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def wait_for_process_startup(
    process: subprocess.Popen,
    module_name: str,
    log_file: str,
    startup_timeout_seconds: int,
) -> None:
    deadline = time.time() + startup_timeout_seconds

    while time.time() < deadline:
        return_code = process.poll()

        if return_code is not None:
            log_tail = read_log_tail(log_file)
            raise RuntimeError(
                f"{module_name} exited during startup with code {return_code}.\n"
                f"Recent log output:\n{log_tail}"
            )

        time.sleep(2)

    print(
        f"{module_name} stayed alive for "
        f"{startup_timeout_seconds} seconds after launch."
    )


def start_streaming_job(
    module_name: str,
    startup_timeout_seconds: int = PROCESS_STARTUP_TIMEOUT_SECONDS,
) -> None:
    os.makedirs(STREAM_LOG_DIR, exist_ok=True)

    existing_pids = get_running_pids(module_name)
    if existing_pids:
        print(f"{module_name} already running. PIDs: {', '.join(existing_pids)}")
        return

    log_file = os.path.join(
        STREAM_LOG_DIR,
        f"{module_name.replace('.', '_')}.log",
    )

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    with open(log_file, "a", buffering=1) as log_handle:
        process = subprocess.Popen(
            ["python", "-m", module_name],
            cwd=PROJECT_ROOT,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    print(f"Started {module_name} with PID {process.pid}")
    wait_for_process_startup(
        process=process,
        module_name=module_name,
        log_file=log_file,
        startup_timeout_seconds=startup_timeout_seconds,
    )


def verify_streaming_jobs(module_names: list[str]) -> None:
    missing_modules = []

    for module_name in module_names:
        pids = get_running_pids(module_name)

        if not pids:
            missing_modules.append(module_name)
        else:
            print(f"{module_name} healthy. PIDs: {', '.join(pids)}")

    if missing_modules:
        raise RuntimeError(
            "The following streaming jobs are not running: "
            + ", ".join(missing_modules)
        )


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=5),
    "on_failure_callback": dag_failure_handler,
}


with DAG(
    dag_id="sentinelpay_streaming_control",
    description="Production startup control for SentinelPay Bronze and Silver streaming jobs.",
    start_date=datetime(2026, 8, 30),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=15),
    default_args=default_args,
    tags=["sentinelpay", "streaming", "bronze", "silver", "control"],
) as dag:
    start = EmptyOperator(task_id="start")
    bronze_started = EmptyOperator(task_id="bronze_started")
    silver_started = EmptyOperator(task_id="silver_started")

    bronze_tasks = []
    for job in BRONZE_STREAM_JOBS:
        task = PythonOperator(
            task_id=job["task_id"],
            python_callable=start_streaming_job,
            op_kwargs={
                "module_name": job["module_name"],
                "startup_timeout_seconds": PROCESS_STARTUP_TIMEOUT_SECONDS,
            },
        )
        bronze_tasks.append(task)
        start >> task >> bronze_started

    silver_tasks = []
    for job in SILVER_STREAM_JOBS:
        task = PythonOperator(
            task_id=job["task_id"],
            python_callable=start_streaming_job,
            op_kwargs={
                "module_name": job["module_name"],
                "startup_timeout_seconds": PROCESS_STARTUP_TIMEOUT_SECONDS,
            },
        )
        silver_tasks.append(task)
        bronze_started >> task >> silver_started

    verify_streaming_processes = PythonOperator(
        task_id="verify_streaming_processes",
        python_callable=verify_streaming_jobs,
        op_kwargs={"module_names": ALL_STREAM_MODULES},
    )

    silver_started >> verify_streaming_processes