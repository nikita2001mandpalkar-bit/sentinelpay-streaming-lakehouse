from pathlib import Path
from src.ai_rca.client import call_llm
from src.ai_rca.log_filter import get_filtered_log_context
from src.ai_rca.prompt import build_rca_prompt
from src.ai_rca.storage import save_rca_report

def generate_rca_from_log(dag_id:str,task_id:str,run_id:str,log_path:str,)->str:
    log_context=get_filtered_log_context(log_path)

    prompt=build_rca_prompt(
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        log_context=log_context,
    )

    rca_result=call_llm(prompt)

    report_path=save_rca_report(
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        log_context=log_context,
        rca_result=rca_result,
    )

    return f"{rca_result}\n\nSaved RCA report to: {Path(report_path)}"