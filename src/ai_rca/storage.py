import json
from datetime import datetime
from pathlib import Path

RCA_OUTPUT_DIR=Path("src/quality/results/ai_rca_reports")

def save_rca_report(dag_id:str,task_id:str,run_id:str,log_context:str,rca_result:str,)->str:
    RCA_OUTPUT_DIR.mkdir(parents=True,exist_ok=True)

    timestamp=datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path=RCA_OUTPUT_DIR/f"{dag_id}_{task_id}_{timestamp}.json"

    payload={
        "dag_id": dag_id,
        "task_id": task_id,
        "run_id": run_id,
        "generated_at": datetime.now().isoformat(),
        "log_context": log_context,
        "rca_result": rca_result,
    }

    file_path.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )

    return str(file_path)