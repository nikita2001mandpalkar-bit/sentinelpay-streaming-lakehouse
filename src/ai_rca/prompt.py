def build_rca_prompt(dag_id:str,task_id:str,run_id:str,log_context:str,)->str:
    return f"""

You are an RCA assistant for a data engineering pipeline failure.

Analyze the failure using only the log snippet below.

DAG ID: {dag_id}
Task ID: {task_id}
Run ID: {run_id}

Relevant log snippet:
{log_context}

Return only these 3 sections:

1. Root cause
2. Classification
Say only one: transient or persistent
3. Recommended next action

Be concise and specific.
""".strip()