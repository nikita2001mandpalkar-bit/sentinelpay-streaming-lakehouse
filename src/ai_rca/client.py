def call_llm(prompt: str) -> str:
    prompt_upper = prompt.upper()

    if "OOM_KILLED" in prompt_upper or "OOMKILLED" in prompt_upper:
        return (
            "1. Root cause\n"
            "Container or Spark process ran out of memory.\n\n"
            "2. Classification\n"
            "persistent\n\n"
            "3. Recommended next action\n"
            "Reduce Spark memory pressure, stop parallel heavy jobs, and rerun the task."
        )

    if "RPCENDPOINTNOTFOUND" in prompt_upper or "CONNECTION REFUSED" in prompt_upper:
        return (
            "1. Root cause\n"
            "Spark driver or Spark session died during execution.\n\n"
            "2. Classification\n"
            "transient\n\n"
            "3. Recommended next action\n"
            "Restart the Airflow/Spark runtime, verify no stale Spark jobs are running, and rerun the task."
        )

    if "TABLE_OR_VIEW_NOT_FOUND" in prompt_upper:
        return (
            "1. Root cause\n"
            "An upstream table or view required by the model was missing.\n\n"
            "2. Classification\n"
            "persistent\n\n"
            "3. Recommended next action\n"
            "Build or register the missing upstream dataset first, then rerun the downstream task."
        )

    if "PARSE_SYNTAX_ERROR" in prompt_upper:
        return (
            "1. Root cause\n"
            "SQL or Jinja syntax error in the model/query.\n\n"
            "2. Classification\n"
            "persistent\n\n"
            "3. Recommended next action\n"
            "Review the compiled SQL and fix the syntax before rerunning."
        )

    if "PATH_NOT_FOUND" in prompt_upper:
        return (
            "1. Root cause\n"
            "Expected Delta or MinIO path does not exist.\n\n"
            "2. Classification\n"
            "persistent\n\n"
            "3. Recommended next action\n"
            "Create or rebuild the missing upstream dataset/path, then rerun the task."
        )

    return (
        "1. Root cause\n"
        "Unable to determine an exact root cause from the filtered log snippet.\n\n"
        "2. Classification\n"
        "transient\n\n"
        "3. Recommended next action\n"
        "Inspect the filtered failure log and add a new RCA rule for this failure pattern."
    )