from collections import deque
from pathlib import Path

def read_last_log_lines(log_path:str,limit:int=300)->list[str]:
    path=Path(log_path)
    if not path.exists():
        return []

    with path.open("r",encoding="utf-8",errors="replace") as file:
        return list(deque(file,maxlen=limit))


ERROR_PATTERNS=(
    "ERROR",
    "Exception",
    "Traceback",
    "Caused by",
    "Runtime Error",
    "FAIL",
    "Task failed"
)

NOISE_PATTERNS=(
    "Stage",
    "artifacts copied",
    "already retrieved",
    "found org.apache",
    "Using Spark's default log4j profile",
    "Setting default log level",
    "NativeCodeLoader",
    "MetricsConfig",
)

def extract_relevant_log_lines(lines:list[str])->list[str]:
    filtered_lines=[]

    for line in lines:
        if any(noise in line for noise in NOISE_PATTERNS):
            continue
        if any(pattern in line for pattern in ERROR_PATTERNS):
            filtered_lines.append(line.rstrip())

    return filtered_lines

def extract_failure_window(lines:list[str],context_before:int=20,context_after:int=20,)->list[str]:
    error_indexes=[]

    for index,line in enumerate(lines):
        if any(pattern in line for pattern in ERROR_PATTERNS):
            error_indexes.append(index)
    if not error_indexes:
        return [line.rstrip() for line in lines[-80:]]

    start=max(0,error_indexes[0]-context_before)
    end=min(len(lines),error_indexes[-1]+context_after+1)

    return [line.rstrip() for line in lines[start:end]]

def get_filtered_log_context(log_path:str,limit:int=300)->str:
    lines=read_last_log_lines(log_path,limit=limit)

    if not lines:
        return "No log lines found......"

    failure_window=extract_failure_window(lines)
    relevant_lines=extract_relevant_log_lines(failure_window)

    if relevant_lines:
        return "\n".join(relevant_lines)

    return "\n".join(line.rstrip() for line in failure_window)