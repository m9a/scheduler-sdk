"""
Entry point for Python job execution.

Supports two invocation modes:
  1. CLI: python3 -m job_runner <script.py> <base64_payload>
  2. Docker: EXECUTION_PAYLOAD=<base64_payload> python3 -m job_runner <script.py>

Decodes the execution payload (containing workerAgentUrl, jobId, params),
loads the user's script, discovers the @job-decorated class, and runs its
@task methods while reporting per-task status back to WorkerAgent via WebSocket.
"""

import base64
import importlib.util
import json
import os
import sys

from job_runner.executor import run_job
from job_runner.reporter import Reporter


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python3 -m job_runner <script.py> [<base64_payload>]",
              file=sys.stderr)
        sys.exit(1)

    script_path = sys.argv[1]

    # Payload from CLI arg or EXECUTION_PAYLOAD env var
    if len(sys.argv) >= 3:
        payload_b64 = sys.argv[2]
    else:
        payload_b64 = os.environ.get("EXECUTION_PAYLOAD")

    if not payload_b64:
        print("[job_runner] no payload: pass as arg or set EXECUTION_PAYLOAD env var",
              file=sys.stderr)
        sys.exit(1)

    payload = json.loads(base64.b64decode(payload_b64))
    worker_agent_url = payload["workerAgentUrl"]
    job_id = payload["jobId"]
    params = payload.get("params", {})

    reporter = Reporter(worker_agent_url, job_id)

    module = _load_script(script_path)
    job_class = _find_job_class(module)

    if job_class is None:
        print(f"[job_runner] no @job-decorated class found in: {script_path}",
              file=sys.stderr)
        sys.exit(1)

    from job_runner.metrics import setup_mlflow
    setup_mlflow(job_class._job_meta["id"], job_id)

    try:
        run_job(job_class, reporter, params)
    except Exception:
        sys.exit(1)
    finally:
        reporter.close()


def _load_script(path: str):
    spec = importlib.util.spec_from_file_location("_user_script", path)
    if spec is None or spec.loader is None:
        print(f"[job_runner] cannot load script: {path}", file=sys.stderr)
        sys.exit(1)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _find_job_class(module):
    """Scans module for a class with _job_meta (set by @job decorator)."""
    for name in dir(module):
        obj = getattr(module, name)
        if isinstance(obj, type) and hasattr(obj, "_job_meta"):
            return obj
    return None


if __name__ == "__main__":
    main()
