"""
Sends task status updates from the job_runner to WorkerAgent's /task-status
HTTP endpoint. Mirrors JobReporter.java in the Java SDK.

    job_runner (child process)
      └─ Reporter
           ├─ task_started()   ── HTTP POST /task-status ──► WorkerAgent
           ├─ module.run(params)                              (parent JVM)
           └─ task_completed() ── HTTP POST /task-status ──► WorkerAgent
"""

import json
import time
import urllib.request
from typing import Optional


class Reporter:

    def __init__(self, worker_agent_url: str, job_id: str):
        self._url = worker_agent_url + "/task-status"
        self._job_id = job_id
        self._start_times: dict[int, int] = {}

    def task_started(self, index: int, name: str) -> None:
        self._start_times[index] = _now_ms()
        self._send(index, name, "RUNNING", 0)

    def task_completed(self, index: int, name: str) -> None:
        duration_ms = _now_ms() - self._start_times.pop(index, _now_ms())
        self._send(index, name, "COMPLETED", duration_ms)

    def task_failed(self, index: int, name: str, error: str) -> None:
        duration_ms = _now_ms() - self._start_times.pop(index, _now_ms())
        self._send(index, name, "FAILED", duration_ms, error)

    def _send(self, index: int, name: str, status: str, duration_ms: int,
              error: Optional[str] = None) -> None:
        body: dict = {
            "jobId": self._job_id,
            "taskIndex": index,
            "taskName": name,
            "status": status,
            "durationMs": duration_ms,
        }
        if error is not None:
            body["errorMessage"] = error

        # Compact format (no spaces) to match Java's TaskStatusUpdate.toJson()
        data = json.dumps(body, separators=(",", ":")).encode("utf-8")
        req = urllib.request.Request(
            self._url, data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req) as resp:
                if resp.status != 200:
                    print(f"[job_runner] status report got HTTP {resp.status}: "
                          f"task={name}, status={status}")
        except Exception as e:
            print(f"[job_runner] failed to report status for task {name}: {e}")


def _now_ms() -> int:
    return int(time.time() * 1000)
