"""
Sends task status updates from the job_runner to WorkerAgent's
WebSocket server as binary proto frames. Mirrors StatusUpdate.java
in the Java SDK.

    job_runner (child process)
      └─ Reporter
           ├─ task_started()      ── ws: [0x01][StatusUpdate]  ──► WorkerAgent
           ├─ module.run(params)                                    (parent JVM)
           └─ task_completed()    ── ws: [0x01][StatusUpdate]  ──► WorkerAgent
"""

import time
from typing import Optional

import websocket

from scheduler.v1 import common_pb2
from scheduler.v1 import job_message_pb2

# Must match WorkerAgent.TYPE_TAG_STATUS and Java SDK's StatusUpdate.TYPE_TAG_STATUS
TYPE_TAG_STATUS = 0x01


class Reporter:

    def __init__(self, worker_agent_url: str, job_id: str):
        self._url = worker_agent_url
        self._job_id = job_id
        self._start_times: dict[int, int] = {}
        self._ws: Optional[websocket.WebSocket] = None
        self._connect()

    def _connect(self) -> None:
        try:
            self._ws = websocket.WebSocket()
            self._ws.connect(self._url)
        except Exception as e:
            print(f"[job_runner] failed to open WebSocket to {self._url}: {e}")
            self._ws = None

    def close(self) -> None:
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:
                pass
            self._ws = None

    def task_started(self, index: int, name: str) -> None:
        self._start_times[index] = _now_ms()
        self._send_status(index, name, common_pb2.TASK_STATUS_RUNNING, 0)

    def task_completed(self, index: int, name: str) -> None:
        duration_ms = _now_ms() - self._start_times.pop(index, _now_ms())
        self._send_status(index, name, common_pb2.TASK_STATUS_COMPLETED, duration_ms)

    def task_failed(self, index: int, name: str, error: str) -> None:
        duration_ms = _now_ms() - self._start_times.pop(index, _now_ms())
        self._send_status(index, name, common_pb2.TASK_STATUS_FAILED, duration_ms, error)

    def _send_status(self, index: int, name: str, task_status: int,
                     duration_ms: int, error: Optional[str] = None) -> None:
        msg = job_message_pb2.StatusUpdate(
            job_id=self._job_id,
            task_index=index,
            task_name=name,
            task_status=task_status,
            duration_ms=duration_ms,
        )
        if error is not None:
            msg.error_message = error

        payload = bytes([TYPE_TAG_STATUS]) + msg.SerializeToString()
        self._send_binary(payload)

    def _send_binary(self, data: bytes) -> None:
        if self._ws is None:
            self._connect()

        if self._ws is not None:
            try:
                self._ws.send_binary(data)
                return
            except Exception as e:
                print(f"[job_runner] WebSocket send failed, reconnecting: {e}")
                self._ws = None

        # Reconnect and retry once
        self._connect()
        if self._ws is not None:
            try:
                self._ws.send_binary(data)
            except Exception as e:
                print(f"[job_runner] WebSocket send failed after reconnect: {e}")


def _now_ms() -> int:
    return int(time.time() * 1000)
