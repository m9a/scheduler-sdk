"""
Sends task status updates and key-value telemetry from the job_runner to
WorkerAgent's WebSocket server as binary proto frames over one persistent
connection. Mirrors the Java SDK.

    job_runner (child process)
      └─ Reporter
           ├─ task_started()      ── ws: [0x01][StatusUpdate]  ──► WorkerAgent
           │                          ◄── ws: [0x02 ack] ──────────  (parent JVM)
           ├─ module.run(params)
           │    └─ progress()/metric()/...  ── ws: [0x03][Report] ──► WorkerAgent
           │    (idle)             ── ws: [0x04][Liveness] ──────► WorkerAgent
           └─ task_completed()    ── ws: [0x01][StatusUpdate]  ──► WorkerAgent
                                      ◄── ws: [0x02 ack] ──────────

Status frames drive the job state machine, so they must not be lost: each waits
for the worker's ack and is resent on a fresh connection if it doesn't arrive.
Telemetry is fire-and-forget. Reports are throttled: at most one frame per
REPORT_INTERVAL_S, keeping only the latest value per key (older numbers dropped
-- full history goes to MLflow). EVENT entries force an immediate flush.
"""

import threading
import time
from typing import Optional

import websocket

from scheduler.v1 import common_pb2
from scheduler.v1 import job_callback_pb2

# Must match WorkerAgent.TYPE_TAG_STATUS and Java SDK's StatusUpdate.TYPE_TAG_STATUS
TYPE_TAG_STATUS = 0x01
# Sent back by the worker to confirm a status frame was received and forwarded.
TYPE_TAG_ACK = 0x02
# Must match WorkerAgent.TYPE_TAG_REPORT and Java SDK's TaskContext report tag
TYPE_TAG_REPORT = 0x03
# Liveness ping (SDK -> worker only; consumed locally for stall detection).
TYPE_TAG_LIVENESS = 0x04
# Minimum seconds between report frames (throttle).
REPORT_INTERVAL_S = 1.0
# How long to wait for the worker's ack before resending a status frame.
ACK_TIMEOUT_S = 10.0
MAX_SEND_ATTEMPTS = 3
# How often to ping while a task runs (must be < the worker's stall probe interval).
LIVENESS_INTERVAL_S = 15.0


class Reporter:

    def __init__(self, worker_agent_url: str, job_id: str):
        self._url = worker_agent_url
        self._job_id = job_id
        self._start_times: dict[int, int] = {}
        self._ws: Optional[websocket.WebSocket] = None
        # Throttle buffer: (task_index, key) -> latest ReportEntry, flushed in batches.
        self._report_buffer: dict[tuple, common_pb2.ReportEntry] = {}
        self._last_flush = time.monotonic()
        self._report_lock = threading.Lock()   # guards the report buffer
        self._send_lock = threading.Lock()     # serializes wire access (one send at a time)
        # Drives the idle liveness ping: time of the last frame sent.
        self._last_send = time.monotonic()
        self._stop = threading.Event()
        self._connect()
        self._send_liveness()  # initial ping so the worker sees proof-of-life promptly
        self._liveness_thread = threading.Thread(
            target=self._liveness_loop, name="liveness-ping", daemon=True)
        self._liveness_thread.start()

    def _connect(self) -> None:
        try:
            self._ws = websocket.WebSocket()
            self._ws.connect(self._url)
        except Exception as e:
            print(f"[job_runner] failed to open WebSocket to {self._url}: {e}")
            self._ws = None

    def close(self) -> None:
        self._stop.set()
        self.flush()
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:
                pass
            self._ws = None

    def task_started(self, index: int, name: str) -> None:
        self._start_times[index] = _now_ms()
        self._send_status(index, name, common_pb2.TASK_STATE_RUNNING, 0)

    def task_completed(self, index: int, name: str) -> None:
        duration_ms = _now_ms() - self._start_times.pop(index, _now_ms())
        self._send_status(index, name, common_pb2.TASK_STATE_COMPLETED, duration_ms)

    def task_failed(self, index: int, name: str, error: str) -> None:
        duration_ms = _now_ms() - self._start_times.pop(index, _now_ms())
        self._send_status(index, name, common_pb2.TASK_STATE_FAILED, duration_ms, error)

    def _send_liveness(self) -> None:
        try:
            msg = job_callback_pb2.Liveness(job_id=self._job_id, timestamp_ms=_now_ms())
            self._send_binary(bytes([TYPE_TAG_LIVENESS]) + msg.SerializeToString())
        except Exception as e:
            print(f"[job_runner] liveness ping failed: {e}")

    def _liveness_loop(self) -> None:
        """Pings the worker (container-alive signal) when nothing was sent for an interval."""
        while not self._stop.wait(LIVENESS_INTERVAL_S):
            if (time.monotonic() - self._last_send) >= LIVENESS_INTERVAL_S:
                self._send_liveness()

    def report(self, task_index: int, key: str, kind: int, value, *, force: bool = False) -> None:
        """Buffers one key-value entry; sends a batched Report frame when due."""
        entry = common_pb2.ReportEntry(key=key, kind=kind)
        if isinstance(value, str):
            entry.str_value = value
        else:
            entry.num_value = float(value)
        with self._report_lock:
            self._report_buffer[(task_index, key)] = entry  # latest-wins per key
            if force or (time.monotonic() - self._last_flush) >= REPORT_INTERVAL_S:
                self._flush_locked()

    def flush(self) -> None:
        """Sends all buffered report entries now (called at task end / close)."""
        with self._report_lock:
            self._flush_locked()

    def _flush_locked(self) -> None:
        self._last_flush = time.monotonic()
        if not self._report_buffer:
            return
        # Group buffered entries by task so each Report carries one task's entries.
        by_task: dict[int, list] = {}
        for (task_index, _key), entry in self._report_buffer.items():
            by_task.setdefault(task_index, []).append(entry)
        self._report_buffer.clear()

        timestamp_ms = _now_ms()
        for task_index, entries in by_task.items():
            msg = job_callback_pb2.Report(
                job_id=self._job_id,
                task_index=task_index,
                timestamp_ms=timestamp_ms,
                entries=entries,
            )
            payload = bytes([TYPE_TAG_REPORT]) + msg.SerializeToString()
            self._send_binary(payload)

    def _send_status(self, index: int, name: str, task_state: int,
                     duration_ms: int, error: Optional[str] = None) -> None:
        msg = job_callback_pb2.StatusUpdate(
            job_id=self._job_id,
            task_index=index,
            task_name=name,
            task_state=task_state,
            duration_ms=duration_ms,
        )
        if error is not None:
            msg.error_message = error

        payload = bytes([TYPE_TAG_STATUS]) + msg.SerializeToString()
        self._send_with_ack(payload)

    def _send_with_ack(self, data: bytes) -> None:
        """Sends a status frame on the persistent connection and waits for the
        worker's ack, resending on a fresh connection if it doesn't arrive.

        Status updates drive the job state machine, so losing one is fatal to
        the job's outcome (a task stuck RUNNING fails the whole job). A half-open
        socket can swallow a send without raising — the ack is the only proof of
        delivery.
        """
        last_error = None
        with self._send_lock:
            self._last_send = time.monotonic()  # any send counts as activity
            for _ in range(MAX_SEND_ATTEMPTS):
                try:
                    if self._ws is None:
                        self._connect()
                    self._ws.settimeout(ACK_TIMEOUT_S)
                    self._ws.send_binary(data)
                    ack = self._ws.recv()  # worker's only inbound frame is the ack
                    if ack and ack[0] == TYPE_TAG_ACK:
                        return
                    last_error = RuntimeError(f"unexpected ack frame: {ack!r}")
                except Exception as e:
                    last_error = e
                self._reset_connection()  # delivery unconfirmed — retry on a fresh socket
        print(f"[job_runner] status send failed after {MAX_SEND_ATTEMPTS} attempts: {last_error}")

    def _send_binary(self, data: bytes) -> None:
        """Sends a telemetry/liveness frame (fire-and-forget — lossy by design)."""
        with self._send_lock:
            self._last_send = time.monotonic()  # any send counts as activity
            try:
                if self._ws is None:
                    self._connect()
                if self._ws is not None:
                    self._ws.send_binary(data)
            except Exception as e:
                print(f"[job_runner] telemetry send failed: {e}")
                self._reset_connection()

    def _reset_connection(self) -> None:
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:
                pass
            self._ws = None


def _now_ms() -> int:
    return int(time.time() * 1000)
