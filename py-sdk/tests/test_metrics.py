import os
import sys
from unittest.mock import MagicMock, patch

import pytest

from scheduler.v1 import common_pb2
from scheduler.v1 import job_callback_pb2


def _status_frames(mock_ws, status_tag):
    """Status frames sent on the socket (excludes fire-and-forget liveness pings)."""
    return [c[0][0] for c in mock_ws.send_binary.call_args_list if c[0][0][0] == status_tag]


class TestSetupMlflow:

    def test_noop_when_mlflow_not_installed(self, monkeypatch):
        """setup_mlflow returns silently when mlflow is not importable."""
        import importlib
        import job_runner.metrics
        importlib.reload(job_runner.metrics)

        original_import = __builtins__.__import__ if hasattr(__builtins__, '__import__') else __import__
        def mock_import(name, *args, **kwargs):
            if name == "mlflow":
                raise ImportError("no mlflow")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            # Should not raise
            job_runner.metrics.setup_mlflow("my-job", "job-123")

    def test_fails_when_uri_not_set(self, monkeypatch):
        """Raises RuntimeError when mlflow is installed but MLFLOW_TRACKING_URI is missing."""
        monkeypatch.delenv("MLFLOW_TRACKING_URI", raising=False)

        with patch("builtins.__import__", wraps=__import__):
            from job_runner.metrics import setup_mlflow
            with pytest.raises(RuntimeError, match="MLFLOW_TRACKING_URI not set"):
                setup_mlflow("my-job", "job-123")

    def test_configures_tracking_and_autolog(self, monkeypatch):
        """Sets tracking URI + experiment and enables mlflow.autolog() — no patching."""
        monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")

        mock_mlflow = MagicMock()
        with patch.dict(sys.modules, {"mlflow": mock_mlflow}):
            from job_runner.metrics import setup_mlflow
            setup_mlflow("mnist-lightning", "job-456")

        mock_mlflow.set_tracking_uri.assert_called_once_with("http://mlflow:5000")
        mock_mlflow.set_experiment.assert_called_once_with("mnist-lightning")
        mock_mlflow.autolog.assert_called_once_with()


class TestReporterSendsBinaryProto:

    @patch("job_runner.reporter.websocket.WebSocket")
    def test_task_started_sends_binary_status(self, MockWebSocket):
        from job_runner.reporter import Reporter, TYPE_TAG_STATUS, TYPE_TAG_ACK

        mock_ws = MagicMock()
        mock_ws.recv.return_value = bytes([TYPE_TAG_ACK])  # worker confirms each status frame
        MockWebSocket.return_value = mock_ws

        reporter = Reporter("ws://localhost:8080", "job-123")
        reporter.task_started(0, "extract")

        statuses = _status_frames(mock_ws, TYPE_TAG_STATUS)
        assert len(statuses) == 1

        msg = job_callback_pb2.StatusUpdate()
        msg.ParseFromString(statuses[0][1:])
        assert msg.job_id == "job-123"
        assert msg.task_index == 0
        assert msg.task_name == "extract"
        assert msg.task_state == common_pb2.TASK_STATE_RUNNING

        reporter.close()

    @patch("job_runner.reporter.websocket.WebSocket")
    def test_task_completed_sends_binary_status(self, MockWebSocket):
        from job_runner.reporter import Reporter, TYPE_TAG_STATUS, TYPE_TAG_ACK

        mock_ws = MagicMock()
        mock_ws.recv.return_value = bytes([TYPE_TAG_ACK])  # worker confirms each status frame
        MockWebSocket.return_value = mock_ws

        reporter = Reporter("ws://localhost:8080", "job-123")
        reporter.task_started(0, "extract")
        reporter.task_completed(0, "extract")

        statuses = _status_frames(mock_ws, TYPE_TAG_STATUS)
        assert len(statuses) == 2

        msg = job_callback_pb2.StatusUpdate()
        msg.ParseFromString(statuses[1][1:])
        assert msg.task_state == common_pb2.TASK_STATE_COMPLETED
        assert msg.duration_ms >= 0

        reporter.close()

    @patch("job_runner.reporter.websocket.WebSocket")
    def test_task_failed_includes_error(self, MockWebSocket):
        from job_runner.reporter import Reporter, TYPE_TAG_STATUS, TYPE_TAG_ACK

        mock_ws = MagicMock()
        mock_ws.recv.return_value = bytes([TYPE_TAG_ACK])  # worker confirms each status frame
        MockWebSocket.return_value = mock_ws

        reporter = Reporter("ws://localhost:8080", "job-123")
        reporter.task_started(0, "extract")
        reporter.task_failed(0, "extract", "out of memory")

        statuses = _status_frames(mock_ws, TYPE_TAG_STATUS)
        assert len(statuses) == 2

        msg = job_callback_pb2.StatusUpdate()
        msg.ParseFromString(statuses[1][1:])
        assert msg.task_state == common_pb2.TASK_STATE_FAILED
        assert msg.error_message == "out of memory"

        reporter.close()
