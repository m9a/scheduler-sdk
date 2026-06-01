import os
import sys
from unittest.mock import MagicMock, patch

import pytest

from scheduler.v1 import common_pb2
from scheduler.v1 import job_message_pb2


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

    def test_patches_trainer(self, monkeypatch):
        """Patches Trainer.__init__ to inject MLFlowLogger."""
        monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")

        init_calls = []

        class FakeTrainer:
            def __init__(self, **kwargs):
                init_calls.append(kwargs)

        mock_mlflow_logger_cls = MagicMock()

        mock_pl = MagicMock()
        mock_pl.Trainer = FakeTrainer
        mock_pl_loggers = MagicMock()
        mock_pl_loggers.MLFlowLogger = mock_mlflow_logger_cls

        with patch.dict(sys.modules, {
            "mlflow": MagicMock(),
            "pytorch_lightning": mock_pl,
            "pytorch_lightning.loggers": mock_pl_loggers,
        }):
            from job_runner.metrics import setup_mlflow
            setup_mlflow("mnist-lightning", "job-456")

            # Instantiate without explicit logger — should inject MLFlowLogger
            FakeTrainer(max_epochs=3)

            mock_mlflow_logger_cls.assert_called_once_with(
                experiment_name="mnist-lightning",
                run_name="job-456",
                tracking_uri="http://mlflow:5000",
                tags={"scheduler.job_id": "job-456", "scheduler.job_name": "mnist-lightning"},
            )

    def test_respects_explicit_logger(self, monkeypatch):
        """Does not override when user passes logger explicitly."""
        monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")

        init_calls = []

        class FakeTrainer:
            def __init__(self, **kwargs):
                init_calls.append(kwargs)

        mock_mlflow_logger_cls = MagicMock()

        mock_pl = MagicMock()
        mock_pl.Trainer = FakeTrainer
        mock_pl_loggers = MagicMock()
        mock_pl_loggers.MLFlowLogger = mock_mlflow_logger_cls

        with patch.dict(sys.modules, {
            "mlflow": MagicMock(),
            "pytorch_lightning": mock_pl,
            "pytorch_lightning.loggers": mock_pl_loggers,
        }):
            from job_runner.metrics import setup_mlflow
            setup_mlflow("my-job", "job-789")

            # Instantiate WITH explicit logger — should NOT inject
            user_logger = MagicMock()
            FakeTrainer(logger=user_logger)

            mock_mlflow_logger_cls.assert_not_called()


class TestReporterSendsBinaryProto:

    @patch("job_runner.reporter.websocket.WebSocket")
    def test_task_started_sends_binary_status(self, MockWebSocket):
        from job_runner.reporter import Reporter, TYPE_TAG_STATUS

        mock_ws = MagicMock()
        MockWebSocket.return_value = mock_ws

        reporter = Reporter("ws://localhost:8080", "job-123")
        reporter.task_started(0, "extract")

        calls = mock_ws.send_binary.call_args_list
        assert len(calls) == 1

        data = calls[0][0][0]
        assert data[0] == TYPE_TAG_STATUS
        msg = job_message_pb2.StatusUpdate()
        msg.ParseFromString(data[1:])
        assert msg.job_id == "job-123"
        assert msg.task_index == 0
        assert msg.task_name == "extract"
        assert msg.task_status == common_pb2.TASK_STATUS_RUNNING

        reporter.close()

    @patch("job_runner.reporter.websocket.WebSocket")
    def test_task_completed_sends_binary_status(self, MockWebSocket):
        from job_runner.reporter import Reporter, TYPE_TAG_STATUS

        mock_ws = MagicMock()
        MockWebSocket.return_value = mock_ws

        reporter = Reporter("ws://localhost:8080", "job-123")
        reporter.task_started(0, "extract")
        reporter.task_completed(0, "extract")

        calls = mock_ws.send_binary.call_args_list
        assert len(calls) == 2

        data = calls[1][0][0]
        assert data[0] == TYPE_TAG_STATUS
        msg = job_message_pb2.StatusUpdate()
        msg.ParseFromString(data[1:])
        assert msg.task_status == common_pb2.TASK_STATUS_COMPLETED
        assert msg.duration_ms >= 0

        reporter.close()

    @patch("job_runner.reporter.websocket.WebSocket")
    def test_task_failed_includes_error(self, MockWebSocket):
        from job_runner.reporter import Reporter, TYPE_TAG_STATUS

        mock_ws = MagicMock()
        MockWebSocket.return_value = mock_ws

        reporter = Reporter("ws://localhost:8080", "job-123")
        reporter.task_started(0, "extract")
        reporter.task_failed(0, "extract", "out of memory")

        calls = mock_ws.send_binary.call_args_list
        assert len(calls) == 2

        data = calls[1][0][0]
        msg = job_message_pb2.StatusUpdate()
        msg.ParseFromString(data[1:])
        assert msg.task_status == common_pb2.TASK_STATUS_FAILED
        assert msg.error_message == "out of memory"

        reporter.close()
