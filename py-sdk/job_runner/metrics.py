"""
Auto-configures MLflow when MLFLOW_TRACKING_URI is set. Called by __main__.py
before running the job — job authors write zero MLflow plumbing code.

When mlflow is installed (training containers), the SDK patches Lightning's
Trainer.__init__ to inject MLFlowLogger automatically. All self.log() calls
(including TorchMetrics) flow to MLflow without any user code changes.

Non-training containers don't install mlflow, so setup_mlflow() is a no-op.
"""

import os


def setup_mlflow(job_name, job_id):
    """Patches Lightning Trainer to auto-inject MLFlowLogger."""
    try:
        import mlflow  # noqa: F401
    except ImportError:
        return

    uri = os.environ.get("MLFLOW_TRACKING_URI")
    if not uri:
        raise RuntimeError(
            "MLFLOW_TRACKING_URI not set — mlflow is installed but tracking server "
            "is not configured. Check WorkerAgent's mlflow.trackingUri setting."
        )

    mlflow.set_tracking_uri(uri)
    _patch_lightning(job_name, job_id, uri)


def _patch_lightning(job_name, job_id, tracking_uri):
    """Monkey-patches Trainer.__init__ to inject MLFlowLogger when no logger is passed."""
    from pytorch_lightning import Trainer
    from pytorch_lightning.loggers import MLFlowLogger

    _original_init = Trainer.__init__

    def _patched_init(self, *args, **kwargs):
        if "logger" not in kwargs or kwargs.get("logger") is True:
            kwargs["logger"] = MLFlowLogger(
                experiment_name=job_name,
                run_name=job_id,
                tracking_uri=tracking_uri,
                tags={"scheduler.job_id": job_id, "scheduler.job_name": job_name},
            )
        _original_init(self, *args, **kwargs)

    Trainer.__init__ = _patched_init
