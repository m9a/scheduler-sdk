"""
Auto-configures MLflow when MLFLOW_TRACKING_URI is set. Called by __main__.py
before running the job — job authors write zero MLflow plumbing code.

Uses mlflow.autolog() — MLflow's own framework integration (covers Lightning,
torch, sklearn, ...). No monkey-patching of user frameworks: progress reporting
to the scheduler is the user calling ctx.progress() in their own code.

Non-training containers don't install mlflow, so setup_mlflow() is a no-op.
"""

import os


def setup_mlflow(job_name, job_id):
    """Points MLflow at the tracking server and enables autologging."""
    try:
        import mlflow
    except ImportError:
        return

    uri = os.environ.get("MLFLOW_TRACKING_URI")
    if not uri:
        raise RuntimeError(
            "MLFLOW_TRACKING_URI not set — mlflow is installed but tracking server "
            "is not configured. Check WorkerAgent's mlflow.trackingUri setting."
        )

    mlflow.set_tracking_uri(uri)
    # One experiment per job definition; runs within it are individual executions.
    mlflow.set_experiment(job_name)
    mlflow.autolog()
