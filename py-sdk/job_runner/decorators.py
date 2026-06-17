import re


def job(cls=None, *, id=None, description="", timeout_seconds=0, max_retries=0):
    """Class decorator marking a class as a job definition."""

    def wrap(cls):
        job_id = id if id is not None else _snake_case(cls.__name__)
        cls._job_meta = {
            "id": job_id,
            "description": description,
            "timeout_seconds": timeout_seconds,
            "max_retries": max_retries,
        }
        return cls

    if cls is not None:
        # Bare @job without parentheses
        return wrap(cls)
    return wrap


def task(name, *, order=0):
    """Method decorator marking a method as a task."""

    def wrap(method):
        method._task_meta = {"name": name, "order": order}
        return method

    return wrap


def before_job(method):
    """Method decorator for a pre-task hook. At most one per class."""
    method._before_job = True
    return method


def after_job(method):
    """Method decorator for a post-task hook. At most one per class."""
    method._after_job = True
    return method


def on_shutdown(method):
    """Method decorator for a graceful-shutdown hook. Runs (best-effort) when the
    worker terminates the container (SIGTERM) before the SIGKILL grace expires —
    not on normal completion. Use it to flush a checkpoint to /workspace/output
    or emit a final event. Takes (self) or (self, ctx). At most one per class."""
    method._on_shutdown = True
    return method


def _snake_case(name):
    """CamelCase to snake_case: 'DailySalesEtlJob' -> 'daily_sales_etl_job'."""
    return re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", name).lower()
