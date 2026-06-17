"""
Discovers and runs a @job-decorated class. Python equivalent of the
generated _Harness in the Java SDK.

    job_runner.__main__
      └─ executor.run_job(cls, reporter, params)
           ├─ inspect __init__ → construct instance with params
           ├─ before_job()
           ├─ for each @task (sorted by order):
           │     reporter.task_started(i, name)
           │     task_method(ctx)        ← per-task TaskContext, the reporting API
           │     reporter.task_completed(i, name)
           └─ after_job()   (finally — always runs)
"""

import inspect
import os
import signal
import traceback

from job_runner.context import TaskContext


def run_job(job_class, reporter, params):
    """Instantiates the @job class, injects params, runs the task lifecycle."""
    tasks, before, after, on_shutdown = _discover(job_class)
    tasks.sort(key=lambda t: t["order"])

    instance = _construct(job_class, params)

    # Tracks the running task so the shutdown hook gets its context.
    current = {"index": 0, "name": None}
    if on_shutdown is not None:
        _install_shutdown_handler(instance, reporter, on_shutdown, current)

    try:
        if before is not None:
            before(instance)

        for index, task_info in enumerate(tasks):
            name = task_info["name"]
            method = task_info["method"]
            current["index"], current["name"] = index, name
            ctx = TaskContext(reporter, index, name)
            reporter.task_started(index, name)
            try:
                method(instance, ctx)
                reporter.flush()  # send any telemetry buffered during the task
                reporter.task_completed(index, name)
            except Exception:
                reporter.flush()
                reporter.task_failed(index, name, traceback.format_exc())
                raise
    finally:
        if after is not None:
            after(instance)


def _install_shutdown_handler(instance, reporter, on_shutdown, current):
    """On SIGTERM (the worker's graceful stop), run the @on_shutdown hook, flush, exit."""
    def handler(_signum, _frame):
        try:
            if len(inspect.signature(on_shutdown).parameters) >= 2:
                on_shutdown(instance, TaskContext(reporter, current["index"], current["name"]))
            else:
                on_shutdown(instance)
        except Exception:
            print(f"[job_runner] @on_shutdown failed:\n{traceback.format_exc()}")
        reporter.flush()
        os._exit(0)

    signal.signal(signal.SIGTERM, handler)


def _discover(job_class):
    """Scans the class for @task methods, @before_job, @after_job, and @on_shutdown."""
    tasks = []
    before = None
    after = None
    on_shutdown = None

    for name in dir(job_class):
        attr = getattr(job_class, name)
        if not callable(attr):
            continue
        if hasattr(attr, "_task_meta"):
            tasks.append({**attr._task_meta, "method": attr})
        if getattr(attr, "_before_job", False):
            before = attr
        if getattr(attr, "_after_job", False):
            after = attr
        if getattr(attr, "_on_shutdown", False):
            on_shutdown = attr

    return tasks, before, after, on_shutdown


def _construct(job_class, params):
    """Instantiates the job class, injecting params matched by __init__ signature."""
    sig = inspect.signature(job_class.__init__)
    kwargs = {}

    for param_name, param in sig.parameters.items():
        if param_name == "self":
            continue
        if param_name in params:
            value = params[param_name]
            annotation = param.annotation
            if annotation != inspect.Parameter.empty:
                value = annotation(value)
            kwargs[param_name] = value
        elif param.default is not inspect.Parameter.empty:
            pass  # use the default
        # else: missing required param — let Python raise TypeError naturally

    return job_class(**kwargs)
