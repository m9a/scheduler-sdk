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
import traceback

from job_runner.context import TaskContext


def run_job(job_class, reporter, params):
    """Instantiates the @job class, injects params, runs the task lifecycle."""
    tasks, before, after = _discover(job_class)
    tasks.sort(key=lambda t: t["order"])

    instance = _construct(job_class, params)

    try:
        if before is not None:
            before(instance)

        for index, task_info in enumerate(tasks):
            name = task_info["name"]
            method = task_info["method"]
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


def _discover(job_class):
    """Scans the class for @task methods, @before_job, and @after_job."""
    tasks = []
    before = None
    after = None

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

    return tasks, before, after


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
