import signal

from job_runner.decorators import job, task, on_shutdown
from job_runner.executor import _discover, run_job


@job(id="hooked")
class _HookedJob:
    def __init__(self):
        self.ran = []

    @task("work")
    def work(self, ctx):
        self.ran.append("work")

    @on_shutdown
    def cleanup(self, ctx):
        self.ran.append("cleanup")


class _NoopReporter:
    def task_started(self, *a): pass
    def task_completed(self, *a): pass
    def task_failed(self, *a): pass
    def flush(self): pass


def test_discovers_on_shutdown():
    tasks, before, after, shutdown = _discover(_HookedJob)
    assert shutdown is not None
    assert shutdown.__name__ == "cleanup"


def test_installs_sigterm_handler_when_hook_present(monkeypatch):
    captured = {}
    monkeypatch.setattr(signal, "signal", lambda sig, handler: captured.__setitem__(sig, handler))

    run_job(_HookedJob, _NoopReporter(), {})

    assert signal.SIGTERM in captured  # the @on_shutdown hook armed a SIGTERM handler


def test_no_handler_without_hook(monkeypatch):
    @job(id="plain")
    class _PlainJob:
        @task("work")
        def work(self, ctx): pass

    captured = {}
    monkeypatch.setattr(signal, "signal", lambda sig, handler: captured.__setitem__(sig, handler))

    run_job(_PlainJob, _NoopReporter(), {})

    assert signal.SIGTERM not in captured
