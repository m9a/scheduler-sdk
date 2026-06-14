"""
TaskContext — the single object injected into every @task method, and the entire
user-facing API inside a task. Mirrors the Java SDK's TaskContext. Created per
task by the executor; job authors never construct one in production (use
TaskContext() with no reporter for unit tests — calls become no-ops).

    @task("train", order=1)
    def train(self, ctx):
        for epoch in range(self.epochs):
            ...
            ctx.progress(epoch + 1, self.epochs)
            ctx.metric("loss", loss)
        ctx.event("checkpoint_saved", f"epoch {self.epochs}")

Three verbs only: progress (drives %/ETA/stall), metric (number or string,
latest-wins), event (one-off note, sent immediately). Inter-task data passing
uses instance fields on the @job class, not the context.
"""

from scheduler.v1 import common_pb2 as _pb


class TaskContext:

    def __init__(self, reporter=None, task_index=0, task_name=None):
        self._reporter = reporter
        self._task_index = task_index
        self.task_name = task_name

    def progress(self, current, total=None):
        """Report progress as "current of total" — drives %, ETA, stall detection.
        total may be omitted when the end is unknown (streaming)."""
        if self._reporter is None:
            return
        self._reporter.report(self._task_index, "progress_current",
                              _pb.REPORT_KIND_METRIC, float(current))
        if total is not None:
            self._reporter.report(self._task_index, "progress_total",
                                  _pb.REPORT_KIND_METRIC, float(total))

    def metric(self, key, value):
        """A measurement, latest-wins. Numbers are metrics; strings are run facts."""
        if self._reporter is None:
            return
        if isinstance(value, str):
            self._reporter.report(self._task_index, key, _pb.REPORT_KIND_ATTRIBUTE, value)
        else:
            self._reporter.report(self._task_index, key, _pb.REPORT_KIND_METRIC, float(value))

    def event(self, name, detail=""):
        """A one-off note (checkpoint_saved, early_stop). Sent immediately."""
        if self._reporter is None:
            return
        self._reporter.report(self._task_index, name, _pb.REPORT_KIND_EVENT,
                              str(detail), force=True)
