"""Tests for key-value reporting: throttle/flush behavior and TaskContext."""

import threading
import time

from job_runner.context import TaskContext
from job_runner.reporter import Reporter, TYPE_TAG_REPORT
from scheduler.v1 import common_pb2
from scheduler.v1 import job_callback_pb2


def _reporter():
    """A Reporter with the WebSocket replaced by an in-memory capture list."""
    r = Reporter.__new__(Reporter)
    r._job_id = "job-1"
    r._report_buffer = {}
    r._last_flush = time.monotonic()  # start the throttle window now
    r._report_lock = threading.Lock()
    r.sent = []
    r._send_binary = lambda data: r.sent.append(data)
    return r


def _reports(reporter):
    """Decode captured [0x03] frames into Report messages."""
    out = []
    for frame in reporter.sent:
        assert frame[0] == TYPE_TAG_REPORT
        out.append(job_callback_pb2.Report.FromString(frame[1:]))
    return out


def test_event_flushes_immediately():
    r = _reporter()
    ctx = TaskContext(r, 0, "train")
    ctx.event("checkpoint_saved", "step 5000")
    reports = _reports(r)
    assert len(reports) == 1
    assert reports[0].entries[0].key == "checkpoint_saved"
    assert reports[0].entries[0].str_value == "step 5000"
    assert reports[0].entries[0].kind == common_pb2.REPORT_KIND_EVENT


def test_metric_throttled_then_flush():
    r = _reporter()
    ctx = TaskContext(r, 0, "train")
    ctx.metric("loss", 0.5)
    ctx.metric("loss", 0.2)  # latest-wins
    assert r.sent == []
    r.flush()
    reports = _reports(r)
    assert len(reports) == 1
    assert len(reports[0].entries) == 1
    assert reports[0].entries[0].num_value == 0.2


def test_progress_keys():
    r = _reporter()
    ctx = TaskContext(r, 2, "train")
    ctx.progress(3, 10)
    r.flush()
    report = _reports(r)[0]
    assert report.task_index == 2
    keys = {e.key: e for e in report.entries}
    assert keys["progress_current"].num_value == 3
    assert keys["progress_total"].num_value == 10


def test_metric_infers_kind():
    r = _reporter()
    ctx = TaskContext(r, 0, "t")
    ctx.metric("output_uri", "s3://bucket/out")  # string -> attribute
    ctx.metric("rows", 1200)                     # number -> metric
    r.flush()
    entries = {e.key: e for e in _reports(r)[0].entries}
    assert entries["output_uri"].kind == common_pb2.REPORT_KIND_ATTRIBUTE
    assert entries["rows"].kind == common_pb2.REPORT_KIND_METRIC


def test_no_reporter_is_noop():
    ctx = TaskContext()  # unit-test constructor: no reporter
    ctx.progress(1, 10)
    ctx.metric("loss", 0.1)
    ctx.event("e", "d")  # must not raise
