package com.scheduler.sdk;

import com.scheduler.proto.v1.ReportKind;

/**
 * The single context injected into {@code @Task} methods (via the {@code @Context}
 * parameter annotation) — the entire user-facing API inside a task. Created per
 * task by the generated {@code _Harness} through {@link JobReporter#taskContext}.
 *
 * <p>Carries the task's identity so reports are attributed correctly, and is the
 * extension point for future per-task data (distributed rank, checkpoint dir, ...).
 *
 * <pre>
 * {@literal @}Task(name = "train", order = 1)
 * public void train(@Context TaskContext ctx) {
 *     for (int epoch = 0; epoch &lt; epochs; epoch++) {
 *         // ... work ...
 *         ctx.progress(epoch + 1, epochs);
 *         ctx.metric("loss", loss);
 *     }
 *     ctx.event("checkpoint_saved", "epoch " + epochs);
 * }
 * </pre>
 */
public final class TaskContext {

    private final ReportSender reports;
    private final int taskIndex;
    private final String taskName;

    TaskContext(ReportSender reports, int taskIndex, String taskName) {
        this.reports = reports;
        this.taskIndex = taskIndex;
        this.taskName = taskName;
    }

    /**
     * Test constructor — reporting calls are buffered but never sent (no WebSocket).
     * For job authors unit-testing their {@code @Task} methods.
     */
    public TaskContext() {
        this(new ReportSender(null, null), 0, null);
    }

    /** Report progress as "current of total" — drives % complete, ETA, stall detection. */
    public void progress(long current, long total) {
        reports.reportNum(taskIndex, "progress_current", ReportKind.REPORT_KIND_METRIC, current, false);
        reports.reportNum(taskIndex, "progress_total", ReportKind.REPORT_KIND_METRIC, total, false);
    }

    /** A numeric measurement, latest-wins (loss, accuracy, records_written). */
    public void metric(String key, double value) {
        reports.reportNum(taskIndex, key, ReportKind.REPORT_KIND_METRIC, value, false);
    }

    /** A textual fact about the run (model_version, output_uri). */
    public void metric(String key, String value) {
        reports.reportStr(taskIndex, key, ReportKind.REPORT_KIND_ATTRIBUTE, value, false);
    }

    /** A one-off note (checkpoint_saved, early_stop). Sent immediately. */
    public void event(String name, String detail) {
        reports.reportStr(taskIndex, name, ReportKind.REPORT_KIND_EVENT, detail, true);
    }

    public String taskName() {
        return taskName;
    }
}
