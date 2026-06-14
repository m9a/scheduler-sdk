package com.scheduler.sdk;

import com.scheduler.proto.job.Report;
import com.scheduler.proto.v1.ReportEntry;
import com.scheduler.proto.v1.ReportKind;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds and throttles key-value telemetry ({@code [0x03][Report]} frames) from
 * the job process and hands each framed batch to {@link JobReporter}, which puts
 * it on the single WebSocket. Used by {@link TaskContext} so job authors can
 * emit progress/metrics/events.
 *
 * <p>Throttled: at most one frame per {@link #REPORT_INTERVAL_MS}, keeping only the
 * latest value per key (older numbers are dropped — full history goes to MLflow).
 * EVENT entries force an immediate flush so one-off notes are never dropped.
 *
 * <p>When constructed without a sender (unit tests), sends are skipped.
 */
final class ReportSender {

    /** WebSocket frame type tag for key-value reports. Mirrors the Python SDK. */
    static final byte TYPE_TAG_REPORT = 0x03;
    private static final long REPORT_INTERVAL_MS = 1000;

    /** Puts one framed telemetry message on the wire (fire-and-forget). */
    @FunctionalInterface
    interface FrameSender {
        void send(byte[] framed);
    }

    private final FrameSender sender;
    private final String jobId;
    // (taskIndex, key) -> latest entry; flushed in batches, one Report per task.
    private final Map<BufferKey, ReportEntry> buffer = new LinkedHashMap<>();
    private long lastFlushMs = System.currentTimeMillis();

    ReportSender(FrameSender sender, String jobId) {
        this.sender = sender;
        this.jobId = jobId;
    }

    synchronized void reportNum(int taskIndex, String key, ReportKind kind, double value, boolean force) {
        buffer.put(new BufferKey(taskIndex, key),
                ReportEntry.newBuilder().setKey(key).setKind(kind).setNumValue(value).build());
        maybeFlush(force);
    }

    synchronized void reportStr(int taskIndex, String key, ReportKind kind, String value, boolean force) {
        buffer.put(new BufferKey(taskIndex, key),
                ReportEntry.newBuilder().setKey(key).setKind(kind).setStrValue(value).build());
        maybeFlush(force);
    }

    /** Sends all buffered entries now (called at task end / close). */
    synchronized void flush() {
        flushLocked();
    }

    private void maybeFlush(boolean force) {
        if (force || System.currentTimeMillis() - lastFlushMs >= REPORT_INTERVAL_MS) {
            flushLocked();
        }
    }

    private void flushLocked() {
        lastFlushMs = System.currentTimeMillis();
        if (buffer.isEmpty() || sender == null) {
            buffer.clear();
            return;
        }
        // Group buffered entries by task so each Report carries one task's entries.
        Map<Integer, List<ReportEntry>> byTask = new LinkedHashMap<>();
        for (Map.Entry<BufferKey, ReportEntry> e : buffer.entrySet()) {
            byTask.computeIfAbsent(e.getKey().taskIndex(), k -> new ArrayList<>()).add(e.getValue());
        }
        buffer.clear();

        long timestampMs = System.currentTimeMillis();
        for (Map.Entry<Integer, List<ReportEntry>> e : byTask.entrySet()) {
            send(Report.newBuilder()
                    .setJobId(jobId)
                    .setTaskIndex(e.getKey())
                    .setTimestampMs(timestampMs)
                    .addAllEntries(e.getValue())
                    .build());
        }
    }

    private void send(Report msg) {
        byte[] proto = msg.toByteArray();
        byte[] framed = new byte[proto.length + 1];
        framed[0] = TYPE_TAG_REPORT;
        System.arraycopy(proto, 0, framed, 1, proto.length);
        sender.send(framed);
    }

    private record BufferKey(int taskIndex, String key) {}
}
