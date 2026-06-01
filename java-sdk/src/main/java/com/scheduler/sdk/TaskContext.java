package com.scheduler.sdk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;

/**
 * Provided by JobProcess to each task during execution. Manages the full
 * task lifecycle: sends status updates to WorkerAgent via WebSocket, captures
 * per-task stdout/stderr, and tracks duration.
 *
 * <p><b>For task authors:</b> Use {@link #progress} and {@link #metric}
 * to emit structured data. For unit testing tasks, use the no-arg constructor.
 *
 * <p><b>For JobProcess (package-private):</b> Call {@link #started()},
 * then {@code task.execute(ctx)}, then {@link #completed()} or {@link #failed(String)}.
 *
 * <pre>
 * public void execute(TaskContext ctx) {
 *     ctx.progress(0.3, "Loading data");
 *     // ... work ...
 *     ctx.metric("rows_processed", 1000);
 *     ctx.progress(1.0, "Done");
 * }
 * </pre>
 */
public final class TaskContext {

    private static final Logger log = LoggerFactory.getLogger(TaskContext.class);

    private final WebSocket webSocket;
    private final String jobId;
    private final int taskIndex;
    private final String taskName;

    private PrintStream originalOut;
    private PrintStream originalErr;
    private ByteArrayOutputStream captureBuffer;
    private long startTimeMs;

    /**
     * Production constructor — called by JobProcess to create a context for each task.
     * All tasks in a job share a single WebSocket connection.
     */
    TaskContext(WebSocket webSocket, String jobId, int taskIndex, String taskName) {
        this.webSocket = webSocket;
        this.jobId = jobId;
        this.taskIndex = taskIndex;
        this.taskName = taskName;
    }

    /**
     * Test constructor — progress and metric just log, no WebSocket.
     * For task authors writing unit tests against their Task implementations.
     */
    public TaskContext() {
        this.webSocket = null;
        this.jobId = null;
        this.taskIndex = 0;
        this.taskName = null;
    }

    /** Task authors call this to report progress. */
    public void progress(double fraction, String message) {
        log.info("Task {} progress: {}% - {}", taskName, Math.round(fraction * 100), message);
    }

    /** Task authors call this to report a metric. */
    public void metric(String name, double value) {
        log.info("Task {} metric: {}={}", taskName, name, value);
    }

    /**
     * Sends RUNNING status and starts capturing stdout/stderr for this task.
     * Called by JobProcess before {@code task.execute(ctx)}.
     */
    void started() {
        startTimeMs = System.currentTimeMillis();
        captureBuffer = new ByteArrayOutputStream();

        originalOut = System.out;
        originalErr = System.err;
        PrintStream teeOut = new PrintStream(new TeeOutputStream(originalOut, captureBuffer), true);
        PrintStream teeErr = new PrintStream(new TeeOutputStream(originalErr, captureBuffer), true);
        System.setOut(teeOut);
        System.setErr(teeErr);

        sendStatus(new StatusUpdate(jobId, taskIndex, taskName, TaskStatus.RUNNING, null));
    }

    /**
     * Restores stdout/stderr, sends COMPLETED status with duration and captured output.
     * Called by JobProcess after successful {@code task.execute(ctx)}.
     */
    void completed() {
        restoreStreams();
        long durationMs = System.currentTimeMillis() - startTimeMs;
        sendStatus(new StatusUpdate(jobId, taskIndex, taskName, TaskStatus.COMPLETED, null,
                durationMs, capturedOutput()));
    }

    /**
     * Restores stdout/stderr, sends FAILED status with duration, output, and error.
     * Called by JobProcess when {@code task.execute(ctx)} throws.
     */
    void failed(String error) {
        restoreStreams();
        long durationMs = System.currentTimeMillis() - startTimeMs;
        sendStatus(new StatusUpdate(jobId, taskIndex, taskName, TaskStatus.FAILED, error,
                durationMs, capturedOutput()));
    }

    /** Returns captured stdout/stderr text, or null if nothing was captured. */
    String output() {
        return capturedOutput();
    }

    private String capturedOutput() {
        if (captureBuffer == null || captureBuffer.size() == 0) {
            return null;
        }
        return captureBuffer.toString();
    }

    private void restoreStreams() {
        if (originalOut != null) {
            System.setOut(originalOut);
            System.setErr(originalErr);
        }
    }

    private void sendStatus(StatusUpdate update) {
        if (webSocket == null) {
            return;
        }
        try {
            webSocket.sendBinary(ByteBuffer.wrap(update.toProto()), true).join();
        } catch (Exception e) {
            log.error("Failed to report status for task {}: {}", taskName, e.getMessage());
        }
    }

    /**
     * Writes to both the original stream and a capture buffer simultaneously.
     * This ensures task output is visible on the process stdout (for WorkerAgent
     * to read) while also being captured per-task for structured status updates.
     */
    private static final class TeeOutputStream extends OutputStream {

        private final OutputStream original;
        private final OutputStream capture;

        TeeOutputStream(OutputStream original, OutputStream capture) {
            this.original = original;
            this.capture = capture;
        }

        @Override
        public void write(int b) throws IOException {
            original.write(b);
            capture.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            original.write(b, off, len);
            capture.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            original.flush();
            capture.flush();
        }
    }
}
