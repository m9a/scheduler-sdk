package com.scheduler.sdk;

import com.scheduler.proto.job.Liveness;
import com.scheduler.proto.job.StatusUpdate;
import com.scheduler.proto.v1.TaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Owns the single WebSocket connection from the job process to WorkerAgent and
 * everything sent over it: task status updates ({@code [0x01] StatusUpdate}) and
 * key-value telemetry ({@code [0x03] Report}, via {@link ReportSender}).
 *
 * <p>Called only by the generated {@code _Harness} — job authors never touch this;
 * they report through the {@link TaskContext} injected into their {@code @Task} methods.
 *
 * <pre>
 * Job process (child JVM)
 *   └─ _Harness
 *        ├─ taskStarted()                ──[0x01]──► WorkerAgent ──[0x02 ack]──►
 *        ├─ @Task method runs
 *        │    └─ ctx.progress()/metric() ──[0x03]──► WorkerAgent
 *        └─ taskCompleted(output)        ──[0x01]──► WorkerAgent ──[0x02 ack]──►
 * </pre>
 *
 * <p>Status frames drive the job state machine, so they must not be lost. A
 * half-open socket can swallow a send without erroring, so each status frame
 * waits for a one-byte ack from the worker and is resent on a fresh connection
 * if the ack doesn't arrive. Telemetry is fire-and-forget (lossy by design).
 * All sends are serialized: the HttpClient WebSocket permits one in-flight send,
 * and the ack correlation assumes one outstanding status frame at a time.
 */
public final class JobReporter {

    private static final Logger log = LoggerFactory.getLogger(JobReporter.class);
    private static final long ACK_TIMEOUT_MS = 10_000;
    private static final int MAX_SEND_ATTEMPTS = 3;
    // WebSocket frame type tags; must match JobCallbackServer.
    private static final byte TYPE_TAG_STATUS = 0x01;
    private static final byte TYPE_TAG_LIVENESS = 0x04;
    // How often to ping while a task runs (must be < the worker's stall probe interval).
    private static final long LIVENESS_INTERVAL_MS = 15_000;

    private final String callbackUrl;
    private final String jobId;
    private final ReportSender reports;
    private final Object sendLock = new Object();
    private final ScheduledExecutorService liveness;

    private volatile WebSocket webSocket;
    // Completed by the listener when the worker's ack frame arrives; non-null
    // only while a status frame is in flight (guarded by sendLock).
    private volatile CompletableFuture<Void> pendingAck;
    // Last time any frame was sent — drives the idle liveness ping.
    private volatile long lastSentAtMs;
    private long taskStartTimeMs;

    private JobReporter(String callbackUrl, String jobId) {
        this.callbackUrl = callbackUrl;
        this.jobId = jobId;
        this.reports = new ReportSender(framed -> send(framed, false), jobId);
        this.webSocket = openSocket();
        this.lastSentAtMs = System.currentTimeMillis();
        this.liveness = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "liveness-ping");
            t.setDaemon(true);
            return t;
        });
        liveness.scheduleAtFixedRate(this::sendLivenessIfIdle,
                LIVENESS_INTERVAL_MS, LIVENESS_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /** Opens the single WebSocket connection to the given callback URL. */
    public static JobReporter connect(String callbackUrl, String jobId) {
        return new JobReporter(callbackUrl, jobId);
    }

    private WebSocket openSocket() {
        return HttpClient.newHttpClient().newWebSocketBuilder()
                .buildAsync(URI.create(callbackUrl), new WebSocket.Listener() {
                    @Override
                    public void onOpen(WebSocket ws) {
                        ws.request(1);
                    }

                    @Override
                    public CompletionStage<?> onBinary(WebSocket ws, ByteBuffer data, boolean last) {
                        // The worker's only inbound frame is the one-byte ack
                        // confirming it received and forwarded a status update.
                        CompletableFuture<Void> ack = pendingAck;
                        if (ack != null) {
                            ack.complete(null);
                        }
                        ws.request(1);
                        return null;
                    }

                    @Override
                    public CompletionStage<?> onClose(WebSocket ws, int statusCode, String reason) {
                        log.debug("WebSocket closed by server: statusCode={}, reason={}", statusCode, reason);
                        return null;
                    }
                })
                .join();
    }

    /** Creates the context injected into the @Task method at the given index. */
    public TaskContext taskContext(int taskIndex, String taskName) {
        return new TaskContext(reports, taskIndex, taskName);
    }

    public void taskStarted(int taskIndex, String taskName) {
        taskStartTimeMs = System.currentTimeMillis();
        sendStatus(taskIndex, taskName, TaskState.TASK_STATE_RUNNING, 0, null, null);
    }

    public void taskCompleted(int taskIndex, String taskName, String output) {
        reports.flush();  // deliver telemetry buffered during the task before the terminal status
        long durationMs = System.currentTimeMillis() - taskStartTimeMs;
        sendStatus(taskIndex, taskName, TaskState.TASK_STATE_COMPLETED, durationMs, null, output);
    }

    public void taskFailed(int taskIndex, String taskName, String error, String output) {
        reports.flush();
        long durationMs = System.currentTimeMillis() - taskStartTimeMs;
        sendStatus(taskIndex, taskName, TaskState.TASK_STATE_FAILED, durationMs, error, output);
    }

    /** Pings the worker (container-alive signal) if nothing has been sent for an interval. */
    private void sendLivenessIfIdle() {
        if (System.currentTimeMillis() - lastSentAtMs < LIVENESS_INTERVAL_MS) {
            return;
        }
        try {
            byte[] proto = Liveness.newBuilder()
                    .setJobId(jobId)
                    .setTimestampMs(System.currentTimeMillis())
                    .build().toByteArray();
            byte[] framed = new byte[proto.length + 1];
            framed[0] = TYPE_TAG_LIVENESS;
            System.arraycopy(proto, 0, framed, 1, proto.length);
            send(framed, false);
        } catch (Exception e) {
            log.debug("Liveness ping failed: {}", e.getMessage());
        }
    }

    /** Builds the task-status proto, frames it ({@code [0x01][proto]}), and sends it acked. */
    private void sendStatus(int taskIndex, String taskName, TaskState state,
                            long durationMs, String error, String output) {
        StatusUpdate.Builder builder = StatusUpdate.newBuilder()
                .setJobId(jobId)
                .setTaskIndex(taskIndex)
                .setTaskName(taskName)
                .setTaskState(state)
                .setDurationMs(durationMs);
        if (error != null) {
            builder.setErrorMessage(error);
        }
        if (output != null) {
            builder.setOutput(output);
        }
        byte[] proto = builder.build().toByteArray();
        byte[] framed = new byte[proto.length + 1];
        framed[0] = TYPE_TAG_STATUS;
        System.arraycopy(proto, 0, framed, 1, proto.length);
        send(framed, true);
    }

    public void close() {
        liveness.shutdownNow();
        reports.flush();
        try {
            webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "done").join();
        } catch (Exception e) {
            log.warn("Failed to close WebSocket: {}", e.getMessage());
        }
    }

    /**
     * Puts one framed message on the single WebSocket. Status frames
     * ({@code awaitAck}) block on the worker's ack and retry on a fresh
     * connection if it doesn't arrive; telemetry is dropped on failure.
     */
    private void send(byte[] framed, boolean awaitAck) {
        synchronized (sendLock) {
            lastSentAtMs = System.currentTimeMillis();  // any send counts as activity
            Exception lastError = null;
            for (int attempt = 0; attempt < MAX_SEND_ATTEMPTS; attempt++) {
                try {
                    pendingAck = awaitAck ? new CompletableFuture<>() : null;
                    webSocket.sendBinary(ByteBuffer.wrap(framed), true).join();
                    if (pendingAck != null) {
                        pendingAck.get(ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    }
                    return;
                } catch (Exception e) {
                    lastError = e;
                    if (!awaitAck) {
                        return;  // telemetry is lossy — don't reconnect for it
                    }
                    reconnect();
                } finally {
                    pendingAck = null;
                }
            }
            log.error("Failed to deliver status frame for job {} after {} attempts: {}",
                    jobId, MAX_SEND_ATTEMPTS, lastError != null ? lastError.getMessage() : "unknown");
        }
    }

    private void reconnect() {
        try {
            webSocket.abort();
        } catch (Exception ignored) {
            // already broken — just open a new one
        }
        webSocket = openSocket();
    }
}
