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
import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
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
 * <p>Status updates ({@code [0x01][StatusUpdate]}: a task moved to
 * RUNNING/COMPLETED/FAILED) drive the job state machine, so they must not be
 * lost. Each one goes into an in-memory FIFO queue; a sender thread delivers
 * them in order and pops one only after the worker's one-byte ack (the worker
 * acks after writing its state store, so an ack means the update is durable).
 * Delivery fails two ways: the socket write throws (connection broken) or no
 * ack arrives within {@code ACK_TIMEOUT_MS} (worker slow, or the connection is
 * half-open). Either way the sender opens a fresh connection with capped
 * backoff + jitter and resends from the queue head — forever. Task threads
 * never block on delivery: tasks keep running while the worker is down and the
 * queue holds every un-acked update. {@link #close()} drains the queue before
 * exiting so terminal updates survive a worker restart. The worker de-dupes,
 * so a resend after a lost ack is safe.
 *
 * <p>Telemetry is fire-and-forget (lossy by design). All sends are serialized
 * on {@code sendLock}: the HttpClient WebSocket permits one in-flight send, and
 * the ack correlation assumes one outstanding status frame at a time.
 */
public final class JobReporter {

    private static final Logger log = LoggerFactory.getLogger(JobReporter.class);
    private static final long ACK_TIMEOUT_MS = 10_000;
    // Reconnect backoff after a failed status send: doubles per failure, jittered.
    private static final long BACKOFF_INITIAL_MS = 500;
    private static final long BACKOFF_CAP_MS = 30_000;
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
    // Un-acked status frames, oldest first. The sender thread pops one only
    // after the worker acks it; guarded by its own monitor.
    private final ArrayDeque<byte[]> statusQueue = new ArrayDeque<>();
    private final Thread sender;
    // Set by close() once the queue is drained; tells the sender to exit.
    private boolean closed;

    private volatile WebSocket webSocket;
    // Completed by the listener when the worker's ack frame arrives; non-null
    // only while a status frame is in flight (guarded by sendLock).
    private volatile CompletableFuture<Void> pendingAck;
    // Last time any frame was sent — drives the idle liveness ping.
    private volatile long lastSentAtMs;
    // The most recently started task — the shutdown hook reports against it.
    private volatile int lastTaskIndex = 0;
    private volatile String lastTaskName = "";
    private long taskStartTimeMs;

    private JobReporter(String callbackUrl, String jobId) {
        this.callbackUrl = callbackUrl;
        this.jobId = jobId;
        this.reports = new ReportSender(this::sendTelemetry, jobId);
        this.webSocket = openSocket();
        this.lastSentAtMs = System.currentTimeMillis();
        this.sender = new Thread(this::senderLoop, "status-sender");
        this.sender.setDaemon(true);
        this.sender.start();
        this.liveness = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "liveness-ping");
            t.setDaemon(true);
            return t;
        });
        liveness.scheduleAtFixedRate(this::sendLivenessIfIdle,
                LIVENESS_INTERVAL_MS, LIVENESS_INTERVAL_MS, TimeUnit.MILLISECONDS);
        sendLiveness();  // initial ping so the worker sees proof-of-life promptly
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
        lastTaskIndex = taskIndex;
        lastTaskName = taskName;
        sendStatus(taskIndex, taskName, TaskState.TASK_STATE_RUNNING, 0, null, null);
    }

    /** Index/name of the most recently started task — used by the generated shutdown hook. */
    public int lastTaskIndex() {
        return lastTaskIndex;
    }

    public String lastTaskName() {
        return lastTaskName;
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
        if (System.currentTimeMillis() - lastSentAtMs >= LIVENESS_INTERVAL_MS) {
            sendLiveness();
        }
    }

    private void sendLiveness() {
        try {
            byte[] proto = Liveness.newBuilder()
                    .setJobId(jobId)
                    .setTimestampMs(System.currentTimeMillis())
                    .build().toByteArray();
            sendTelemetry(frame(TYPE_TAG_LIVENESS, proto));
        } catch (Exception e) {
            log.debug("Liveness ping failed: {}", e.getMessage());
        }
    }

    /** Builds the task-status proto, frames it ({@code [0x01][proto]}), and queues it for delivery. */
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
        byte[] framed = frame(TYPE_TAG_STATUS, builder.build().toByteArray());
        synchronized (statusQueue) {
            statusQueue.addLast(framed);
            statusQueue.notifyAll();
        }
    }

    /** Prefixes the proto bytes with the one-byte frame type tag. */
    private static byte[] frame(byte typeTag, byte[] proto) {
        byte[] framed = new byte[proto.length + 1];
        framed[0] = typeTag;
        System.arraycopy(proto, 0, framed, 1, proto.length);
        return framed;
    }

    public void close() {
        reports.flush();
        awaitQueueDrained();
        synchronized (statusQueue) {
            closed = true;
            statusQueue.notifyAll();  // wakes the sender so it can exit
        }
        liveness.shutdownNow();
        synchronized (sendLock) {
            if (webSocket == null) {
                return;  // connection already dropped — nothing to close
            }
            try {
                webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "done").join();
            } catch (Exception e) {
                log.warn("Failed to close WebSocket: {}", e.getMessage());
            }
        }
    }

    /**
     * Blocks until every queued status update is acked — waits as long as it
     * takes for the worker to come back. Exiting with the queue non-empty would
     * lose a state transition (a restarted worker fails the job as
     * NOT_FOUND_ON_RECOVERY if the container is gone before delivery).
     */
    private void awaitQueueDrained() {
        synchronized (statusQueue) {
            while (!statusQueue.isEmpty()) {
                try {
                    statusQueue.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("Interrupted draining {} un-acked status update(s) for job {}",
                            statusQueue.size(), jobId);
                    return;
                }
            }
        }
    }

    /**
     * Delivers queued status frames in order, each confirmed by the worker's ack.
     *
     * <p>Status updates drive the job state machine, so losing one is fatal to
     * the job's outcome (a task stuck RUNNING fails the whole job). A half-open
     * socket can swallow a send without erroring — the ack is the only proof of
     * delivery. So the head frame is popped only after its ack; a failure
     * reconnects with capped backoff + jitter and resends the head, forever.
     * The worker de-dupes, so resending after a lost ack is safe.
     */
    private void senderLoop() {
        long backoffMs = BACKOFF_INITIAL_MS;
        byte[] head;
        while ((head = awaitQueueHead()) != null) {
            if (trySendStatus(head)) {
                popQueueHead();
                backoffMs = BACKOFF_INITIAL_MS;
            } else {
                dropConnection();  // delivery unconfirmed — retry on a fresh socket
                if (!sleepJittered(backoffMs)) {
                    return;
                }
                backoffMs = Math.min(backoffMs * 2, BACKOFF_CAP_MS);
            }
        }
    }

    /** Blocks until a status frame is queued; null once closed and drained. */
    private byte[] awaitQueueHead() {
        synchronized (statusQueue) {
            while (statusQueue.isEmpty() && !closed) {
                try {
                    statusQueue.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
            return statusQueue.peekFirst();  // null when closed and drained
        }
    }

    /** Drops the acked head frame and wakes close() waiting for the drain. */
    private void popQueueHead() {
        synchronized (statusQueue) {
            statusQueue.pollFirst();
            statusQueue.notifyAll();
        }
    }

    /** Sleeps the backoff ± 25% jitter; false if interrupted (sender must exit). */
    private boolean sleepJittered(long backoffMs) {
        try {
            Thread.sleep((long) (backoffMs * ThreadLocalRandom.current().nextDouble(0.75, 1.25)));
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /** One send + ack-wait attempt on the current connection. */
    private boolean trySendStatus(byte[] framed) {
        synchronized (sendLock) {
            lastSentAtMs = System.currentTimeMillis();  // any send counts as activity
            try {
                if (webSocket == null) {
                    webSocket = openSocket();
                }
                pendingAck = new CompletableFuture<>();
                webSocket.sendBinary(ByteBuffer.wrap(framed), true).join();
                pendingAck.get(ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                return true;
            } catch (Exception e) {
                log.warn("Status send for job {} failed, will reconnect: {}", jobId, e.getMessage());
                return false;
            } finally {
                pendingAck = null;
            }
        }
    }

    /** Sends one telemetry/liveness frame — dropped on failure (lossy by design). */
    private void sendTelemetry(byte[] framed) {
        synchronized (sendLock) {
            lastSentAtMs = System.currentTimeMillis();  // any send counts as activity
            try {
                if (webSocket == null) {
                    webSocket = openSocket();
                }
                webSocket.sendBinary(ByteBuffer.wrap(framed), true).join();
            } catch (Exception e) {
                log.debug("Telemetry send failed, dropping frame: {}", e.getMessage());
                dropConnection();
            }
        }
    }

    private void dropConnection() {
        synchronized (sendLock) {
            if (webSocket != null) {
                try {
                    webSocket.abort();
                } catch (Exception ignored) {
                    // already broken — the next send opens a new one
                }
                webSocket = null;
            }
        }
    }
}
