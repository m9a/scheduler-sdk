package com.scheduler.sdk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;

/**
 * Sends task status updates from the generated {@code _Harness} (in the job process)
 * to WorkerAgent's WebSocket server.
 *
 * <pre>
 * Job process (child JVM)
 *   └─ _Harness
 *        ├─ taskStarted()  ──► JobReporter ──WebSocket──► WorkerAgent
 *        ├─ @Task method executes                          (parent JVM)
 *        └─ taskCompleted() ──► JobReporter ──WebSocket──► WorkerAgent
 * </pre>
 *
 * <p>Reuses the same {@link StatusUpdate} proto format as {@link TaskContext},
 * so WorkerAgent's WebSocket handler requires no changes.
 */
public final class JobReporter {

    private static final Logger log = LoggerFactory.getLogger(JobReporter.class);

    private final WebSocket webSocket;
    private final String jobId;

    private String currentTaskName;
    private int currentTaskIndex;
    private long taskStartTimeMs;

    private JobReporter(WebSocket webSocket, String jobId) {
        this.webSocket = webSocket;
        this.jobId = jobId;
    }

    /** Opens a WebSocket connection to the given callback URL. */
    public static JobReporter connect(String callbackUrl, String jobId) {
        WebSocket ws = HttpClient.newHttpClient().newWebSocketBuilder()
                .buildAsync(URI.create(callbackUrl), new WebSocket.Listener() {
                    @Override
                    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                        log.debug("WebSocket closed by server: statusCode={}, reason={}", statusCode, reason);
                        return null;
                    }
                })
                .join();
        return new JobReporter(ws, jobId);
    }

    /** Called by generated harness to report status from user code via {@link JobContext#report}. */
    void report(TaskStatus status, String message) {
        log.info("User report for task {}: status={}, message={}", currentTaskName, status, message);
    }

    public void taskStarted(int taskIndex, String taskName) {
        currentTaskIndex = taskIndex;
        currentTaskName = taskName;
        taskStartTimeMs = System.currentTimeMillis();
        sendStatus(new StatusUpdate(jobId, taskIndex, taskName, TaskStatus.RUNNING, null));
    }

    public void taskCompleted(int taskIndex, String taskName) {
        long durationMs = System.currentTimeMillis() - taskStartTimeMs;
        sendStatus(new StatusUpdate(jobId, taskIndex, taskName, TaskStatus.COMPLETED, null, durationMs, null));
    }

    public void taskFailed(int taskIndex, String taskName, String error) {
        long durationMs = System.currentTimeMillis() - taskStartTimeMs;
        sendStatus(new StatusUpdate(jobId, taskIndex, taskName, TaskStatus.FAILED, error, durationMs, null));
    }

    public void close() {
        try {
            webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "done").join();
        } catch (Exception e) {
            log.warn("Failed to close WebSocket: {}", e.getMessage());
        }
    }

    private void sendStatus(StatusUpdate update) {
        try {
            webSocket.sendBinary(ByteBuffer.wrap(update.toProto()), true).join();
        } catch (Exception e) {
            log.error("Failed to report status for task {}: {}", update.taskName(), e.getMessage());
        }
    }
}
