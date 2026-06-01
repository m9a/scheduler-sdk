package com.scheduler.sdk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Runs inside the job process (the child JVM spawned by
 * {@code com.scheduler.worker.WorkerAgent} on the worker).
 *
 * <p><b>Who calls this:</b> The job author, from their JAR's {@code main()} method.
 * The task list is defined by the job author in code — it is not passed in by the
 * coordinator or worker. The coordinator only knows task <em>names</em> (metadata
 * submitted by the client); the actual {@link Task} implementations live in the
 * job JAR.
 *
 * <pre>
 * // Inside a job JAR — this is what the job author writes:
 * public class MyEtlJob {
 *     public static void main(String[] args) {
 *         JobProcess.run(List.of(
 *             new ExtractTask(),    // implements Task
 *             new TransformTask(),
 *             new LoadTask()
 *         ));
 *     }
 * }
 * </pre>
 *
 * <p><b>How it gets invoked:</b> WorkerAgent (in the worker JVM) spawns this
 * JAR as a child process: {@code java -cp <artifactUri> <mainClass>}. Two environment
 * variables are set by WorkerAgent:
 * <ul>
 *   <li>{@code EXECUTION_PAYLOAD} — base64(JSON) with workerAgentUrl, jobId, params</li>
 *   <li>workerAgentUrl is a WebSocket URL ({@code ws://host:port})</li>
 * </ul>
 *
 * <p><b>What it does:</b> Opens a single WebSocket connection to WorkerAgent,
 * runs tasks sequentially, creates a {@link TaskContext} per task that handles
 * lifecycle status (RUNNING, COMPLETED, FAILED), stdout/stderr capture, and
 * duration tracking — all sent over the shared WebSocket connection.
 *
 * <pre>
 *  Job process (child JVM)                     Worker JVM
 *  ───────────────────────                     ──────────
 *  main() {
 *    JobProcess.run(tasks)
 *      ├─ open WebSocket ──────────────────► WorkerAgent
 *      ├─ ctx.started()
 *      │    └─ send RUNNING ──WebSocket──►
 *      ├─ task.execute(ctx)
 *      │    ├─ ctx.progress(0.5, "halfway")
 *      │    └─ ctx.metric("rows", 1000)
 *      └─ ctx.completed()
 *           └─ send COMPLETED + duration + output ──WebSocket──►
 *      └─ close WebSocket
 *  }
 * </pre>
 */
public final class JobProcess {

    private static final Logger log = LoggerFactory.getLogger(JobProcess.class);

    private JobProcess() {
    }

    /**
     * Production entry point. Reads the EXECUTION_PAYLOAD env var (base64 JSON
     * with workerAgentUrl, jobId, params) set by WorkerAgent and delegates to
     * {@link #run(List, String, String)}.
     */
    public static void run(List<Task> tasks) {
        String payloadB64 = System.getenv(ExecutionPayload.ENV_EXECUTION_PAYLOAD);
        if (payloadB64 == null || payloadB64.isEmpty()) {
            throw new IllegalStateException(
                    "Environment variable '" + ExecutionPayload.ENV_EXECUTION_PAYLOAD + "' is required");
        }

        ExecutionPayload payload = ExecutionPayload.decodeBase64Json(payloadB64);
        run(tasks, payload.jobId(), payload.workerAgentUrl());
    }

    /**
     * Runs tasks sequentially. Opens one WebSocket connection for the entire job,
     * creates a {@link TaskContext} per task that manages lifecycle status, stdout
     * capture, and duration — all sent over the shared connection.
     */
    public static void run(List<Task> tasks, String jobId, String callbackUrl) {
        WebSocket webSocket = HttpClient.newHttpClient().newWebSocketBuilder()
                .buildAsync(URI.create(callbackUrl), new WebSocket.Listener() {
                    @Override
                    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                        log.debug("WebSocket closed by server: statusCode={}, reason={}", statusCode, reason);
                        return null;
                    }
                })
                .join();

        try {
            for (int i = 0; i < tasks.size(); i++) {
                Task task = tasks.get(i);
                TaskContext ctx = new TaskContext(webSocket, jobId, i, task.name());

                log.info("Starting task {} ({}/{})", task.name(), i + 1, tasks.size());
                ctx.started();

                try {
                    task.execute(ctx);
                    log.info("Task {} completed", task.name());
                    ctx.completed();
                } catch (Exception e) {
                    log.error("Task {} failed: {}", task.name(), e.getMessage(), e);
                    ctx.failed(e.getMessage());
                    return;
                }
            }

            log.info("All {} tasks completed", tasks.size());
        } finally {
            try {
                webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "done").join();
            } catch (Exception e) {
                log.warn("Failed to close WebSocket: {}", e.getMessage());
            }
        }
    }
}
