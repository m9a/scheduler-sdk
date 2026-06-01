package com.scheduler.sdk;

import java.util.Objects;

/**
 * Status update sent from JobProcess (in the job process) to WorkerAgent (in the worker JVM)
 * via WebSocket. Serialized as a prefix-byte-framed binary proto: one byte type tag
 * ({@code 0x01}) followed by {@link com.scheduler.proto.job.StatusUpdate} bytes.
 * Only carries task-level fields — the worker is responsible for deriving and reporting
 * job-level status.
 */
public record StatusUpdate(String jobId, int taskIndex, String taskName,
                               TaskStatus status, String errorMessage,
                               long durationMs, String output) {

    /** WebSocket frame type tag for task status updates. */
    static final byte TYPE_TAG_STATUS = 0x01;

    public StatusUpdate {
        Objects.requireNonNull(jobId);
        Objects.requireNonNull(taskName);
        Objects.requireNonNull(status);
    }

    /** Convenience constructor for updates without duration/output (e.g. RUNNING). */
    public StatusUpdate(String jobId, int taskIndex, String taskName, TaskStatus status, String errorMessage) {
        this(jobId, taskIndex, taskName, status, errorMessage, 0, null);
    }

    /**
     * Serializes as a prefix-byte-framed binary proto: {@code [0x01][StatusUpdate bytes]}.
     * The worker reads the first byte to determine the message type.
     */
    public byte[] toProto() {
        com.scheduler.proto.v1.TaskStatus protoStatus = switch (status) {
            case RUNNING -> com.scheduler.proto.v1.TaskStatus.TASK_STATUS_RUNNING;
            case COMPLETED -> com.scheduler.proto.v1.TaskStatus.TASK_STATUS_COMPLETED;
            case FAILED -> com.scheduler.proto.v1.TaskStatus.TASK_STATUS_FAILED;
        };

        com.scheduler.proto.job.StatusUpdate.Builder builder =
                com.scheduler.proto.job.StatusUpdate.newBuilder()
                        .setJobId(jobId)
                        .setTaskIndex(taskIndex)
                        .setTaskName(taskName)
                        .setTaskStatus(protoStatus)
                        .setDurationMs(durationMs);
        if (errorMessage != null) {
            builder.setErrorMessage(errorMessage);
        }
        if (output != null) {
            builder.setOutput(output);
        }

        byte[] proto = builder.build().toByteArray();
        byte[] framed = new byte[proto.length + 1];
        framed[0] = TYPE_TAG_STATUS;
        System.arraycopy(proto, 0, framed, 1, proto.length);
        return framed;
    }
}
