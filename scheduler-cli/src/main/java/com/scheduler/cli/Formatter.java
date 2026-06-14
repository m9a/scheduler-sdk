package com.scheduler.cli;

import com.scheduler.proto.v1.Job;
import com.scheduler.proto.v1.JobState;
import com.scheduler.proto.client.FileInfo;
import com.scheduler.proto.v1.Task;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

final class Formatter {

    private static final DateTimeFormatter TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

    private Formatter() {}

    static String formatJob(Job job) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Job:      %s%n", job.getId()));
        sb.append(String.format("Name:     %s%n", job.getName()));
        sb.append(String.format("Image:    %s%n", job.getArtifactUri()));
        sb.append(String.format("Status:   %s%n", formatStatus(job.getState())));

        if (job.getCreatedAtMillis() > 0) {
            sb.append(String.format("Created:  %s%n", formatTimestamp(job.getCreatedAtMillis())));
        }
        if (job.getStartedAtMillis() > 0) {
            sb.append(String.format("Started:  %s%n", formatTimestamp(job.getStartedAtMillis())));
        }
        if (job.getCompletedAtMillis() > 0) {
            sb.append(String.format("Finished: %s%n", formatTimestamp(job.getCompletedAtMillis())));
        }

        if (job.getState() == JobState.JOB_STATE_FAILED) {
            if (!job.getErrorMessage().isEmpty()) {
                sb.append(String.format("Error:    %s%n", job.getErrorMessage()));
            }
            if (job.getFailureReason().getNumber() > 0) {
                sb.append(String.format("Reason:   %s%n", job.getFailureReason().name()));
            }
        }

        if (job.getTasksCount() > 0) {
            sb.append(String.format("%nTasks:%n"));
            for (Task task : job.getTasksList()) {
                sb.append(String.format("  #%d  %-20s %s%n",
                        task.getSequenceNumber(), task.getName(),
                        formatTaskState(task.getState())));
            }
        }

        return sb.toString().stripTrailing();
    }

    static String formatFiles(String jobId, List<FileInfo> files) {
        if (files.isEmpty()) {
            return "No output files for job " + jobId;
        }

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Files for job %s:%n", jobId));

        int maxNameLen = files.stream().mapToInt(f -> f.getName().length()).max().orElse(10);
        for (FileInfo file : files) {
            sb.append(String.format("  %-" + maxNameLen + "s  %s%n",
                    file.getName(), formatBytes(file.getSizeBytes())));
        }

        return sb.toString().stripTrailing();
    }

    static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
        return String.format("%.1f GB", bytes / (1024.0 * 1024 * 1024));
    }

    private static String formatStatus(JobState state) {
        return state.name().replace("JOB_STATE_", "");
    }

    private static String formatTaskState(com.scheduler.proto.v1.TaskState state) {
        return state.name().replace("TASK_STATE_", "");
    }

    private static String formatTimestamp(long millis) {
        return TIMESTAMP_FORMAT.format(Instant.ofEpochMilli(millis));
    }
}
