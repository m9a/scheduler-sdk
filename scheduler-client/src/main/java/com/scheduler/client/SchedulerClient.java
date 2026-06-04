package com.scheduler.client;

import com.scheduler.proto.v1.*;
import com.scheduler.proto.v1.ResourceRequirements;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Client for the scheduler's {@code ClientService} gRPC API. Wraps all 4 RPCs
 * with connection management, per-call deadlines, and retry on UNAVAILABLE.
 *
 * <pre>
 * try (SchedulerClient client = SchedulerClient.builder()
 *         .host("localhost").port(9090).build()) {
 *     Job job = client.submitJob("my-job", "image:latest", Map.of("epochs", "5"));
 *     Job completed = client.waitForCompletion(job.getId(), Duration.ofMinutes(10));
 * }
 * </pre>
 */
public class SchedulerClient implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SchedulerClient.class);
    private static final Duration POLL_INTERVAL = Duration.ofSeconds(2);

    private final ManagedChannel channel;
    private final Duration deadline;
    private final int maxRetries;

    private SchedulerClient(String host, int port, Duration deadline, int maxRetries) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.deadline = deadline;
        this.maxRetries = maxRetries;
        log.info("Connected to scheduler at {}:{}", host, port);
    }

    public static Builder builder() {
        return new Builder();
    }

    // -- RPCs --

    /**
     * Submits a job with full control over all request fields.
     */
    public SubmitJobResponse submitJob(SubmitJobRequest request) {
        return callWithRetry(stub -> stub.submitJob(request));
    }

    /**
     * Convenience method for the common case: name + Docker image + params.
     * Returns the {@link Job} from the response directly.
     */
    public Job submitJob(String name, String artifactUri, Map<String, String> params) {
        SubmitJobRequest request = SubmitJobRequest.newBuilder()
                .setName(name)
                .setArtifactUri(artifactUri)
                .putAllParams(params)
                .build();
        return submitJob(request).getJob();
    }

    /**
     * Submits a job with resource requirements for capability-based placement.
     */
    public Job submitJob(String name, String artifactUri, Map<String, String> params,
                         int memoryMb, int cpuCores, java.util.Collection<String> capabilities) {
        SubmitJobRequest.Builder request = SubmitJobRequest.newBuilder()
                .setName(name)
                .setArtifactUri(artifactUri)
                .putAllParams(params);

        if (memoryMb > 0 || cpuCores > 0 || (capabilities != null && !capabilities.isEmpty())) {
            ResourceRequirements.Builder resources = ResourceRequirements.newBuilder()
                    .setMemoryMb(memoryMb)
                    .setCpuCores(cpuCores);
            if (capabilities != null) {
                resources.addAllCapabilities(capabilities);
            }
            request.setResources(resources.build());
        }

        return submitJob(request.build()).getJob();
    }

    public Job getJobStatus(String jobId) {
        GetJobStatusResponse response = callWithRetry(stub ->
                stub.getJobStatus(GetJobStatusRequest.newBuilder()
                        .setJobId(jobId)
                        .build()));
        return response.getJob();
    }

    public List<OutputFile> listJobFiles(String jobId) {
        ListJobFilesResponse response = callWithRetry(stub ->
                stub.listJobFiles(ListJobFilesRequest.newBuilder()
                        .setJobId(jobId)
                        .build()));
        return response.getFilesList();
    }

    /**
     * Downloads a file from the job's output to a local path.
     */
    public void downloadFile(String jobId, String path, Path destination) {
        try (OutputStream out = Files.newOutputStream(destination)) {
            downloadFile(jobId, path, out);
        } catch (IOException e) {
            throw new SchedulerException("Failed to write to " + destination, e);
        }
    }

    /**
     * Downloads a file from the job's output, streaming chunks to the given
     * {@link OutputStream}. The first message from the server is a
     * {@link FileHeader}; subsequent messages are 64KB byte chunks.
     */
    public void downloadFile(String jobId, String path, OutputStream out) {
        try {
            ClientServiceGrpc.ClientServiceBlockingStub stub = newStub();
            Iterator<GetJobOutputResponse> stream = stub.getJobOutput(
                    GetJobOutputRequest.newBuilder()
                            .setJobId(jobId)
                            .setPath(path)
                            .build());

            while (stream.hasNext()) {
                GetJobOutputResponse response = stream.next();
                if (response.hasChunk()) {
                    out.write(response.getChunk().toByteArray());
                }
                // FileHeader is logged but not written — caller can use listJobFiles for metadata
                if (response.hasHeader()) {
                    log.debug("Downloading {} ({} bytes)", response.getHeader().getName(),
                            response.getHeader().getSizeBytes());
                }
            }
        } catch (StatusRuntimeException e) {
            throw new SchedulerException(e);
        } catch (IOException e) {
            throw new SchedulerException("I/O error during download", e);
        }
    }

    /**
     * Polls {@code getJobStatus} until the job reaches a terminal state
     * (COMPLETED, FAILED, CANCELLED, KILLED) or the timeout expires.
     *
     * @throws SchedulerException if the timeout expires before a terminal state
     */
    public Job waitForCompletion(String jobId, Duration timeout) {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();

        while (System.nanoTime() < deadlineNanos) {
            Job job = getJobStatus(jobId);
            JobStatus status = job.getStatus();

            if (status == JobStatus.JOB_STATUS_COMPLETED
                    || status == JobStatus.JOB_STATUS_FAILED
                    || status == JobStatus.JOB_STATUS_CANCELLED
                    || status == JobStatus.JOB_STATUS_KILLED) {
                return job;
            }

            try {
                Thread.sleep(POLL_INTERVAL.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SchedulerException("Interrupted while waiting for job " + jobId, e);
            }
        }

        throw new SchedulerException("Timed out waiting for job " + jobId
                + " after " + timeout, null);
    }

    @Override
    public void close() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            channel.shutdownNow();
        }
    }

    // -- internals --

    /**
     * Creates a fresh blocking stub with the configured deadline.
     * Fresh stub per call so the deadline resets (not cumulative across retries).
     */
    private ClientServiceGrpc.ClientServiceBlockingStub newStub() {
        return ClientServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(deadline.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Retries unary RPCs on UNAVAILABLE with exponential backoff (1s, 2s, 4s).
     * Other status codes are wrapped in {@link SchedulerException} immediately.
     */
    private <T> T callWithRetry(Function<ClientServiceGrpc.ClientServiceBlockingStub, T> rpc) {
        StatusRuntimeException lastException = null;

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                return rpc.apply(newStub());
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode() != Status.Code.UNAVAILABLE) {
                    throw new SchedulerException(e);
                }
                lastException = e;
                if (attempt < maxRetries) {
                    long backoffMs = 1000L * (1L << attempt); // 1s, 2s, 4s
                    log.warn("RPC unavailable (attempt {}/{}), retrying in {}ms",
                            attempt + 1, maxRetries + 1, backoffMs);
                    try {
                        Thread.sleep(backoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new SchedulerException(e);
                    }
                }
            }
        }

        throw new SchedulerException(lastException);
    }

    // -- builder --

    public static class Builder {
        private String host = "localhost";
        private int port = 9090;
        private Duration deadline = Duration.ofSeconds(30);
        private int maxRetries = 3;

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder deadline(Duration deadline) {
            this.deadline = deadline;
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public SchedulerClient build() {
            return new SchedulerClient(host, port, deadline, maxRetries);
        }
    }
}
