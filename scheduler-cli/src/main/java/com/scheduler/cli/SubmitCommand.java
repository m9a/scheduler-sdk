package com.scheduler.cli;

import com.google.protobuf.ByteString;
import com.scheduler.client.SchedulerClient;
import com.scheduler.client.SchedulerException;
import com.scheduler.proto.v1.InputFile;
import com.scheduler.proto.v1.Job;
import com.scheduler.proto.v1.SubmitJobRequest;
import com.scheduler.proto.v1.SubmitJobResponse;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;

@Command(name = "submit", description = "Submit a job to the scheduler", mixinStandardHelpOptions = true)
class SubmitCommand implements Callable<Integer> {

    private static final String DEFAULT_REGISTRY = "localhost:5050";

    @ParentCommand
    SchedulerCli parent;

    @Option(names = "--name", required = true,
            description = "Job name, e.g. my-etl-job")
    String name;

    @Option(names = "--image", required = true,
            description = "Docker image to run. Use image:tag for the local registry "
                    + "(e.g. sample-job:latest → localhost:5050/sample-job:latest) "
                    + "or a full URI for a remote registry (e.g. registry.example.com/my-job:v1)")
    String image;

    @Option(names = "--param",
            description = "Key=value parameter passed to the job container (repeatable), "
                    + "e.g. --param epochs=5 --param batch_size=128")
    Map<String, String> params = new LinkedHashMap<>();

    @Option(names = "--input",
            description = "Input file as name=localPath — the file is uploaded and mounted at "
                    + "/workspace/input/<name> inside the container (repeatable), "
                    + "e.g. --input data.csv=./training_data.csv")
    Map<String, String> inputFiles = new LinkedHashMap<>();

    @Option(names = "--priority", defaultValue = "0",
            description = "Scheduling priority — higher values are picked first (default: ${DEFAULT-VALUE})")
    int priority;

    @Option(names = "--wait",
            description = "Block until the job reaches a terminal state (COMPLETED, FAILED, or KILLED) "
                    + "instead of returning immediately after submission")
    boolean wait;

    @Option(names = "--timeout", defaultValue = "300",
            description = "Maximum seconds to wait when --wait is set — the CLI gives up after this "
                    + "duration even if the job is still running, e.g. 600 for 10 minutes (default: ${DEFAULT-VALUE}s)")
    int timeout;

    @Override
    public Integer call() {
        String resolvedImage = resolveImage(image);

        try (SchedulerClient client = parent.connect()) {
            Job job;

            if (inputFiles.isEmpty() && priority == 0) {
                job = client.submitJob(name, resolvedImage, params);
            } else {
                SubmitJobRequest.Builder request = SubmitJobRequest.newBuilder()
                        .setName(name)
                        .setArtifactUri(resolvedImage)
                        .putAllParams(params)
                        .setPriority(priority);

                for (Map.Entry<String, String> entry : inputFiles.entrySet()) {
                    Path path = Path.of(entry.getValue());
                    byte[] content = Files.readAllBytes(path);
                    request.addInputFiles(InputFile.newBuilder()
                            .setName(entry.getKey())
                            .setContent(ByteString.copyFrom(content))
                            .build());
                }

                SubmitJobResponse response = client.submitJob(request.build());
                job = response.getJob();
            }

            System.out.println("Job submitted: " + job.getId());
            System.out.println("Status: " + job.getStatus().name().replace("JOB_STATUS_", ""));

            if (wait) {
                System.out.println("Waiting for completion...");
                long startNanos = System.nanoTime();
                Job completed = client.waitForCompletion(job.getId(), Duration.ofSeconds(timeout));
                long elapsedSeconds = (System.nanoTime() - startNanos) / 1_000_000_000;

                System.out.println();
                System.out.println(Formatter.formatJob(completed));
                System.out.printf("%nElapsed: %ds%n", elapsedSeconds);
            }

            return 0;
        } catch (IOException e) {
            System.err.println("Failed to read input file: " + e.getMessage());
            return 1;
        } catch (SchedulerException e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    private static String resolveImage(String image) {
        if (!image.contains("/")) {
            return DEFAULT_REGISTRY + "/" + image;
        }
        return image;
    }
}
