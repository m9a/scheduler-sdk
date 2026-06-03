package com.scheduler.cli;

import com.scheduler.client.SchedulerClient;
import com.scheduler.client.SchedulerException;
import com.scheduler.proto.v1.Job;
import com.scheduler.proto.v1.JobStatus;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

@Command(name = "run",
        description = "Start all infrastructure, run sample jobs interactively, shut down on exit",
        mixinStandardHelpOptions = true)
class RunCommand implements Callable<Integer> {

    private static final Duration JOB_TIMEOUT = Duration.ofMinutes(10);

    private static final List<Preset> PRESETS = List.of(
            new Preset("mnist-training",
                    "localhost:5050/sample-py-training-job:latest",
                    "MNIST training with Lightning + MLflow metrics",
                    "sample-py-training-job/train.py",
                    Map.of("epochs", "2", "batch_size", "128")),
            new Preset("java-etl",
                    "localhost:5050/sample-job:latest",
                    "Java ETL pipeline (3 tasks: extract, transform, load)",
                    "sample-job/src/main/java/com/scheduler/sample/DailySalesEtlJob.java",
                    Map.of("region", "us", "batchSize", "100"))
    );

    @ParentCommand
    SchedulerCli parent;

    @Override
    public Integer call() {
        String coordinatorJar = System.getenv("SCHEDULER_COORDINATOR_JAR");
        String workerJar = System.getenv("SCHEDULER_WORKER_JAR");

        System.out.println("Checking prerequisites...");
        if (!checkJar("SCHEDULER_COORDINATOR_JAR", coordinatorJar)) return 1;
        if (!checkJar("SCHEDULER_WORKER_JAR", workerJar)) return 1;

        InfraManager infra = new InfraManager();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println();
            infra.close();
        }));

        try {
            infra.start(coordinatorJar, workerJar);

            System.out.println();
            System.out.println("Infrastructure:");
            System.out.println("  MLflow:    http://localhost:5000");
            System.out.println("  MinIO:     http://localhost:9000  (console: http://localhost:9001)");
            System.out.println("  Registry:  http://localhost:5050");
            System.out.println();
            System.out.println("Logs: " + infra.getLogFile());
            System.out.println("  tail -f " + infra.getLogFile());

            PrintStream originalErr = System.err;
            System.setErr(new PrintStream(new FileOutputStream(infra.getLogFile().toFile(), true)));

            interactiveLoop();

            System.setErr(originalErr);

            return 0;
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        } finally {
            infra.close();
        }
    }

    private void interactiveLoop() {
        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));

        while (true) {
            printMenu();

            String input;
            try {
                input = stdin.readLine();
            } catch (IOException e) {
                break;
            }
            if (input == null || input.strip().equalsIgnoreCase("q")) {
                break;
            }

            int selection;
            try {
                selection = Integer.parseInt(input.strip());
            } catch (NumberFormatException e) {
                System.out.println("Invalid input. Enter a number (1-" + PRESETS.size() + ") or 'q' to quit.");
                continue;
            }

            if (selection < 1 || selection > PRESETS.size()) {
                System.out.println("Invalid job number. Enter 1-" + PRESETS.size() + ".");
                continue;
            }

            Preset preset = PRESETS.get(selection - 1);
            runJob(preset);
        }
    }

    private void printMenu() {
        System.out.println();
        System.out.println("Available jobs:");
        System.out.println();
        for (int i = 0; i < PRESETS.size(); i++) {
            Preset p = PRESETS.get(i);
            System.out.printf("  %d  %s%n", i + 1, p.description);
            System.out.printf("     → %s%n", p.sourceFile);
        }
        System.out.println();
        System.out.println("  q  Stop and exit");
        System.out.println();
        System.out.print("Enter selection: ");
        System.out.flush();
    }

    private void runJob(Preset preset) {
        try (SchedulerClient client = SchedulerClient.builder()
                .host("localhost")
                .port(9090)
                .deadline(Duration.ofSeconds(30))
                .maxRetries(3)
                .build()) {

            System.out.println();
            System.out.println("Submitting " + preset.name + "...");
            Job job = client.submitJob(preset.name, preset.image, preset.params);
            String jobId = job.getId();
            System.out.println("Job " + jobId + " submitted");
            System.out.println();

            // Poll until the container starts so we can attach to its logs
            Job running = pollUntilRunning(client, jobId, 120);
            if (running == null) {
                System.err.println("Job did not reach RUNNING state");
                return;
            }

            // Stream container logs in background
            Process dockerLogs = startDockerLogs(jobId);

            // Wait for completion
            Job completed = client.waitForCompletion(jobId, JOB_TIMEOUT);

            // Give docker logs a moment to flush, then stop
            if (dockerLogs != null) {
                try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
                dockerLogs.destroy();
            }

            System.out.println();
            System.out.println(Formatter.formatJob(completed));

        } catch (SchedulerException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    private Job pollUntilRunning(SchedulerClient client, String jobId, int timeoutSeconds) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (System.nanoTime() < deadline) {
            Job job = client.getJobStatus(jobId);
            JobStatus status = job.getStatus();
            if (status == JobStatus.JOB_STATUS_RUNNING) {
                return job;
            }
            if (status == JobStatus.JOB_STATUS_COMPLETED || status == JobStatus.JOB_STATUS_FAILED
                    || status == JobStatus.JOB_STATUS_KILLED) {
                return job;
            }
            try { Thread.sleep(500); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
        return null;
    }

    private Process startDockerLogs(String jobId) {
        String containerName = "job-" + jobId;
        try {
            ProcessBuilder pb = new ProcessBuilder("docker", "logs", "-f", containerName)
                    .redirectErrorStream(true);
            Process process = pb.start();

            Thread reader = new Thread(() -> {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        System.out.println(line);
                    }
                } catch (IOException ignored) {
                }
            });
            reader.setDaemon(true);
            reader.setName("docker-logs-" + jobId);
            reader.start();

            return process;
        } catch (IOException e) {
            System.err.println("Warning: could not attach to container logs: " + e.getMessage());
            return null;
        }
    }

    private boolean checkJar(String envVar, String path) {
        if (path == null || path.isEmpty()) {
            System.err.println("  " + envVar + ": not set");
            System.err.println();
            System.err.println("Set the environment variable to the fat JAR path:");
            System.err.println("  export " + envVar + "=/path/to/scheduler/scheduler-*/target/*.jar");
            return false;
        }
        if (!Files.isRegularFile(Path.of(path))) {
            System.err.println("  " + envVar + ": " + path + " (not found)");
            System.err.println();
            System.err.println("Build the fat JAR first:");
            System.err.println("  cd /path/to/scheduler && mvn package -DskipTests");
            return false;
        }
        System.out.println("  " + envVar + ": " + path);
        return true;
    }

    record Preset(String name, String image, String description, String sourceFile, Map<String, String> params) {}
}
