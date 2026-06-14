package com.scheduler.client;

import com.scheduler.proto.v1.Job;
import com.scheduler.proto.client.FileInfo;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Demonstrates all {@link SchedulerClient} APIs end-to-end: submit a job,
 * poll for completion, list output files, and download a file.
 *
 * <p>Configure via system properties:
 * <pre>
 *   -Dscheduler.host=localhost
 *   -Dscheduler.port=9090
 *   -Dscheduler.image=registry.example.com/my-job:latest
 * </pre>
 */
public class SampleClient {

    public static void main(String[] args) {
        String host = System.getProperty("scheduler.host", "localhost");
        int port = Integer.parseInt(System.getProperty("scheduler.port", "9090"));
        String image = System.getProperty("scheduler.image", "sample-job:latest");

        try (SchedulerClient client = SchedulerClient.builder()
                .host(host)
                .port(port)
                .deadline(Duration.ofSeconds(30))
                .build()) {

            // 1. Submit a job
            System.out.println("Submitting job...");
            Job job = client.submitJob("sample-run", image, Map.of(
                    "epochs", "5",
                    "batch_size", "64"));
            System.out.println("Job submitted: id=" + job.getId() + ", status=" + job.getState());

            // 2. Wait for completion
            System.out.println("Waiting for completion...");
            Job completed = client.waitForCompletion(job.getId(), Duration.ofMinutes(10));
            System.out.println("Job finished: status=" + completed.getState());

            if (!completed.getErrorMessage().isEmpty()) {
                System.out.println("Error: " + completed.getErrorMessage());
            }

            // 3. List output files
            List<FileInfo> files = client.listJobFiles(job.getId());
            System.out.println("Output files (" + files.size() + "):");
            for (FileInfo file : files) {
                System.out.println("  " + file.getName() + " (" + file.getSizeBytes() + " bytes)");
            }

            // 4. Download the first file (if any)
            if (!files.isEmpty()) {
                String fileName = files.get(0).getName();
                Path dest = Path.of("/tmp", Path.of(fileName).getFileName().toString());
                System.out.println("Downloading " + fileName + " to " + dest);
                client.downloadFile(job.getId(), fileName, dest);
                System.out.println("Download complete.");
            }
        }
    }
}
