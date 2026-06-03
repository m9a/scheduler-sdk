package com.scheduler.cli;

import com.scheduler.client.SchedulerClient;
import com.scheduler.client.SchedulerException;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

import java.nio.file.Path;
import java.util.concurrent.Callable;

@Command(name = "download", description = "Download a job output file", mixinStandardHelpOptions = true)
class DownloadCommand implements Callable<Integer> {

    @ParentCommand
    SchedulerCli parent;

    @Parameters(index = "0", description = "Job ID")
    String jobId;

    @Parameters(index = "1", description = "File path in job output, e.g. model.pt or subdir/results.json")
    String filePath;

    @Option(names = "--output", description = "Local destination path — defaults to ./<filename> in the current directory")
    Path output;

    @Override
    public Integer call() {
        Path destination = output;
        if (destination == null) {
            String fileName = filePath.contains("/")
                    ? filePath.substring(filePath.lastIndexOf('/') + 1)
                    : filePath;
            destination = Path.of(fileName);
        }

        try (SchedulerClient client = parent.connect()) {
            client.downloadFile(jobId, filePath, destination);
            long size = destination.toFile().length();
            System.out.printf("Downloaded %s (%s)%n", destination, Formatter.formatBytes(size));
            return 0;
        } catch (SchedulerException e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }
}
