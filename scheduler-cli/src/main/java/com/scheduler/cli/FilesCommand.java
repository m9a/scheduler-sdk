package com.scheduler.cli;

import com.scheduler.client.SchedulerClient;
import com.scheduler.client.SchedulerException;
import com.scheduler.proto.client.FileInfo;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

import java.util.List;
import java.util.concurrent.Callable;

@Command(name = "files", description = "List job output files", mixinStandardHelpOptions = true)
class FilesCommand implements Callable<Integer> {

    @ParentCommand
    SchedulerCli parent;

    @Parameters(index = "0", description = "Job ID")
    String jobId;

    @Override
    public Integer call() {
        try (SchedulerClient client = parent.connect()) {
            List<FileInfo> files = client.listJobFiles(jobId);
            System.out.println(Formatter.formatFiles(jobId, files));
            return 0;
        } catch (SchedulerException e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }
}
