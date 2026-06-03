package com.scheduler.cli;

import com.scheduler.client.SchedulerClient;
import com.scheduler.client.SchedulerException;
import com.scheduler.proto.v1.Job;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

import java.util.concurrent.Callable;

@Command(name = "status", description = "Get job status", mixinStandardHelpOptions = true)
class StatusCommand implements Callable<Integer> {

    @ParentCommand
    SchedulerCli parent;

    @Parameters(index = "0", description = "Job ID")
    String jobId;

    @Override
    public Integer call() {
        try (SchedulerClient client = parent.connect()) {
            Job job = client.getJobStatus(jobId);
            System.out.println(Formatter.formatJob(job));
            return 0;
        } catch (SchedulerException e) {
            System.err.println("Error: " + e.getMessage());
            return 1;
        }
    }
}
