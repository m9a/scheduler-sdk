package com.scheduler.cli;

import com.scheduler.client.SchedulerClient;
import com.scheduler.client.SchedulerException;
import com.scheduler.proto.v1.Job;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

import java.time.Duration;
import java.util.concurrent.Callable;

@Command(name = "wait", description = "Poll until a job reaches a terminal state, then print its final status", mixinStandardHelpOptions = true)
class WaitCommand implements Callable<Integer> {

    @ParentCommand
    SchedulerCli parent;

    @Parameters(index = "0", description = "Job ID")
    String jobId;

    @Option(names = "--timeout", defaultValue = "300",
            description = "Maximum seconds to wait — exits with code 2 if the job is still "
                    + "running after this duration, e.g. 600 for 10 minutes (default: ${DEFAULT-VALUE}s)")
    int timeout;

    @Override
    public Integer call() {
        try (SchedulerClient client = parent.connect()) {
            Job job = client.waitForCompletion(jobId, Duration.ofSeconds(timeout));
            System.out.println(Formatter.formatJob(job));
            return 0;
        } catch (SchedulerException e) {
            System.err.println("Error: " + e.getMessage());
            return isTimeout(e) ? 2 : 1;
        }
    }

    private static boolean isTimeout(SchedulerException e) {
        return e.getMessage() != null && e.getMessage().startsWith("Timed out");
    }
}
