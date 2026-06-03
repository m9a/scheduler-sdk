package com.scheduler.cli;

import com.scheduler.client.SchedulerClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.time.Duration;

@Command(name = "scheduler",
        description = "Command-line tool for the distributed job scheduler",
        mixinStandardHelpOptions = true,
        subcommands = {
                RunCommand.class,
                StopCommand.class,
                SubmitCommand.class,
                StatusCommand.class,
                WaitCommand.class,
                FilesCommand.class,
                DownloadCommand.class
        })
public class SchedulerCli implements Runnable {

    @Option(names = "--host", defaultValue = "localhost", description = "Coordinator hostname (default: ${DEFAULT-VALUE})")
    String host;

    @Option(names = "--port", defaultValue = "9090", description = "Coordinator port (default: ${DEFAULT-VALUE})")
    int port;

    SchedulerClient connect() {
        return SchedulerClient.builder()
                .host(host)
                .port(port)
                .deadline(Duration.ofSeconds(30))
                .build();
    }

    @Override
    public void run() {
        // No subcommand → default to interactive mode
        new CommandLine(this).execute("run");
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new SchedulerCli()).execute(args);
        System.exit(exitCode);
    }
}
