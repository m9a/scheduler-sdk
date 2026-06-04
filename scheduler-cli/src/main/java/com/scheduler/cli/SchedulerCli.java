package com.scheduler.cli;

import com.scheduler.client.SchedulerClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.nio.file.Path;
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

    @Option(names = "--config",
            description = "Path to YAML config file (default: ~/.scheduler/config.yaml)")
    Path configPath;

    @Option(names = "--host", description = "Coordinator hostname (overrides config file)")
    String host;

    @Option(names = "--port", description = "Coordinator port (overrides config file)")
    Integer port;

    private CliConfig config;

    CliConfig config() {
        if (config == null) {
            if (configPath != null) {
                try {
                    config = CliConfig.load(configPath);
                } catch (IOException e) {
                    System.err.println("Failed to load config from " + configPath + ": " + e.getMessage());
                    config = new CliConfig();
                }
            } else {
                config = CliConfig.loadDefault();
            }
        }
        return config;
    }

    SchedulerClient connect() {
        CliConfig cfg = config();
        String resolvedHost = host != null ? host : cfg.getCoordinator().getHost();
        int resolvedPort = port != null ? port : cfg.getCoordinator().getPort();

        return SchedulerClient.builder()
                .host(resolvedHost)
                .port(resolvedPort)
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
