package com.scheduler.cli;

import com.scheduler.client.SchedulerClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;

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

    private CliConfig config;

    // Config path comes only from CONTROL_PLANE_CONFIG — no default path, no flag.
    CliConfig config() {
        if (config == null) {
            String path = System.getenv("CONTROL_PLANE_CONFIG");
            if (path == null || path.isBlank()) {
                throw new IllegalStateException("CONTROL_PLANE_CONFIG must point to control_plane_config.yaml");
            }
            try {
                config = CliConfig.load(Path.of(path));
            } catch (IOException e) {
                throw new IllegalStateException(
                        "Failed to load CONTROL_PLANE_CONFIG=" + path + ": " + e.getMessage(), e);
            }
        }
        return config;
    }

    SchedulerClient connect() {
        CliConfig cfg = config();
        return SchedulerClient.builder()
                .host(cfg.getCoordinator().getHost())
                .port(cfg.getCoordinator().getPort())
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
