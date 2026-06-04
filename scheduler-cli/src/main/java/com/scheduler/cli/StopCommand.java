package com.scheduler.cli;

import picocli.CommandLine.Command;

import java.util.concurrent.Callable;

@Command(name = "stop",
        description = "Stop the docker-compose control plane — use after a crash to clean up orphaned containers",
        mixinStandardHelpOptions = true)
class StopCommand implements Callable<Integer> {

    @Override
    public Integer call() {
        InfraManager infra = new InfraManager();
        infra.stopDockerCompose();
        return 0;
    }
}
