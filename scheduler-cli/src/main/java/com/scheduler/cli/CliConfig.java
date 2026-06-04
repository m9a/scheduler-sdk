package com.scheduler.cli;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * YAML-backed configuration for the CLI tool.
 * Loaded from {@code ~/.scheduler/config.yaml} if present, or from a path
 * passed via {@code --config}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class CliConfig {

    private static final Path DEFAULT_PATH = Path.of(System.getProperty("user.home"),
            ".scheduler", "config.yaml");

    private Coordinator coordinator = new Coordinator();

    static CliConfig load(Path path) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(path.toFile(), CliConfig.class);
    }

    static CliConfig loadDefault() {
        if (Files.exists(DEFAULT_PATH)) {
            try {
                return load(DEFAULT_PATH);
            } catch (IOException e) {
                return new CliConfig();
            }
        }
        return new CliConfig();
    }

    static Path defaultPath() {
        return DEFAULT_PATH;
    }

    public Coordinator getCoordinator() { return coordinator; }
    public void setCoordinator(Coordinator coordinator) { this.coordinator = coordinator; }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Coordinator {
        private String host = "localhost";
        private int port = 9090;

        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
    }
}
