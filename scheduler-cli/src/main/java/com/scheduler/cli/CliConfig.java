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
    private Minio minio = new Minio();
    private Mlflow mlflow = new Mlflow();
    private Registry registry = new Registry();

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

    public Minio getMinio() { return minio; }
    public void setMinio(Minio minio) { this.minio = minio; }

    public Mlflow getMlflow() { return mlflow; }
    public void setMlflow(Mlflow mlflow) { this.mlflow = mlflow; }

    public Registry getRegistry() { return registry; }
    public void setRegistry(Registry registry) { this.registry = registry; }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Coordinator {
        private String host = "localhost";
        private int port = 9090;

        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Minio {
        private String endpoint;

        public String getEndpoint() { return endpoint; }
        public void setEndpoint(String endpoint) { this.endpoint = endpoint; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Mlflow {
        private String healthUrl;

        public String getHealthUrl() { return healthUrl; }
        public void setHealthUrl(String healthUrl) { this.healthUrl = healthUrl; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Registry {
        private String url;

        public String getUrl() { return url; }
        public void setUrl(String url) { this.url = url; }
    }
}
