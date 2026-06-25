package com.scheduler.cli;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Manages the lifecycle of all infrastructure: docker-compose services
 * (MinIO, PostgreSQL, MLflow, Docker registry, Prometheus, Grafana) and local
 * Java processes (coordinator, worker). Streams all process output to both the
 * terminal and a session log file.
 */
class InfraManager implements AutoCloseable {

    private static final Path SCHEDULER_DIR = Path.of(System.getProperty("user.home"), ".scheduler");

    // Monitoring UIs — ports fixed in docker-compose.control-plane.yml.
    private static final String PROMETHEUS_URL = "http://localhost:9095";
    private static final String GRAFANA_URL = "http://localhost:3000";

    // Prometheus/Grafana provisioning bundled in the JAR; extracted alongside the
    // compose file so its relative volume mounts (./metrics/...) resolve.
    private static final String[] METRICS_RESOURCES = {
            "metrics/prometheus.yml",
            "metrics/grafana/provisioning/datasources/prometheus.yml",
            "metrics/grafana/provisioning/dashboards/dashboards.yml",
            "metrics/grafana/dashboards/scheduler.json",
    };

    private Path composePath;
    private Path controlPlaneFile;
    private Path workerFile;
    // Where the bundled UI was unpacked, or null when no UI ships in the jar.
    private Path uiDir;
    private CliConfig config;
    private Path logFile;
    private PrintWriter logWriter;
    private Process coordinatorProcess;
    private Process workerProcess;

    Path getLogFile() {
        return logFile;
    }

    void start(String coordinatorJar, String workerJar) throws Exception {
        Files.createDirectories(SCHEDULER_DIR.resolve("logs"));

        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss"));
        logFile = SCHEDULER_DIR.resolve("logs/session-" + timestamp + ".log");
        logWriter = new PrintWriter(new FileWriter(logFile.toFile()), true);

        extractComposeFile();
        extractUi();
        extractConfig();
        configureUiDir();
        extractMetricsFiles();
        startDockerCompose();
        waitForServices();
        startCoordinator(coordinatorJar);
        waitForCoordinator();
        startWorker(workerJar);
        logUiUrls();
    }

    private void extractComposeFile() throws IOException {
        composePath = SCHEDULER_DIR.resolve("docker-compose.control-plane.yml");
        try (InputStream in = getClass().getResourceAsStream("/docker-compose.control-plane.yml")) {
            if (in == null) {
                throw new IOException("docker-compose.control-plane.yml not found in JAR resources");
            }
            Files.copy(in, composePath, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    // The monitoring UI ships as ui.zip in the jar and is unpacked into
    // ~/.scheduler/ui each run; the coordinator serves it (config uiDir). When no
    // ui.zip is bundled the coordinator simply runs API-only.
    private void extractUi() throws IOException {
        Path dest = SCHEDULER_DIR.resolve("ui");
        try (InputStream in = getClass().getResourceAsStream("/ui.zip")) {
            if (in == null) {
                log("No bundled UI (ui.zip); coordinator will run API-only");
                uiDir = null;
                return;
            }
            if (Files.isDirectory(dest)) {
                deleteRecursively(dest);
            }
            Files.createDirectories(dest);
            unzip(in, dest);
            uiDir = dest;
            log("Unpacked UI to " + dest);
        }
    }

    // Point the coordinator's control_plane_config.yaml at the unpacked UI. Only fills the
    // empty default (uiDir: "") so an explicit user path is preserved; done as a
    // string replace so the file's comments survive. This keeps one config source —
    // the coordinator still reads uiDir from this file; the CLI only supplies the
    // machine-specific path it can't know at build time.
    private void configureUiDir() throws IOException {
        if (uiDir == null) {
            return;
        }
        String content = Files.readString(controlPlaneFile);
        if (content.contains("uiDir: \"\"")) {
            Files.writeString(controlPlaneFile, content.replace("uiDir: \"\"", "uiDir: \"" + uiDir + "\""));
        }
    }

    private static void unzip(InputStream in, Path destDir) throws IOException {
        try (ZipInputStream zip = new ZipInputStream(in)) {
            ZipEntry entry;
            while ((entry = zip.getNextEntry()) != null) {
                Path out = destDir.resolve(entry.getName()).normalize();
                // Refuse entries that would escape destDir (zip-slip).
                if (!out.startsWith(destDir)) {
                    throw new IOException("Zip entry escapes target dir: " + entry.getName());
                }
                if (entry.isDirectory()) {
                    Files.createDirectories(out);
                } else {
                    Files.createDirectories(out.getParent());
                    Files.copy(zip, out, StandardCopyOption.REPLACE_EXISTING);
                }
                zip.closeEntry();
            }
        }
    }

    private static void deleteRecursively(Path dir) throws IOException {
        try (var paths = Files.walk(dir)) {
            paths.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
    }

    // Two configs, one per entity: control_plane_config.yaml feeds the coordinator (and the
    // CLI's readiness checks below), worker_config.yaml feeds the worker. Each file's path
    // comes from its env var, the same one the child process and the CLI read.
    private void extractConfig() throws IOException {
        controlPlaneFile = seedConfig("control_plane_config.yaml", "CONTROL_PLANE_CONFIG");
        workerFile = seedConfig("worker_config.yaml", "WORKER_CONFIG");
        config = CliConfig.load(controlPlaneFile);
    }

    // Seeds bundled defaults at the env-named path only if absent, so user edits survive.
    private Path seedConfig(String resource, String envVar) throws IOException {
        String dest = System.getenv(envVar);
        if (dest == null || dest.isBlank()) {
            throw new IOException(envVar + " must be set to the path for " + resource);
        }
        Path destPath = Path.of(dest);
        if (Files.notExists(destPath)) {
            if (destPath.getParent() != null) {
                Files.createDirectories(destPath.getParent());
            }
            try (InputStream in = getClass().getResourceAsStream("/" + resource)) {
                if (in == null) {
                    throw new IOException(resource + " not found in JAR resources");
                }
                Files.copy(in, destPath);
            }
        }
        return destPath;
    }

    private void extractMetricsFiles() throws IOException {
        for (String resource : METRICS_RESOURCES) {
            Path dest = SCHEDULER_DIR.resolve(resource);
            Files.createDirectories(dest.getParent());
            try (InputStream in = getClass().getResourceAsStream("/" + resource)) {
                if (in == null) {
                    throw new IOException(resource + " not found in JAR resources");
                }
                Files.copy(in, dest, StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }

    private void startDockerCompose() throws Exception {
        log("Starting control plane...");
        ProcessBuilder pb = new ProcessBuilder(
                "docker", "compose", "-f", composePath.toString(), "up", "-d")
                .redirectErrorStream(true);
        Process process = pb.start();
        streamOutput(process.getInputStream(), "compose");
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("docker-compose up failed with exit code " + exitCode);
        }
    }

    private void waitForServices() throws Exception {
        waitForUrl("MinIO", config.getMinio().getEndpoint() + "/minio/health/ready", 60);
        waitForUrl("Registry", config.getRegistry().getUrl() + "/v2/", 30);
        // MLflow takes longer — installs psycopg2-binary + boto3 on first start
        waitForUrl("MLflow", config.getMlflow().getHealthUrl(), 120);
        waitForUrl("Prometheus", PROMETHEUS_URL + "/-/ready", 30);
        waitForUrl("Grafana", GRAFANA_URL + "/api/health", 60);
    }

    private void logUiUrls() {
        log("");
        log("Stack ready. UIs:");
        if (uiDir != null) {
            log("  Scheduler UI:          http://localhost:" + config.getCoordinator().getHttpPort());
        }
        log("  Grafana (dashboards):  " + GRAFANA_URL);
        log("  Prometheus:            " + PROMETHEUS_URL);
        log("  MinIO console:         http://localhost:9001");
        log("  MLflow:                http://localhost:5000");
    }

    private void waitForUrl(String name, String url, int timeoutSeconds) throws Exception {
        log("  Waiting for " + name + "...");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (System.nanoTime() < deadline) {
            try {
                HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
                conn.setConnectTimeout(2000);
                conn.setReadTimeout(2000);
                if (conn.getResponseCode() == 200) {
                    log("  " + name + " ready");
                    return;
                }
            } catch (IOException ignored) {
            }
            Thread.sleep(1000);
        }
        throw new RuntimeException(name + " did not become ready within " + timeoutSeconds + "s");
    }

    private void startCoordinator(String jarPath) throws IOException {
        // Coordinator reads everything (including its port) from control_plane_config.yaml,
        // located via the CONTROL_PLANE_CONFIG env var — one source, no args.
        log("Starting coordinator (port " + config.getCoordinator().getPort() + ")...");
        ProcessBuilder pb = new ProcessBuilder("java", "-jar", jarPath)
                .redirectErrorStream(true);
        pb.environment().put("CONTROL_PLANE_CONFIG", controlPlaneFile.toString());
        coordinatorProcess = pb.start();
        streamOutputAsync(coordinatorProcess.getInputStream(), "coordinator");
    }

    private void waitForCoordinator() throws Exception {
        CliConfig.Coordinator coordinator = config.getCoordinator();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            try {
                java.net.Socket socket = new java.net.Socket();
                socket.connect(new java.net.InetSocketAddress(coordinator.getHost(), coordinator.getPort()), 500);
                socket.close();
                log("  Coordinator ready");
                return;
            } catch (IOException ignored) {
            }
            Thread.sleep(500);
        }
        throw new RuntimeException("Coordinator did not start within 15s");
    }

    private void startWorker(String jarPath) throws IOException {
        log("Starting worker...");

        ProcessBuilder pb = new ProcessBuilder("java", "-jar", jarPath)
                .redirectErrorStream(true);
        pb.environment().put("WORKER_CONFIG", workerFile.toString());
        workerProcess = pb.start();
        streamOutputAsync(workerProcess.getInputStream(), "worker");
        // Give the worker a moment to register
        try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
        log("  Worker started");
    }

    @Override
    public void close() {
        log("Shutting down...");

        if (workerProcess != null && workerProcess.isAlive()) {
            log("  Stopping worker...");
            workerProcess.destroy();
            try { workerProcess.waitFor(5, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
            if (workerProcess.isAlive()) workerProcess.destroyForcibly();
        }

        if (coordinatorProcess != null && coordinatorProcess.isAlive()) {
            log("  Stopping coordinator...");
            coordinatorProcess.destroy();
            try { coordinatorProcess.waitFor(5, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
            if (coordinatorProcess.isAlive()) coordinatorProcess.destroyForcibly();
        }

        if (composePath != null) {
            System.out.println("  Stopping control plane...");
            try {
                ProcessBuilder pb = new ProcessBuilder(
                        "docker", "compose", "-f", composePath.toString(), "down", "-v")
                        .redirectErrorStream(true);
                Process process = pb.start();
                streamOutput(process.getInputStream(), "compose");
                process.waitFor(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                System.err.println("  Warning: docker-compose down failed: " + e.getMessage());
            }
        }

        log("Shutdown complete");
        if (logWriter != null) {
            logWriter.close();
        }
    }

    void stopDockerCompose() {
        if (composePath == null) {
            composePath = SCHEDULER_DIR.resolve("docker-compose.control-plane.yml");
        }
        if (!Files.exists(composePath)) {
            System.err.println("No compose file found at " + composePath);
            return;
        }
        System.out.println("Stopping control plane...");
        try {
            ProcessBuilder pb = new ProcessBuilder(
                    "docker", "compose", "-f", composePath.toString(), "down", "-v")
                    .redirectErrorStream(true);
            Process process = pb.start();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println("  " + line);
                }
            }
            process.waitFor(30, TimeUnit.SECONDS);
            System.out.println("Control plane stopped");
        } catch (Exception e) {
            System.err.println("Failed to stop: " + e.getMessage());
        }
    }

    private void log(String message) {
        System.out.println(message);
        if (logWriter != null) {
            logWriter.println(message);
        }
    }

    private void streamOutput(InputStream inputStream, String prefix) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (logWriter != null) logWriter.println("[" + prefix + "] " + line);
            }
        } catch (IOException ignored) {
        }
    }

    private void streamOutputAsync(InputStream inputStream, String prefix) {
        Thread thread = new Thread(() -> streamOutput(inputStream, prefix));
        thread.setDaemon(true);
        thread.setName(prefix + "-log-reader");
        thread.start();
    }
}
