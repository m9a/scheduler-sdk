package com.scheduler.cli;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

/**
 * Manages the lifecycle of all infrastructure: docker-compose services
 * (MinIO, PostgreSQL, MLflow, Docker registry) and local Java processes
 * (coordinator, worker). Streams all process output to both the terminal
 * and a session log file.
 */
class InfraManager implements AutoCloseable {

    private static final Path SCHEDULER_DIR = Path.of(System.getProperty("user.home"), ".scheduler");
    private static final int COORDINATOR_PORT = 9090;

    private Path composePath;
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
        startDockerCompose();
        waitForServices();
        startCoordinator(coordinatorJar);
        waitForCoordinator();
        startWorker(workerJar);
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
        waitForUrl("MinIO", "http://localhost:9000/minio/health/ready", 60);
        waitForUrl("Registry", "http://localhost:5050/v2/", 30);
        // MLflow takes longer — installs psycopg2-binary + boto3 on first start
        waitForUrl("MLflow", "http://localhost:5000/health", 120);
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
        log("Starting coordinator (port " + COORDINATOR_PORT + ")...");
        ProcessBuilder pb = new ProcessBuilder(
                "java", "-jar", jarPath, String.valueOf(COORDINATOR_PORT))
                .redirectErrorStream(true);
        coordinatorProcess = pb.start();
        streamOutputAsync(coordinatorProcess.getInputStream(), "coordinator");
    }

    private void waitForCoordinator() throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            try {
                java.net.Socket socket = new java.net.Socket();
                socket.connect(new java.net.InetSocketAddress("localhost", COORDINATOR_PORT), 500);
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

        Path configFile = SCHEDULER_DIR.resolve("worker.yaml");
        Files.writeString(configFile, String.join("\n",
                "coordinator:",
                "  host: localhost",
                "  port: " + COORDINATOR_PORT,
                "",
                "worker:",
                "  hostname: host.docker.internal",
                "  capacity: 1",
                "",
                "docker:",
                "  network: scheduler-net",
                "",
                "mlflow:",
                "  trackingUri: http://mlflow:5000",
                ""));

        ProcessBuilder pb = new ProcessBuilder(
                "java", "-jar", jarPath, "--config", configFile.toString())
                .redirectErrorStream(true);
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
