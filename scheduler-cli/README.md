# scheduler-cli

Interactive command-line tool for the distributed job scheduler. Starts all infrastructure (MinIO, MLflow, Docker registry, Prometheus, Grafana, coordinator, worker), presents a menu of sample jobs, streams job output in real time, and tears everything down on exit.

## Prerequisites

### 1. Build coordinator and worker fat JARs

```bash
cd /path/to/scheduler
mvn package -DskipTests
```

### 2. Set environment variables

```bash
export SCHEDULER_COORDINATOR_JAR=/path/to/scheduler/scheduler-coordinator/target/scheduler-coordinator-1.0-SNAPSHOT.jar
export SCHEDULER_WORKER_JAR=/path/to/scheduler/scheduler-worker/target/scheduler-worker-1.0-SNAPSHOT.jar
```

### 3. Build and push sample Docker images

```bash
cd /path/to/scheduler-sdk

# Java ETL job
docker build -t localhost:5050/sample-job:latest -f sample-job/Dockerfile sample-job/
docker push localhost:5050/sample-job:latest

# MNIST Lightning training job
docker build -t localhost:5050/sample-py-training-job:latest -f sample-py-training-job/Dockerfile .
docker push localhost:5050/sample-py-training-job:latest
```

Note: the Docker registry must be running before you can push images. Run `scheduler` once to bring up infrastructure, then push images in another terminal.

### 4. Build the CLI

```bash
cd /path/to/scheduler-sdk
mvn install -pl scheduler-client
mvn package -pl scheduler-cli -DskipTests
```

## Usage

### Interactive mode (default)

```bash
java -jar scheduler-cli/target/scheduler-cli-1.0-SNAPSHOT.jar
```

Or with an alias:
```bash
alias scheduler='java -jar /path/to/scheduler-sdk/scheduler-cli/target/scheduler-cli-1.0-SNAPSHOT.jar'
scheduler
```

This starts all infrastructure, then presents a job menu:

```
Starting control plane...
  MinIO ready
  MLflow ready
  Registry ready
  Prometheus ready
  Grafana ready
Starting coordinator (port 9090)... ready
Starting worker... started

Stack ready. UIs:
  Grafana (dashboards):  http://localhost:3000
  Prometheus:            http://localhost:9095
  MinIO console:         http://localhost:9001
  MLflow:                http://localhost:5000

Logs: ~/.scheduler/logs/session-2026-06-02T14-30-05.log

Available jobs:

  1  MNIST training with Lightning + MLflow metrics
     → sample-py-training-job/train.py

  2  Java ETL pipeline (3 tasks: extract, transform, load)
     → sample-job/src/main/java/com/scheduler/sample/DailySalesEtlJob.java

  q  Stop and exit

Enter selection:
```

Pick a number to run a job. The job's container output streams to the terminal in real time. After it finishes, the menu reappears so you can run another job or quit.

All infrastructure logs (docker-compose, coordinator, worker) are written to `~/.scheduler/logs/session-*.log`.

### Cleanup after a crash

If the tool exits abnormally and leaves containers running:

```bash
scheduler stop
```

### Advanced commands

These commands assume infrastructure is already running (started by `scheduler run` or manually):

```bash
# Submit a custom job
scheduler submit --name my-job --image sample-job:latest --param region=us --wait

# Check job status
scheduler status <job-id>

# Wait for a job to finish
scheduler wait <job-id> --timeout 600

# List output files
scheduler files <job-id>

# Download an output file
scheduler download <job-id> model.pt
```

## What starts and what stops

| Component | Managed by | Start | Stop |
|-----------|-----------|-------|------|
| MinIO, PostgreSQL, MLflow, Registry, Prometheus, Grafana | docker-compose | `scheduler` / `scheduler run` | Exit menu (`q`) or Ctrl+C |
| Coordinator | Local Java process | `scheduler` / `scheduler run` | Exit menu (`q`) or Ctrl+C |
| Worker | Local Java process | `scheduler` / `scheduler run` | Exit menu (`q`) or Ctrl+C |
| Job containers | Worker (via `docker run`) | When job is submitted | Container exits when job finishes |
