# Scheduler SDK

SDKs for running jobs on the [distributed scheduler](../scheduler). Supports both Java and Python. Handles execution lifecycle and status reporting — the job author writes task logic, the SDK manages everything else.

## How it works

```
WorkerAgent (worker JVM)          Job process (Docker container)
────────────────────────          ────────────────────────────────
docker run ... <artifactUri>  →   SDK reads EXECUTION_PAYLOAD env var
                                  SDK opens WebSocket to WorkerAgent
                              ←   [0x01][StatusUpdate proto]  (task RUNNING)
                              ←   [0x01][StatusUpdate proto]  (task COMPLETED)
                                  process exits
```

1. WorkerAgent spawns a Docker container with `EXECUTION_PAYLOAD` (base64 JSON containing `workerAgentUrl`, `jobId`, `params`)
2. The SDK decodes the payload, loads the job, and runs tasks sequentially
3. Task status updates are sent as binary proto over WebSocket (prefix byte `0x01` + `StatusUpdate` proto)
4. WorkerAgent forwards updates to the coordinator via gRPC

## Java SDK

Job authors implement `Task` and call `JobProcess.run()` from their JAR's main method:

```java
public class MyEtlJob {
    public static void main(String[] args) {
        JobProcess.run(List.of(
            new ExtractTask(),
            new TransformTask(),
            new LoadTask()
        ));
    }
}

class ExtractTask implements Task {
    public String name() { return "extract"; }

    public void execute(TaskContext ctx) throws Exception {
        ctx.progress(0.5, "Loading data");
        // ... work ...
        ctx.metric("rows_processed", 1000);
    }
}
```

Each `Task` receives a `TaskContext` with `progress(fraction, message)` and `metric(name, value)` for structured reporting. The SDK handles status transitions (RUNNING → COMPLETED/FAILED), stdout capture, and duration tracking automatically.

### Dockerfile (Java)

```dockerfile
FROM eclipse-temurin:21-jre
COPY target/my-job.jar /opt/job/my-job.jar
ENTRYPOINT ["java", "-cp", "/opt/job/my-job.jar", "com.example.MyEtlJob"]
```

## Python SDK

The `py-sdk` provides a `job_runner` module with `@job`/`@task` decorators:

```python
from job_runner import job, task

@job(id="my-etl", description="Daily ETL pipeline")
class MyEtlJob:

    def __init__(self, region: str, batch_size: int = 1000):
        self.region = region
        self.batch_size = int(batch_size)

    @task("extract", order=1)
    def extract(self):
        ...

    @task("transform", order=2)
    def transform(self):
        ...
```

The SDK discovers `@task` methods, runs them in `order`, and reports each task's status (RUNNING, COMPLETED, FAILED) to the worker automatically.

### Dockerfile (Python)

```dockerfile
FROM python:3.12-slim
RUN pip install --no-cache-dir websocket-client protobuf
COPY py-sdk/job_runner /opt/scheduler/job_runner
COPY py-sdk/scheduler /opt/scheduler/scheduler
ENV PYTHONPATH=/opt/scheduler
COPY your-job/script.py /opt/job/script.py
ENTRYPOINT ["python3", "-m", "job_runner", "/opt/job/script.py"]
```

## Training metrics

Training metrics (loss, accuracy, etc.) go directly from the job process to MLflow — they are **not** routed through the scheduler. The SDK auto-configures this; job authors write standard PyTorch/Lightning code.

**Separation of concerns:**

| Layer | Responsibility |
|-------|---------------|
| **You write** | Standard PyTorch/Lightning code. Call `self.log()` for metrics you care about. |
| **SDK handles** | Auto-injects `MLFlowLogger` into Lightning Trainer when `MLFLOW_TRACKING_URI` is set. Sets `mlflow.set_tracking_uri()` for raw PyTorch. No-op when mlflow isn't installed. |
| **Infrastructure provides** | MLflow server, PostgreSQL backend, MinIO artifact store (`docker-compose.test.yml`). WorkerAgent passes `MLFLOW_TRACKING_URI` and `--network` to containers. |

### With Lightning (fully automatic)

The SDK monkey-patches `Trainer.__init__` to inject `MLFlowLogger` before user code loads (`setup_mlflow()` in `py-sdk/job_runner/metrics.py`). Job authors write zero MLflow code:

```python
# sample-py-training-job/train.py — no MLflow imports, no logger setup
trainer = Trainer(max_epochs=self.epochs, accelerator="auto")
trainer.fit(self.model, self.datamodule)
```

- `self.log("val_acc", value)` — standard Lightning API, works with or without TorchMetrics
- SDK injects `MLFlowLogger(experiment_name=<@job id>, run_name=<jobId>)`
- If the user explicitly passes `logger=...`, the SDK respects it (no override)

### Without Lightning (manual mlflow calls)

The SDK calls `mlflow.set_tracking_uri()` automatically — the user doesn't configure the URI. The user manages experiment/run/metric calls:

```python
import mlflow

mlflow.set_experiment("my-experiment")
with mlflow.start_run():
    for epoch in range(epochs):
        loss = train_one_epoch(model, data)
        mlflow.log_metric("train_loss", loss, step=epoch)
```

Requires `mlflow` in the Dockerfile. Contrast with Lightning: here the user writes MLflow API calls directly; with Lightning the user writes zero MLflow code.

### No metrics (ETL, inference, etc.)

Non-training jobs don't install mlflow in their Dockerfile. The SDK's `setup_mlflow()` is a no-op when mlflow isn't importable — nothing to configure.

## File I/O

Inside the container:
- `/workspace/input/` — input files staged by WorkerAgent from object storage (read-only)
- `/workspace/output/` — write output files here; WorkerAgent uploads them after the job exits

## Sample jobs

| Directory | Language | Description |
|-----------|----------|-------------|
| `sample-job/` | Java | ETL pipeline with extract/transform/load tasks |
| `sample-failing-job/` | Java | Deliberately fails to test error handling |
| `sample-py-job/` | Python | ETL pipeline using `@job`/`@task` decorators |
| `sample-py-failing-job/` | Python | Deliberately fails to test error handling |
| `sample-pytorch-job/` | Python | Raw PyTorch LSTM training |
| `sample-py-training-job/` | Python | Lightning MNIST with TorchMetrics |
| `sample-inference-job/` | Python | Inference server using `@job`/`@task` |

## Build

### Prerequisites

The `scheduler-client` module depends on `scheduler-proto` from the [scheduler](../scheduler) repo. Install it to your local Maven repository first:

```bash
cd ../scheduler
mvn install -pl scheduler-proto
```

### Compile

```bash
mvn compile -pl java-sdk           # Java SDK only
mvn compile -pl scheduler-client   # client library only
mvn compile                        # all modules
```

### Python

```bash
python3 -m pytest py-sdk/tests/

# Regenerate Python proto bindings (requires protoc)
protoc --proto_path=java-sdk/src/main/proto \
       --python_out=py-sdk \
       java-sdk/src/main/proto/scheduler/v1/common.proto \
       java-sdk/src/main/proto/scheduler/v1/job_message.proto
```
