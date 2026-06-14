# Scheduler SDK

SDKs for running jobs on the [distributed scheduler](../scheduler). Two SDKs: **Java** and **Python** — PyTorch jobs are plain Python jobs (the SDK has no framework-specific code). The job author writes task logic; the SDK handles status reporting, telemetry, stdout capture, and MLflow wiring.

## How it works

```
WorkerAgent (worker JVM)          Job process (Docker container)
────────────────────────          ────────────────────────────────
docker run ... <artifactUri>  →   SDK reads EXECUTION_PAYLOAD env var
                                  SDK opens WebSocket to WorkerAgent
                              ←   [0x01][StatusUpdate]  task RUNNING/COMPLETED/FAILED
                              ←   [0x03][Report]        progress / metrics / events
                                  process exits
```

1. WorkerAgent spawns a Docker container with `EXECUTION_PAYLOAD` (base64 JSON containing `workerAgentUrl`, `jobId`, `params`)
2. The SDK decodes the payload, constructs the `@job` class, and runs its `@task` methods in `order`
3. Task status (`0x01`) and key-value telemetry (`0x03`) are sent as binary proto over WebSocket — telemetry on one persistent connection, each status frame on its own short-lived connection (loss-proof for state transitions)
4. WorkerAgent forwards updates to the coordinator via gRPC

## Writing a job

A job is a `@job`-decorated class with `@task` methods — this is the only way to define one. Every task receives a **`TaskContext`** (`ctx`), the entire API inside a task:

| Call | Meaning |
|------|---------|
| `ctx.progress(current, total)` | "I'm on step N of M" — drives % complete, ETA, stall detection |
| `ctx.metric(key, value)` | A measurement, latest-wins. Numbers (loss) or strings (model_version) |
| `ctx.event(name, detail)` | A one-off note (checkpoint_saved) — sent immediately |

Telemetry is throttled (≤1 frame/s, latest value per key) — call `ctx.progress`/`ctx.metric` as often as you like, even per batch.

### Python

```python
from job_runner import job, task, before_job, after_job

@job(id="my-etl", description="Daily ETL pipeline")
class MyEtlJob:

    def __init__(self, region: str, batch_size: int = 1000):
        self.region = region
        self.batch_size = int(batch_size)
        self.row_count = 0          # inter-task data = instance fields

    @before_job
    def setup(self):
        ...

    @task("extract", order=1)
    def extract(self, ctx):
        self.row_count = fetch_rows(self.region)
        ctx.metric("rows_extracted", self.row_count)

    @task("transform", order=2)
    def transform(self, ctx):
        for i, batch in enumerate(batches(self.row_count, self.batch_size)):
            process(batch)
            ctx.progress(i + 1, num_batches)
```

Constructor parameters are filled from the submitted job `params` by name (with type coercion from annotations; defaults apply when a param is missing).

### Java

```java
@Job(id = "my-etl", description = "Daily ETL pipeline")
public class MyEtlJob {

    private int rowCount;           // inter-task data = instance fields

    public MyEtlJob(@Param("region") String region,
                    @Param(value = "batchSize", defaultValue = "1000") int batchSize) { ... }

    @Task(name = "extract", order = 1, critical = true)
    public void extract(@Context TaskContext ctx) {
        rowCount = fetchRows();
        ctx.metric("rows_extracted", rowCount);
    }

    @Task(name = "transform", order = 2, dependsOn = "extract")
    public void transform(@Context TaskContext ctx) {
        for (int i = 0; i < batches; i++) {
            ctx.progress(i + 1, batches);
        }
    }
}
```

The annotation processor generates `MyEtlJob_Harness` (the container entry point) and a descriptor. The harness runs tasks in order, injects `TaskContext`, captures each task's stdout/stderr into its status update, and reports RUNNING/COMPLETED/FAILED — no user code for any of this.

### PyTorch

PyTorch jobs are plain Python jobs — the SDK contains no framework-specific code (no callbacks, no patching). Which training framework to use (raw torch, Lightning, ...) is entirely the user's choice.

- **Metric history → MLflow, automatically.** The runner calls `mlflow.set_tracking_uri` + `mlflow.set_experiment(<job id>)` + `mlflow.autolog()` when mlflow is installed — MLflow's own integration covers Lightning, torch, sklearn, etc. with zero plumbing.
- **Progress → scheduler, user's call.** In a raw loop: `ctx.progress(epoch, num_epochs)` per epoch (see `sample-pytorch-job`). With Lightning, report progress from your own code if you want it — or skip it; status + MLflow history still work.

## Building & packaging a job

Jobs are Docker images; the image reference is the job's `artifactUri`.

### Python

```dockerfile
FROM python:3.12-slim
RUN pip install --no-cache-dir websocket-client protobuf
COPY py-sdk/job_runner /opt/scheduler/job_runner
COPY py-sdk/scheduler /opt/scheduler/scheduler
ENV PYTHONPATH=/opt/scheduler
COPY my_job.py /opt/job/my_job.py
ENTRYPOINT ["python3", "-m", "job_runner", "/opt/job/my_job.py"]
```

Training images additionally install `torch pytorch-lightning mlflow`.

### Java

Build a fat JAR with the generated harness as the main class:

```xml
<mainClass>com.example.MyEtlJob_Harness</mainClass>
```

```dockerfile
FROM eclipse-temurin:21-jre
COPY target/my-job.jar /opt/job/app.jar
ENTRYPOINT ["java", "-jar", "/opt/job/app.jar"]
```

Push the image to the local registry and submit:

```bash
docker build -t localhost:5050/my-etl:latest .
docker push localhost:5050/my-etl:latest
scheduler submit --name my-etl --image my-etl:latest --param region=emea
```

## Monitoring a job

Telemetry routing — one rule: **coordinator shows now, MLflow shows history, Grafana shows machines.**

### Status & progress (coordinator)

```bash
scheduler status <job-id>          # job + per-task status, timestamps, errors
scheduler wait <job-id>            # block until terminal state
scheduler files <job-id>           # list output files
scheduler download <job-id> <path> # fetch an output file
```

Programmatic access: `scheduler-client` (`SchedulerClient.getJobStatus(jobId)` over gRPC).

Telemetry (progress/metrics/events) flows job → worker (`0x03` frame) →
coordinator (`ReportTelemetry` RPC) and is stored as a latest-wins snapshot per
task. `GetJobStatus` returns it on each task's `reports` field — key, kind
(METRIC/ATTRIBUTE/EVENT), and value (e.g. `progress_current`/`progress_total`,
`loss`, `transform_duration_ms`).

> **Placeholder — not built yet:** derived/rendered views. Planned: `scheduler
> status` rendering of % complete + ETA + latest metrics, a `STALLED` flag
> (RUNNING with no report past a threshold), and an SDK liveness ping so
> uninstrumented jobs get stall detection too.

### Training metrics (MLflow)

- UI: `http://localhost:5000` (started by `scheduler run`'s control plane).
- Experiment = the `@job` id; each execution is a run inside it.
- Holds per-step metric history, parameters, and artifacts via `mlflow.autolog()`.

### Machine metrics (Prometheus/Grafana)

> **Placeholder — not built yet:** CPU/GPU/memory/container metrics sampled by
> the **worker** (not the SDK) and exported to Prometheus, with Grafana
> dashboards. The SDK never touches Prometheus.

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
| `sample-py-training-job/` | Python | Lightning MNIST with MLflow autolog |
| `sample-inference-job/` | Python | Inference server using `@job`/`@task` |

## Build (this repo)

### Prerequisites

Modules depend on `scheduler-proto` from the [scheduler](../scheduler) repo — the single source of truth for protos (never copied here). Install it first:

```bash
cd ../scheduler
mvn install -pl scheduler-proto
```

### Compile & test

```bash
mvn compile                        # all Java modules
mvn test -pl java-sdk              # Java SDK tests
python3 -m pytest py-sdk/tests/    # Python SDK tests
```

### Regenerate Python proto bindings

```bash
protoc --proto_path=../scheduler/scheduler-proto/src/main/proto \
       --python_out=py-sdk \
       scheduler/v1/common.proto scheduler/v1/job_message.proto
```
