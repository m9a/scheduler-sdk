#!/bin/bash
set -e

SDK_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SCHEDULER_DIR="$SDK_DIR/../scheduler"

echo "=== Scheduler CLI test setup ==="
echo

# ── 1. Build coordinator and worker fat JARs ──
echo "Building coordinator and worker fat JARs..."
(cd "$SCHEDULER_DIR" && mvn package -DskipTests -q)
echo "  done"

export SCHEDULER_COORDINATOR_JAR="$SCHEDULER_DIR/scheduler-coordinator/target/scheduler-coordinator-1.0-SNAPSHOT.jar"
export SCHEDULER_WORKER_JAR="$SCHEDULER_DIR/scheduler-worker/target/scheduler-worker-1.0-SNAPSHOT.jar"
echo "  SCHEDULER_COORDINATOR_JAR=$SCHEDULER_COORDINATOR_JAR"
echo "  SCHEDULER_WORKER_JAR=$SCHEDULER_WORKER_JAR"
echo

# ── 2. Build the CLI ──
echo "Building scheduler-client and scheduler-cli..."
(cd "$SDK_DIR" && mvn install -pl scheduler-client -DskipTests -q && mvn package -pl scheduler-cli -DskipTests -q)
echo "  done"
echo

# ── 3. Build sample Docker images ──
echo "Building sample Docker images..."
(cd "$SDK_DIR" && docker build -q -t localhost:5050/sample-job:latest -f sample-job/Dockerfile sample-job/)
echo "  sample-job built"
(cd "$SDK_DIR" && docker build -q -t localhost:5050/sample-py-training-job:latest -f sample-py-training-job/Dockerfile .)
echo "  sample-py-training-job built"
echo

# ── 4. Start docker-compose to get registry up, then push images ──
COMPOSE_FILE="$HOME/.scheduler/docker-compose.control-plane.yml"
echo "Starting docker-compose to push images to registry..."
mkdir -p "$HOME/.scheduler"
cp "$SDK_DIR/scheduler-cli/src/main/resources/docker-compose.control-plane.yml" "$COMPOSE_FILE"
docker compose -f "$COMPOSE_FILE" up -d --quiet-pull 2>/dev/null

echo "  Waiting for registry..."
until curl -sf http://localhost:5050/v2/ > /dev/null 2>&1; do sleep 1; done
echo "  Registry ready"

docker push localhost:5050/sample-job:latest 2>&1 | tail -1
docker push localhost:5050/sample-py-training-job:latest 2>&1 | tail -1
echo "  Images pushed"
echo

# ── 5. Launch the interactive CLI ──
echo "Launching scheduler CLI..."
echo
exec java -jar "$SDK_DIR/scheduler-cli/target/scheduler-cli-1.0-SNAPSHOT.jar"
