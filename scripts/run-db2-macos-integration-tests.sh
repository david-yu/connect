#!/usr/bin/env bash
# Run DB2 CDC integration tests from macOS.
#
# Compiles the test binary for linux/amd64, starts the DB2 container,
# waits for first-time setup to complete, then runs the test binary INSIDE
# the DB2 container via docker exec. Running inside the container avoids
# all IPC namespace and library-extraction problems that arise when
# libdb2.so.1 (the full server library) tries to initialise a local DB2
# instance from inside a separate, isolated container.
#
# Usage:
#   ./scripts/run-db2-macos-integration-tests.sh
#   ./scripts/run-db2-macos-integration-tests.sh -test.run TestIntegrationDB2CDCDriver
#
# Flags:
#   -timeout-minutes N          Override test timeout (default 40).
#   -test.run=<regex>           Override which tests to run.
#   --keep                      Keep the DB2 container alive after the run.
#                               On the next run the existing container is reused,
#                               skipping the 8-minute first-time DB2 setup.
#   --reset                     Remove the named persistent container and volume,
#                               then start fresh.
#
# Speed tips:
#   - Docker Desktop → Resources: CPUs ≥ 8, Memory ≥ 8 GB, Swap ≥ 2 GB.
#   - Use --keep to reuse the container across runs (skips 8-min setup).
#   - The DB2 database is stored in a named Docker volume (db2-testdb-data)
#     so it survives container restarts when used with --keep.
#   - DB2 first-time setup takes ~8 min; plan for ~20 min total on first run.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$(mktemp -t db2test.XXXXXX)"
# Use a stable container name so --keep / container reuse works across runs.
DB2_CTR_NAME="${DB2_CONTAINER_NAME:-db2-integration-test-persistent}"
# Named Docker volume for /database — survives container restarts.
DB2_VOLUME="db2-testdb-data"
SETUP_LOG="$(mktemp -t db2setup.XXXXXX)"
LOG_PID=""
KEEP=false
RESET=false

cleanup() {
  [[ -n "$LOG_PID" ]] && kill "$LOG_PID" 2>/dev/null || true
  rm -f "$BIN" "$SETUP_LOG"
  if [[ "$KEEP" == "false" ]]; then
    echo "==> Cleaning up container: $DB2_CTR_NAME"
    docker rm -f "$DB2_CTR_NAME" 2>/dev/null || true
  else
    echo "==> Keeping container $DB2_CTR_NAME alive for reuse (--keep was set)"
  fi
}
trap cleanup EXIT

# --------------------------------------------------------------------------
# Parse flags
# --------------------------------------------------------------------------
TEST_TIMEOUT="40m"
TEST_RUN="^TestIntegration"
TEST_EXTRA_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -timeout-minutes)
      shift; TEST_TIMEOUT="${1}m"; shift ;;
    -test.run=*)
      TEST_RUN="${1#-test.run=}"; shift ;;
    --keep)
      KEEP=true; shift ;;
    --reset)
      RESET=true; shift ;;
    *)
      TEST_EXTRA_ARGS+=("$1"); shift ;;
  esac
done

# --------------------------------------------------------------------------
# Reset: destroy container + volume so next run starts fresh.
# --------------------------------------------------------------------------
if [[ "$RESET" == "true" ]]; then
  echo "==> --reset: removing container $DB2_CTR_NAME and volume $DB2_VOLUME"
  docker rm -f "$DB2_CTR_NAME" 2>/dev/null || true
  docker volume rm "$DB2_VOLUME" 2>/dev/null || true
  echo "==> Reset complete. Re-run without --reset to start fresh."
  exit 0
fi

# --------------------------------------------------------------------------
# Reuse: if the container already exists and is running, skip setup entirely.
# --------------------------------------------------------------------------
CONTAINER_REUSED=false
if docker ps -q --filter "name=^${DB2_CTR_NAME}$" | grep -q .; then
  echo "==> Reusing existing container $DB2_CTR_NAME (skipping ~8-min first-time setup)"
  CONTAINER_REUSED=true
elif docker ps -aq --filter "name=^${DB2_CTR_NAME}$" | grep -q .; then
  echo "==> Starting stopped container $DB2_CTR_NAME"
  docker start "$DB2_CTR_NAME"
  CONTAINER_REUSED=true
fi

# --------------------------------------------------------------------------
# First run: pull image, create volume, start container.
# --------------------------------------------------------------------------
if [[ "$CONTAINER_REUSED" == "false" ]]; then
  echo "==> Pulling DB2 image (no-op if already cached)"
  docker pull --platform linux/amd64 icr.io/db2_community/db2:latest

  echo "==> Creating named volume $DB2_VOLUME (no-op if already exists)"
  docker volume create "$DB2_VOLUME" >/dev/null

  echo "==> Starting DB2 container $DB2_CTR_NAME (first-time setup ~8 min)"
  docker run --detach \
    --name "$DB2_CTR_NAME" \
    --platform linux/amd64 \
    --privileged \
    --ipc=host \
    --shm-size 512m \
    --volume "${DB2_VOLUME}:/database" \
    -p 50000:50000 \
    -e LICENSE=accept \
    -e DB2INST1_PASSWORD=password \
    -e DBNAME=TESTDB \
    -e ARCHIVE_LOGS=true \
    -e AUTOCONFIG=false \
    -e SAMPLEDB=false \
    -e REPODB=false \
    -e HADR_ENABLED=NO \
    -e UPDATEAVAIL=NO \
    icr.io/db2_community/db2:latest

  # Tail DB2 container logs to a temp file and to stdout simultaneously.
  docker logs -f "$DB2_CTR_NAME" 2>&1 | tee "$SETUP_LOG" | sed 's/^/[db2] /' &
  LOG_PID=$!

  echo "==> Waiting for DB2 first-time setup (up to 25 min)..."
  SETUP_DEADLINE=$(( $(date +%s) + 1500 ))
  while true; do
    if grep -q "Setup has completed" "$SETUP_LOG" 2>/dev/null; then
      echo "==> DB2 setup complete"
      break
    fi
    if [[ $(date +%s) -ge $SETUP_DEADLINE ]]; then
      echo "ERROR: DB2 setup did not complete within 25 min" >&2
      exit 1
    fi
    if ! docker ps -q --filter "name=^${DB2_CTR_NAME}$" | grep -q . 2>/dev/null; then
      echo "ERROR: DB2 container exited before setup completed" >&2
      exit 1
    fi
    sleep 5
  done

  kill "$LOG_PID" 2>/dev/null || true
  LOG_PID=""
fi

# --------------------------------------------------------------------------
# Build and copy test binary.
# --------------------------------------------------------------------------
echo "==> Building linux/amd64 test binary"
cd "$REPO_ROOT"
GOOS=linux GOARCH=amd64 go test -c -o "$BIN" ./internal/impl/db2/

echo "==> Copying test binary into $DB2_CTR_NAME"
docker cp "$BIN" "$DB2_CTR_NAME:/tmp/db2test"
docker exec "$DB2_CTR_NAME" chmod 755 /tmp/db2test

# --------------------------------------------------------------------------
# Run tests inside the container.
# --------------------------------------------------------------------------
# DB2_USE_LOCAL=1 instructs the test helper to skip container creation and
# connect directly to 127.0.0.1:50000 (the local DB2 instance already running
# inside this container).
echo "==> Running integration tests inside DB2 container (timeout: $TEST_TIMEOUT, run: $TEST_RUN)"
docker exec \
  -e DB2_USE_LOCAL=1 \
  -e DB2DIR=/opt/ibm/db2/V12.1 \
  -e DB2INSTANCE=db2inst1 \
  -e DB2MSGPATH=/opt/ibm/db2/V12.1/msg \
  -e LANG=en_US.iso88591 \
  "$DB2_CTR_NAME" \
  /tmp/db2test -test.v -test.timeout "$TEST_TIMEOUT" "-test.run=${TEST_RUN}" "${TEST_EXTRA_ARGS[@]+"${TEST_EXTRA_ARGS[@]}"}"
