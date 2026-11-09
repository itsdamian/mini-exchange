#!/usr/bin/env bash
# Shared helpers for the chaos scripts. Sourced, not run.
# The Kafka broker comes from deploy/docker-compose.yml (fixed port, container
# survives kill/start). The gateway and loadgen run natively.

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

KAFKA_ADDR="${KAFKA_ADDR:-localhost:9092}"
GRPC_PORT="${GRPC_PORT:-50081}"
HTTP_PORT="${HTTP_PORT:-18081}"
RATE="${RATE:-5000}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
SYMBOL="${SYMBOL:-CHAOS-$STAMP}"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/mx-chaos.XXXXXX")"
DATA_DIR="$WORK/data"
OUT="bench/results"
mkdir -p bin "$OUT" "$DATA_DIR"
GW_PID=""

say() { printf '\n== %s ==\n' "$*"; }

build() {
  go build -o bin/gateway ./cmd/gateway
  go build -o bin/loadgen ./cmd/loadgen
  go build -o bin/logtool ./cmd/logtool
}

kafka_up() {
  docker compose -f deploy/docker-compose.yml up -d --wait kafka >/dev/null
  KAFKA_CID="$(docker compose -f deploy/docker-compose.yml ps -q kafka)"
}

metric() { # metric <name>  -> value from the running gateway
  curl -fsS "localhost:${HTTP_PORT}/metrics" | awk -v n="$1" '$1==n {print $2}'
}

start_gateway() { # start_gateway <logfile>
  ./bin/gateway -grpc ":${GRPC_PORT}" -http ":${HTTP_PORT}" -symbol "$SYMBOL" -kafka "$KAFKA_ADDR" \
    -data-dir "$DATA_DIR" -snapshot-every "${SNAP_EVERY:-20000}" -log-high-watermark "${HIGH_WATER:-4096}" >>"$1" 2>&1 &
  GW_PID=$!
  GW_START_NS=$(python3 -c 'import time;print(time.monotonic_ns())')
  for _ in $(seq 1 600); do
    curl -fsS "localhost:${HTTP_PORT}/readyz" >/dev/null 2>&1 && break
    sleep 0.1
  done
  GW_READY_MS=$(python3 -c "import time;print(int((time.monotonic_ns()-$GW_START_NS)/1e6))")
}

stop_gateway() { [ -n "$GW_PID" ] && kill -TERM "$GW_PID" 2>/dev/null && wait "$GW_PID" 2>/dev/null || true; GW_PID=""; }

cleanup() {
  [ -n "$GW_PID" ] && kill -9 "$GW_PID" 2>/dev/null || true
  [ -n "${LG_PID:-}" ] && kill "$LG_PID" 2>/dev/null || true
}
trap cleanup EXIT

recovered_line() { # recovered_line <logfile> [n]  -> n-th "recovered" JSON line (default last)
  grep '"msg":"recovered"' "$1" | sed -n "${2:-\$}p"
}
