#!/usr/bin/env bash
# Open-loop end-to-end load test: a local gateway process and the loadgen on the
# same machine (loopback). Each run uses a fresh gateway so runs are independent.
#
# Usage: scripts/loadtest.sh                       (10k orders/s, 30s measured, 3 runs)
#        RATE=20000 DURATION=20s RUNS=2 scripts/loadtest.sh
#        KAFKA=localhost:9092 scripts/loadtest.sh   with the event log in the path
#                                                   (start Kafka first: docker compose -f deploy/docker-compose.yml up -d --wait kafka)
set -euo pipefail
cd "$(dirname "$0")/.."

RATE="${RATE:-10000}"
DURATION="${DURATION:-30s}"
WARMUP="${WARMUP:-5s}"
RUNS="${RUNS:-3}"
CONNS="${CONNS:-4}"
GRPC_PORT="${GRPC_PORT:-50071}"
HTTP_PORT="${HTTP_PORT:-18071}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
COMMIT="$(git rev-parse --short HEAD 2>/dev/null || echo nogit)"
OUT="bench/results"
mkdir -p bin "$OUT"

go build -o bin/gateway ./cmd/gateway
go build -o bin/loadgen ./cmd/loadgen

GW_PID=""
cleanup() { [ -n "$GW_PID" ] && kill "$GW_PID" 2>/dev/null || true; }
trap cleanup EXIT

KAFKA="${KAFKA:-}"
TAG="${TAG:-$([ -n "$KAFKA" ] && echo kafka || echo nolog)}"
for run in $(seq 1 "$RUNS"); do
  SYMBOL="BTC-USD"; GW_ARGS=()
  if [ -n "$KAFKA" ]; then
    SYMBOL="LT-${STAMP}-${run}"                       # fresh log per run
    GW_ARGS=(-kafka "$KAFKA" -data-dir "$(mktemp -d)")
  fi
  ./bin/gateway -grpc ":${GRPC_PORT}" -http ":${HTTP_PORT}" -symbol "$SYMBOL" ${GW_ARGS[@]+"${GW_ARGS[@]}"} >"$OUT/gateway-${STAMP}-run${run}.log" 2>&1 &
  GW_PID=$!
  for _ in $(seq 1 50); do curl -fsS "localhost:${HTTP_PORT}/healthz" >/dev/null 2>&1 && break; sleep 0.1; done

  echo "== run ${run}/${RUNS}: ${RATE} req/s, ${DURATION} measured =="
  ./bin/loadgen -addr "localhost:${GRPC_PORT}" -symbol "$SYMBOL" -rate "$RATE" -duration "$DURATION" -warmup "$WARMUP" -conns "$CONNS" \
    -note "gateway and loadgen on the same host over loopback; event log: ${KAFKA:-none}" \
    -out "$OUT/gateway-${TAG}-${RATE}rps-${STAMP}-${COMMIT}-run${run}.json"

  curl -fsS "localhost:${HTTP_PORT}/metrics" | grep -E '^(mx_|go_gc_duration_seconds)' \
    >"$OUT/gateway-metrics-${TAG}-${RATE}rps-${STAMP}-${COMMIT}-run${run}.txt" || true
  kill "$GW_PID"; wait "$GW_PID" 2>/dev/null || true; GW_PID=""
done
