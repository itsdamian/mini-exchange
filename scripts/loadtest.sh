#!/usr/bin/env bash
# Open-loop end-to-end load test: a local gateway process and the loadgen on the
# same machine (loopback). Each run uses a fresh gateway so runs are independent.
#
# Usage: scripts/loadtest.sh                       (10k orders/s, 30s measured, 3 runs)
#        RATE=20000 DURATION=20s RUNS=2 scripts/loadtest.sh
#        KAFKA=localhost:9092 scripts/loadtest.sh   with the event log in the path
#                                                   (start Kafka first: docker compose -f deploy/docker-compose.yml up -d --wait kafka)
#        REDIS=localhost:16379 scripts/loadtest.sh  with pre-trade risk in the path (Redis balances are funded
#                                                   first and audited after each run: funds must be conserved)
#                                                   (start Redis first: docker compose -f deploy/docker-compose.yml up -d --wait redis)
#        KAFKA=... REDIS=... PG=1 scripts/loadtest.sh  also run the ledger consumer into the compose PostgreSQL
#                                                   (PG=<dsn> to use another one), then reconcile the ledger
#                                                   with the event log and Redis after each run (needs both above)
#        WS=1000 KAFKA=... scripts/loadtest.sh      also run marketdata and that many WebSocket clients during the load, and
#                                                   record marketdata's memory and the frames the clients got (FR-7)
#        GW_EXTRA="-kafka-linger 1ms" scripts/loadtest.sh   extra gateway flags (phase 8 experiments)
#        GW_ENV="GOGC=400 GOMEMLIMIT=2GiB" scripts/loadtest.sh   environment of the gateway process only (not the load generator)
#        PPROF=1 scripts/loadtest.sh                capture a 20 s CPU profile and the allocation profile of the gateway
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
go build -o bin/risktool ./cmd/risktool
go build -o bin/ledger ./cmd/ledger
go build -o bin/marketdata ./cmd/marketdata
go build -o bin/wsload ./cmd/wsload

GW_PID=""; LEDGER_PID=""; MD_PID=""; RSS_PID=""
cleanup() { [ -n "$MD_PID" ] && kill "$MD_PID" 2>/dev/null || true; [ -n "$RSS_PID" ] && kill "$RSS_PID" 2>/dev/null || true; [ -n "$GW_PID" ] && kill "$GW_PID" 2>/dev/null || true; [ -n "$LEDGER_PID" ] && kill "$LEDGER_PID" 2>/dev/null || true; }
trap cleanup EXIT

KAFKA="${KAFKA:-}"
REDIS="${REDIS:-}"
PG="${PG:-}"
WS="${WS:-0}"
GW_EXTRA="${GW_EXTRA:-}"
GW_ENV="${GW_ENV:-}"   # environment for the gateway process only, e.g. "GOGC=400 GOMEMLIMIT=2GiB"
PPROF="${PPROF:-}"
PPROF_PORT="${PPROF_PORT:-16071}"
MD_HTTP_PORT="${MD_HTTP_PORT:-18083}"
[ "$WS" -gt 0 ] && { [ -n "$KAFKA" ] || { echo "WS needs KAFKA" >&2; exit 2; }; }
PG_HTTP_PORT="${PG_HTTP_PORT:-18072}"
if [ -n "$PG" ]; then
  [ -n "$KAFKA" ] && [ -n "$REDIS" ] || { echo "PG needs KAFKA and REDIS" >&2; exit 2; }
  [ "$PG" = "1" ] && PG="postgres://exchange:exchange-local-dev@localhost:15432/exchange?sslmode=disable"
fi
# Per account, generous enough that funds never run out: the test measures the
# cost of the risk check, not rejections. 16 accounts match the load generator.
ACCOUNTS=16; FUND_BASE=1000000000; FUND_QUOTE=1000000000000
LABEL="$([ -n "$KAFKA" ] && echo kafka || echo nolog)"
[ -n "$REDIS" ] && LABEL="${LABEL}+risk"
[ -n "$PG" ] && LABEL="${LABEL}+ledger"
[ "$WS" -gt 0 ] && LABEL="${LABEL}+ws${WS}"
TAG="${TAG:-$LABEL}"
for run in $(seq 1 "$RUNS"); do
  SYMBOL="BTC-USD"; GW_ARGS=()
  if [ -n "$KAFKA" ]; then
    SYMBOL="LT-${STAMP}-${run}"                       # fresh log per run
    [ -n "$PG" ] && SYMBOL="LT${STAMP}R${run}-USD"      # the ledger needs BASE-QUOTE
    GW_ARGS=(-kafka "$KAFKA" -data-dir "$(mktemp -d)")
  fi
  if [ -n "$REDIS" ]; then
    # Fresh balances for every run: flush only the risk keys of these accounts.
    for a in $(seq 1 "$ACCOUNTS"); do redis-cli -u "redis://${REDIS}" --scan --pattern "{${a}}:*" | xargs -n 100 redis-cli -u "redis://${REDIS}" del >/dev/null 2>&1 || true; done
    ./bin/risktool fund -redis "$REDIS" -from 1 -to "$ACCOUNTS" -base "$FUND_BASE" -quote "$FUND_QUOTE"
    # A rate limit high enough not to interfere; the limits themselves are tested separately.
    GW_ARGS+=(-redis "$REDIS" -risk-rate 1000000 -risk-burst 1000000 -risk-max-open 100000)
  fi
  if [ -n "$PG" ]; then
    # Fresh ledger for every run, funded like Redis (compose PostgreSQL: wipe through psql in the container).
    docker compose -f deploy/docker-compose.yml exec -T postgres psql -U exchange -d exchange -q \
      -c "TRUNCATE entries, settlements, accounts" >/dev/null 2>&1 || true
    ./bin/ledger deposit -pg "$PG" -symbol "$SYMBOL" -account 1 -to "$ACCOUNTS" -base "$FUND_BASE" -quote "$FUND_QUOTE" -ref "lt-${STAMP}-${run}" >/dev/null
  fi
  [ -n "$PPROF" ] && GW_ARGS+=(-pprof-addr "localhost:${PPROF_PORT}")
  # shellcheck disable=SC2086  # GW_EXTRA is a list of flags on purpose
  env $GW_ENV ./bin/gateway -grpc ":${GRPC_PORT}" -http ":${HTTP_PORT}" -symbol "$SYMBOL" ${GW_ARGS[@]+"${GW_ARGS[@]}"} $GW_EXTRA >"$OUT/gateway-${STAMP}-run${run}.log" 2>&1 &
  GW_PID=$!
  for _ in $(seq 1 50); do curl -fsS "localhost:${HTTP_PORT}/healthz" >/dev/null 2>&1 && break; sleep 0.1; done

  if [ -n "$PG" ]; then
    ./bin/ledger run -pg "$PG" -kafka "$KAFKA" -symbol "$SYMBOL" -group "g-${SYMBOL}" -http ":${PG_HTTP_PORT}" >"$OUT/ledger-${STAMP}-run${run}.log" 2>&1 &
    LEDGER_PID=$!
  fi
  if [ "$WS" -gt 0 ]; then
    ./bin/marketdata -kafka "$KAFKA" -symbol "$SYMBOL" -http ":${MD_HTTP_PORT}" >"$OUT/marketdata-${STAMP}-run${run}.log" 2>&1 &
    MD_PID=$!
    for _ in $(seq 1 100); do curl -fsS "localhost:${MD_HTTP_PORT}/healthz" >/dev/null 2>&1 && break; sleep 0.1; done
    RSS_FILE="$OUT/marketdata-rss-${TAG}-${RATE}rps-${STAMP}-${COMMIT}-run${run}.txt"; : >"$RSS_FILE"
    ( while kill -0 "$MD_PID" 2>/dev/null; do echo "$(date +%s) $(ps -o rss= -p "$MD_PID" | tr -d ' ')" >>"$RSS_FILE"; sleep 2; done ) &
    RSS_PID=$!
    # Clients connect first and listen until the load is over (SIGINT ends the measurement).
    ./bin/wsload -url "ws://localhost:${MD_HTTP_PORT}/ws" -clients "$WS" -duration 10m \
      -out "$OUT/wsload-${TAG}-${RATE}rps-${STAMP}-${COMMIT}-run${run}.json" >/dev/null &
    WS_PID=$!
    sleep 8   # let the clients connect before the load starts
  fi
  if [ -n "$PPROF" ]; then
    # 20 s of CPU profile starting 5 s into the measured period.
    ( sleep $(( $(printf '%s' "$WARMUP" | tr -dc 0-9) + 5 ))
      curl -fsS "localhost:${PPROF_PORT}/debug/pprof/profile?seconds=20" -o "$OUT/gateway-cpu-${TAG}-${RATE}rps-${STAMP}-${COMMIT}-run${run}.pprof" ) &
  fi
  echo "== run ${run}/${RUNS}: ${RATE} req/s, ${DURATION} measured =="
  ./bin/loadgen -addr "localhost:${GRPC_PORT}" -symbol "$SYMBOL" -rate "$RATE" -duration "$DURATION" -warmup "$WARMUP" -conns "$CONNS" \
    -note "gateway and loadgen on the same host over loopback; event log: ${KAFKA:-none}" \
    -out "$OUT/gateway-${TAG}-${RATE}rps-${STAMP}-${COMMIT}-run${run}.json"

  [ -n "$PPROF" ] && curl -fsS "localhost:${PPROF_PORT}/debug/pprof/allocs" -o "$OUT/gateway-allocs-${TAG}-${RATE}rps-${STAMP}-${COMMIT}-run${run}.pprof" || true
  curl -fsS "localhost:${HTTP_PORT}/metrics" | grep -E '^(mx_|go_gc_duration_seconds|go_memstats_alloc_bytes_total|go_memstats_mallocs_total|process_cpu_seconds_total)' \
    >"$OUT/gateway-metrics-${TAG}-${RATE}rps-${STAMP}-${COMMIT}-run${run}.txt" || true
  if [ "$WS" -gt 0 ]; then
    curl -fsS "localhost:${MD_HTTP_PORT}/metrics" | grep -E '^(mx_md_|process_resident_memory_bytes)' \
      >"$OUT/marketdata-metrics-${TAG}-${RATE}rps-${STAMP}-${COMMIT}-run${run}.txt" || true
    kill -INT "$WS_PID" 2>/dev/null || true; wait "$WS_PID" 2>/dev/null || true
    kill "$RSS_PID" "$MD_PID" 2>/dev/null || true; wait "$MD_PID" 2>/dev/null || true; MD_PID=""; RSS_PID=""
  fi
  kill "$GW_PID"; wait "$GW_PID" 2>/dev/null || true; GW_PID=""   # SIGTERM drains the settlement queue
  if [ -n "$PG" ]; then
    # The gateway has flushed the log; give the ledger time to catch up, then compare
    # the ledger with the event log and Redis. Non-zero exit (set -e) on any discrepancy.
    ./bin/ledger reconcile -pg "$PG" -kafka "$KAFKA" -symbol "$SYMBOL" -redis "$REDIS" -wait 120s | tee "$OUT/reconcile-${TAG}-${RATE}rps-${STAMP}-${COMMIT}-run${run}.json"
    curl -fsS "localhost:${PG_HTTP_PORT}/metrics" | grep -E '^mx_ledger_' >"$OUT/ledger-metrics-${TAG}-${RATE}rps-${STAMP}-${COMMIT}-run${run}.txt" || true
    kill "$LEDGER_PID"; wait "$LEDGER_PID" 2>/dev/null || true; LEDGER_PID=""
  fi
  if [ -n "$REDIS" ]; then
    ./bin/risktool audit -redis "$REDIS" -expect-base $((ACCOUNTS * FUND_BASE)) -expect-quote $((ACCOUNTS * FUND_QUOTE)) | tee "$OUT/risk-audit-${TAG}-${RATE}rps-${STAMP}-${COMMIT}-run${run}.txt"
  fi
done
