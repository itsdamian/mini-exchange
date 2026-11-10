#!/usr/bin/env bash
# Reproduce every phase 8 number in BENCHMARKS.md with one command:
#
#     scripts/bench-all.sh                 full run (about 45 minutes)
#     QUICK=1 scripts/bench-all.sh         a short run to check the script works (about 10 minutes)
#     KAFKA=localhost:9092 scripts/bench-all.sh   also the Kafka linger experiment
#                                          (docker compose -f deploy/docker-compose.yml up -d --wait kafka)
#
# STAGES="3 5" runs only those stages. Results go to bench/results/phase8/ and the summary tables are printed at the end.
# Run it on a quiet machine: the gateway experiments compete for CPU with anything else running,
# and the Docker VM alone can use several cores. The environment is recorded in env.txt.
#
# 1  engine index: the commit before phase 8 (BASELINE) against this tree, in a temporary worktree
# 2  micro-experiments (internal/experiments): price-level structures, order queues, ring vs channel,
#    cursor padding, hot-path costs, protobuf encoding
# 3  gateway: GC settings, thread pinning, GOMAXPROCS, channels instead of rings, baseline. With KAFKA set
#    the event log is in the path (the numbers in BENCHMARKS.md were taken that way)
# 4  Kafka producer linger against publish lag (needs KAFKA)
# 5  a CPU and an allocation profile of the gateway under load
# 6  rings against channels again, interleaved (ring, channel, ring, ...) so slow drift of the machine hits both
# 7  host jitter: spinning goroutines that do nothing else, to see what the machine alone does to a tail
set -euo pipefail
cd "$(dirname "$0")/.."

BASELINE="${BASELINE:-08060db}"      # last commit before phase 8 (the map-based id index)
if [ -n "${QUICK:-}" ]; then COUNT=2; RUNS=1; DURATION=10s; else COUNT="${COUNT:-5}"; RUNS="${RUNS:-3}"; DURATION="${DURATION:-30s}"; fi
RATE="${RATE:-10000}"
OUT=bench/results/phase8
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUT"
want() { [[ " ${STAGES:-1 2 3 4 5 6 7} " == *" $1 "* ]]; }
run() { # tag VAR=value... : one load test with this tag; GW_EXTRA = gateway flags, GW_ENV = gateway environment
  local tag="$1"; shift
  env RATE="$RATE" DURATION="$DURATION" RUNS="$RUNS" "$@" TAG="$tag" ./scripts/loadtest.sh >/dev/null
}

{ date -u; uptime; sysctl -n machdep.cpu.brand_string 2>/dev/null || true; go version; echo "commit $(git rev-parse --short HEAD)"; echo "baseline $BASELINE"; echo "GOGC=${GOGC:-} GOMEMLIMIT=${GOMEMLIMIT:-}"; } >"$OUT/env-$STAMP.txt"

if want 1; then
echo "== 1. engine id index: $BASELINE (before) against this tree (after)"
wt="$(mktemp -d)"; trap 'git worktree remove --force "$wt" 2>/dev/null || true' EXIT
git worktree add --detach "$wt" "$BASELINE" >/dev/null
( cd "$wt" && go test ./internal/engine -run '^$' -bench 'ApplyMixed|ApplyDeepBook' -benchmem -count "$COUNT" ) | tee "$OUT/engine-before-$STAMP.txt" | grep -E '^Benchmark' || true
go test ./internal/engine -run '^$' -bench 'ApplyMixed|ApplyDeepBook' -benchmem -count "$COUNT" | tee "$OUT/engine-after-$STAMP.txt" | grep -E '^Benchmark' || true

fi
if want 2; then
echo "== 2. micro-experiments"
go test ./internal/experiments -run '^$' -bench . -benchmem -benchtime 1s -count "$COUNT" | tee "$OUT/experiments-$STAMP.txt" | grep -E '^Benchmark' >/dev/null

fi
if want 3; then
echo "== 3. gateway variants ($RUNS runs of $DURATION at $RATE orders/s each; event log: ${KAFKA:-none})"
run p8-baseline        GW_EXTRA=""
run p8-gogc400         GW_ENV="GOGC=400"
run p8-gc-off-limit    GW_ENV="GOGC=off GOMEMLIMIT=2GiB"
run p8-lock-thread     GW_EXTRA="-lock-engine-thread"
run p8-gomaxprocs4     GW_ENV="GOMAXPROCS=4"
run p8-channels        GW_EXTRA="-queue-channels"
fi

if want 4 && [ -n "${KAFKA:-}" ]; then
  echo "== 4. Kafka producer linger against publish lag"
  for l in 0 1ms 5ms 20ms; do
    run "p8-linger-$l" KAFKA="$KAFKA" GW_EXTRA="-kafka-linger $l"
  done
fi

if want 5; then
echo "== 5. profiles"
run p8-profile PPROF=1
fi

if want 6; then
echo "== 6. rings against channels, interleaved"
for i in $(seq 1 "${AB_PAIRS:-5}"); do
  RUNS=1 run p8-ab-ring GW_EXTRA=""
  RUNS=1 run p8-ab-chan GW_EXTRA="-queue-channels"
done
fi

if want 7; then
echo "== 7. host jitter (spinning goroutines, nothing else)"
for w in 4 10; do
  MX_JITTER_SECONDS="${JITTER_SECONDS:-20}" MX_JITTER_WORKERS="$w" go test ./internal/experiments -run HostJitter -count=1 -v | grep "host jitter" | tee -a "$OUT/host-jitter-$STAMP.txt"
done
fi

echo
echo "== summary (also in $OUT)"
tags=(p8-baseline p8-gogc400 p8-gc-off-limit p8-lock-thread p8-gomaxprocs4 p8-channels p8-ab-ring p8-ab-chan)
[ -n "${KAFKA:-}" ] && tags+=(p8-linger-0 p8-linger-1ms p8-linger-5ms p8-linger-20ms)
scripts/bench-summary.py bench/results "${tags[@]}" | tee "$OUT/gateway-summary-$STAMP.md"
echo "profiles: go tool pprof -top bench/results/gateway-cpu-p8-profile-*.pprof"
