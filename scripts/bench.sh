#!/usr/bin/env bash
# Reproduce the engine benchmarks in BENCHMARKS.md.
# Usage: scripts/bench.sh            (full: 5 runs x 5M orders per scenario)
#        ORDERS=1000000 RUNS=3 scripts/bench.sh   (quicker)
set -euo pipefail
cd "$(dirname "$0")/.."

ORDERS="${ORDERS:-5000000}"
RUNS="${RUNS:-5}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
COMMIT="$(git rev-parse --short HEAD 2>/dev/null || echo nogit)"
mkdir -p bench/results

echo "== go test -bench (ns/op, allocs/op) =="
go test -run '^$' -bench . -benchmem -benchtime 2s -count 3 ./internal/engine | tee "bench/results/gobench-${STAMP}-${COMMIT}.txt"

for scenario in mixed deep; do
  echo "== enginebench scenario=${scenario} =="
  go run ./bench/enginebench -scenario "${scenario}" -orders "${ORDERS}" -runs "${RUNS}" \
    -out "bench/results/engine-${scenario}-${STAMP}-${COMMIT}.json"
done
