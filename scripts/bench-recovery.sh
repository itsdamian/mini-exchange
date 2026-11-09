#!/usr/bin/env bash
# Recovery-time benchmark: publish about 1M events to a fresh Kafka topic, then
# time a clean replay of the whole log several times (Kafka fetch + protobuf
# decode + state rebuild), which is the worst case for recovery.
# Target (requirements 5.1): replay of 1M events in under 10 seconds.
#
# Usage: scripts/bench-recovery.sh      (EVENTS, RUNS env vars)
source "$(dirname "${BASH_SOURCE[0]}")/chaos/lib.sh"
EVENTS="${EVENTS:-1000000}"
RUNS="${RUNS:-5}"
SYMBOL="REPLAY-$STAMP"
REPORT="$OUT/recovery-replay-$STAMP-$(git rev-parse --short HEAD 2>/dev/null || echo nogit).txt"

{
build; kafka_up
echo "hardware: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m); $(go version); commit $(git rev-parse --short HEAD) $(git status --porcelain --untracked-files=no | grep -q . && echo '(dirty)')"
say "publish ~$EVENTS events to $SYMBOL"
./bin/logtool gen -kafka "$KAFKA_ADDR" -symbol "$SYMBOL" -events "$EVENTS"
say "clean replay of the whole log, $RUNS runs"
for i in $(seq 1 "$RUNS"); do ./bin/logtool check -kafka "$KAFKA_ADDR" -symbol "$SYMBOL"; done
} 2>&1 | tee "$REPORT"
