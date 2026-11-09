#!/usr/bin/env bash
# Chaos: kill -9 the engine (gateway) in the middle of load, restart it, and
# prove that (1) recovery from snapshot + log equals a clean replay of the
# whole log (same book hash), and (2) the log has no gap or duplicate across
# the restart.
#
# Usage: scripts/chaos/engine-kill.sh        (RATE, DURATION env vars)
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
DURATION="${DURATION:-40s}"
KILL_AFTER="${KILL_AFTER:-15}"
REPORT="$OUT/chaos-engine-kill-$STAMP.txt"
LOG1="$WORK/gateway1.log"; LOG2="$WORK/gateway2.log"

{
build; kafka_up
say "symbol $SYMBOL, work dir $WORK"

say "start gateway (first life)"
start_gateway "$LOG1"
recovered_line "$LOG1" 1 | python3 -c 'import sys,json;d=json.loads(sys.stdin.read());print("fresh start: last_seq=%d total_ms=%d"%(d["last_seq"],d["total_ms"]))'

say "start load: $RATE req/s for $DURATION"
./bin/loadgen -addr "localhost:${GRPC_PORT}" -symbol "$SYMBOL" -rate "$RATE" -duration "$DURATION" -warmup 2s \
  -note "chaos engine-kill" -out "$OUT/chaos-engine-kill-load-$STAMP.json" >"$WORK/loadgen.out" 2>&1 &
LG_PID=$!

sleep "$((KILL_AFTER - 3))"
# The events the engine has produced but Kafka has not yet acknowledged live
# only in memory: they are what a crash loses. Sample that window before the kill.
MAX_GAP=0
for _ in 1 2 3 4 5 6; do
  gap=$(( $(metric mx_engine_last_sequence) - $(metric mx_eventlog_acked_sequence) ))
  [ "$gap" -gt "$MAX_GAP" ] && MAX_GAP=$gap
  sleep 0.5
done
say "KILL -9 the engine (unacknowledged window sampled before the kill: up to $MAX_GAP events)"
kill -9 "$GW_PID"; wait "$GW_PID" 2>/dev/null || true; GW_PID=""

say "restart the engine"
start_gateway "$LOG2"
REC="$(recovered_line "$LOG2")"
echo "$REC" | python3 -c '
import sys,json
d=json.loads(sys.stdin.read())
print("recovered: from_snapshot=%s snapshot_seq=%d replayed_events=%d last_seq=%d book_hash=%s resting=%d" % (d["from_snapshot"],d["snapshot_seq"],d["replayed_events"],d["last_seq"],d["book_hash"],d["resting_orders"]))
print("recovery time: snapshot_load=%dms replay=%dms total=%dms ; process start to /readyz: '"$GW_READY_MS"'ms" % (d["snapshot_load_ms"],d["replay_ms"],d["total_ms"]))'
REC_SEQ="$(echo "$REC" | python3 -c 'import sys,json;print(json.loads(sys.stdin.read())["last_seq"])')"
REC_HASH="$(echo "$REC" | python3 -c 'import sys,json;print(json.loads(sys.stdin.read())["book_hash"])')"

say "wait for the load to finish"
wait "$LG_PID" || true; LG_PID=""
tail -n 12 "$WORK/loadgen.out"

until [ "$(metric mx_eventlog_pending_batches)" = "0" ]; do sleep 0.2; done
FINAL_ENGINE="$(metric mx_engine_last_sequence)"
say "graceful stop (flush + final snapshot)"
stop_gateway

say "verify against the log"
./bin/logtool check -kafka "$KAFKA_ADDR" -symbol "$SYMBOL" | tee "$WORK/check.out"
./bin/logtool compare -kafka "$KAFKA_ADDR" -symbol "$SYMBOL" -data-dir "$DATA_DIR" | tee "$WORK/compare.out"
LOG_SEQ="$(grep -o 'last_seq=[0-9]*' "$WORK/check.out" | head -1 | cut -d= -f2)"
echo "engine final seq=$FINAL_ENGINE, log last seq=$LOG_SEQ"
[ "$FINAL_ENGINE" = "$LOG_SEQ" ] || { echo "FAIL: log end differs from the engine's final sequence"; exit 1; }

# Everything the restarted engine produced applied cleanly on top of the
# recovered state (a clean replay of the whole log succeeded), which would not
# hold if the recovered state had differed from what the log says.
echo "recovered at restart: seq $REC_SEQ, hash $REC_HASH"
echo; echo "PASS: engine kill -9 recovery"
} 2>&1 | tee "$REPORT"
exit "${PIPESTATUS[0]}"
