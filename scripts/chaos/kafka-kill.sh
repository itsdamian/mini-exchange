#!/usr/bin/env bash
# Chaos: kill -9 Kafka in the middle of load, keep the load running while it
# is down, bring it back, and prove that no event was lost or duplicated: the
# log replays cleanly (contiguous sequence) and ends exactly where the engine
# ended. While Kafka is down the gateway must shed load with explicit errors
# once its bounded backlog is full.
#
# Usage: scripts/chaos/kafka-kill.sh        (RATE, DURATION, DOWN_FOR env vars)
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
DURATION="${DURATION:-45s}"
KILL_AFTER="${KILL_AFTER:-12}"
DOWN_FOR="${DOWN_FOR:-12}"
REPORT="$OUT/chaos-kafka-kill-$STAMP.txt"
LOG1="$WORK/gateway.log"

{
build; kafka_up
say "symbol $SYMBOL, work dir $WORK"
start_gateway "$LOG1"

say "start load: $RATE req/s for $DURATION"
./bin/loadgen -addr "localhost:${GRPC_PORT}" -symbol "$SYMBOL" -rate "$RATE" -duration "$DURATION" -warmup 2s \
  -note "chaos kafka-kill" -out "$OUT/chaos-kafka-kill-load-$STAMP.json" >"$WORK/loadgen.out" 2>&1 &
LG_PID=$!

sleep "$KILL_AFTER"
say "KILL -9 Kafka (acked seq $(metric mx_eventlog_acked_sequence), engine seq $(metric mx_engine_last_sequence))"
docker kill "$KAFKA_CID" >/dev/null
sleep "$DOWN_FOR"
echo "while Kafka is down: pending batches=$(metric mx_eventlog_pending_batches), engine seq=$(metric mx_engine_last_sequence), acked seq=$(metric mx_eventlog_acked_sequence), refused=$(metric mx_pipeline_overloaded_total)"
say "start Kafka again"
docker start "$KAFKA_CID" >/dev/null
for _ in $(seq 1 120); do (echo > /dev/tcp/localhost/9092) 2>/dev/null && break; sleep 0.5; done

say "wait for the load to finish"
wait "$LG_PID" || true; LG_PID=""
tail -n 14 "$WORK/loadgen.out"

until [ "$(metric mx_eventlog_pending_batches)" = "0" ]; do sleep 0.2; done
ENGINE_SEQ="$(metric mx_engine_last_sequence)"; ACKED_SEQ="$(metric mx_eventlog_acked_sequence)"
echo "engine last seq=$ENGINE_SEQ, Kafka-acked seq=$ACKED_SEQ, publish errors=$(metric mx_eventlog_publish_errors_total)"
stop_gateway

say "verify against the log"
./bin/logtool check -kafka "$KAFKA_ADDR" -symbol "$SYMBOL" | tee "$WORK/check.out"
LOG_SEQ="$(grep -o 'last_seq=[0-9]*' "$WORK/check.out" | head -1 | cut -d= -f2)"
EV="$(grep -o 'events=[0-9]*' "$WORK/check.out" | head -1 | cut -d= -f2)"
echo "log last seq=$LOG_SEQ, events in log=$EV (must equal the last seq: nothing lost, nothing duplicated)"
[ "$ENGINE_SEQ" = "$LOG_SEQ" ] && [ "$EV" = "$LOG_SEQ" ] || { echo "FAIL: events lost or duplicated"; exit 1; }
echo; echo "PASS: Kafka kill -9, no event lost or duplicated"
} 2>&1 | tee "$REPORT"
exit "${PIPESTATUS[0]}"
