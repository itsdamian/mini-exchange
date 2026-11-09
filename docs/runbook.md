# Runbook

Commands assume the repository root. Docker with Compose is needed for everything that touches Kafka.

## Run the gateway with the event log
```bash
docker compose -f deploy/docker-compose.yml up -d --wait kafka
go run ./cmd/gateway -kafka localhost:9092 -data-dir ./data -symbol BTC-USD
```
Without `-kafka` the gateway runs with no event log and loses all state on exit (it logs a warning).
On start it prints one `recovered` line (snapshot used, events replayed, last sequence, book hash, timings).

Useful flags: `-snapshot-every N`, `-log-high-watermark N` (unacknowledged batches before orders are refused), `-kafka-linger D`.

## Check the log (replay and sequence continuity)
```bash
go run ./cmd/logtool check   -kafka localhost:9092 -symbol BTC-USD
go run ./cmd/logtool compare -kafka localhost:9092 -symbol BTC-USD -data-dir ./data
```
`check` replays the whole log from offset 0. It fails on a gap, a duplicate, an unknown schema version or an event that does not apply, and prints the last sequence and the book hash. `compare` additionally recovers from the newest snapshot and exits non-zero unless both give the same hash and sequence.

## Chaos tests (real processes, real Kafka)
```bash
make chaos                         # both scripts below
scripts/chaos/engine-kill.sh       # kill -9 the engine mid-load, restart, verify
scripts/chaos/kafka-kill.sh        # kill -9 Kafka mid-load, keep loading, restart Kafka, verify
```
Each starts a fresh symbol, runs the gateway natively against the compose Kafka (fixed port 9092, so the broker can be killed and started), drives load with `cmd/loadgen`, injects the failure, and asserts:
- engine kill: snapshot + log recovery equals a clean replay (same book hash); the engine's final sequence equals the log's last sequence; the log replays cleanly across the restart.
- Kafka kill: the gateway sheds load with `RESOURCE_EXHAUSTED` while the backlog is full; afterwards every event is in the log exactly once (events in log == last sequence == engine's last sequence).
Output is saved under `bench/results/chaos-*.txt`.

The same two failures run hermetically (Testcontainers, a Kafka started and stopped or killed by the test) in `go test ./internal/eventlog` (not under `-short`).

## Recovery-time benchmark
```bash
make bench-recovery        # publishes ~1M events, then times 5 clean replays
```

## Reconciliation
Arrives with the ledger (phase 5).
