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

## Pre-trade risk (Redis)
```bash
docker compose -f deploy/docker-compose.yml up -d --wait redis
go run ./cmd/risktool fund  -redis localhost:16379 -from 1 -to 16 -base 1000000 -quote 1000000000   # seed balances
go run ./cmd/gateway -redis localhost:16379 -kafka localhost:9092 ...                                  # enable risk
go run ./cmd/risktool audit -redis localhost:16379 [-expect-base N -expect-quote N]                    # scan all accounts
REDIS=localhost:16379 scripts/loadtest.sh                                                               # load test with risk, audits each run
```
`audit` exits non-zero on any negative balance, and, when `-expect-*` is given, unless available plus held equals the funded total (settlement conserves funds, so it must). It is only meaningful while no orders are in flight and after the gateway has been stopped with SIGTERM (which drains the settlement queue).

Useful flags: `-risk-max-qty`, `-risk-max-price` (also the reserve price of a market buy), `-risk-max-open`, `-risk-rate` and `-risk-burst` (per account token bucket), `-risk-timeout` (Redis call bound; orders are refused with `UNAVAILABLE` past it).

Metrics: `mx_risk_reserve_seconds{result}`, `mx_risk_rejects_total{reason}`, `mx_risk_settle_queue`, `mx_risk_settle_lag_seconds`, `mx_risk_settle_errors_total`, and two to alert on: `mx_risk_settle_dropped_total` (an op was lost, balances need reconciliation) and `mx_risk_settle_anomalies_total` (an op found no reservation, or a fill larger than its reservation: Redis and the engine disagree).

Reset before running against a new event log: the settlement dedup remembers the last engine sequence per account and symbol, which restarts at 1 for a new topic. Delete the keys `{<account>}:*` first (`loadtest.sh` does).

The hermetic tests (`go test ./internal/risk ./internal/gateway`, not under `-short`) run against a Redis started by Testcontainers, including stopping and restarting it.

## Ledger and reconciliation (PostgreSQL)
```bash
docker compose -f deploy/docker-compose.yml up -d --wait kafka postgres
go run ./cmd/ledger deposit -symbol BTC-USD -account 1 -to 16 -base 1000000 -quote 1000000000   # fund the ledger like Redis (idempotent with -ref)
go run ./cmd/ledger run -kafka localhost:9092 -symbol BTC-USD                                   # settle trades; metrics on :8082
go run ./cmd/ledger reconcile -kafka localhost:9092 -symbol BTC-USD -redis localhost:16379 -wait 60s
KAFKA=localhost:9092 REDIS=localhost:16379 PG=1 scripts/loadtest.sh                              # load + ledger + reconcile per run
```
`-pg` is the connection string (default is the compose database). The fee flags `-taker-bps` and `-maker-bps` must be the same for `run` and `reconcile`. Migrations run when the binary starts.

`reconcile` prints a JSON report and exits 1 on any discrepancy. Kinds: `missing_settlement` (a logged trade has no settlement: look in the dead-letter topic), `orphan_settlement`, `balance_vs_log`, `balance_vs_entries`, `unbalanced_event`, `empty_settlement`, `asset_not_zero`, `negative_balance`, `balance_vs_redis`, `redis_negative`, `log_gap`, `log_invalid_trade`. Run it when the system is quiet and the gateway was stopped with SIGTERM (drains the Redis settlement queue); `-wait` covers the ledger catching up. It re-reads the whole log each attempt.

Replay: to rebuild or verify, start `ledger run` with a **new** `-group`; the whole log is read again and every already-settled trade is skipped (`mx_ledger_trades_duplicate_total`). To rebuild from nothing, truncate `entries, settlements, accounts`, redo the deposits, and run with a new group.

Dead letters: `mx.events.<symbol>.dlq` holds the original record bytes with headers `reason`, `source-topic`, `source-partition`, `source-offset`. Inspect with any Kafka console consumer printing headers. A dead letter is never retried automatically.

Metrics (ledger, `:8082/metrics`): `mx_ledger_settle_lag_seconds`, `mx_ledger_trades_settled_total`, `mx_ledger_trades_duplicate_total`, `mx_ledger_records_total`, `mx_ledger_retries_total` (database or offset commit trouble, alert if it keeps growing), `mx_ledger_dead_letter_total{reason}` (alert on any), `mx_ledger_last_settle_timestamp_seconds`.

The tests (`go test ./internal/ledger`, not under `-short`) start PostgreSQL, Kafka and Redis with Testcontainers, including stopping PostgreSQL mid-load.
