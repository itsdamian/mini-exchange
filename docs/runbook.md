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

## Kubernetes (k3d)
```bash
make k8s-up        # images, cluster "mini-exchange", Argo CD, then Argo CD deploys everything (20 to 30 minutes the first time)
make k8s-seed      # fund accounts 1..16 in Redis and the ledger (once per cluster)
make k8s-load      # RATE=2000 DURATION=120s make k8s-load: in-cluster load Job
make k8s-test      # 27 checks of the deployment
make k8s-validate  # offline: overlay builds, alert unit tests, metrics exist (what CI runs)
make k8s-down      # delete the cluster and its data
export KUBECONFIG=$PWD/.k8s/kubeconfig   # kubectl for this cluster; your own context is never changed
```
URLs: Grafana http://localhost:13000 (admin / mx-local-dev), Prometheus http://localhost:19090, Argo CD http://localhost:18800 (admin, password from `kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' | base64 -d`), gateway gRPC localhost:15051, market data `ws://localhost:18183/ws`.

`k8s-up` needs about 5 GiB free in the Docker VM and refuses below 4.5 GB (`FORCE=1` overrides). On a 16 GB Docker VM that means stopping other large clusters first (`k3d cluster stop <name>`, `k3d cluster start <name>` afterwards).

Changing anything under `deploy/`: run `scripts/k8s-publish.sh`; Argo CD reads the working tree from the in-cluster git server (not from GitHub) and syncs within about 30 seconds. If the `gitserver` pod restarts its repository is empty: publish again. Argo CD application state: `kubectl -n argocd get applications`.

Things to know:
- The gateway is one replica on purpose; scaling it is refused by the `mx-single-engine` admission policy. Replacing the pod recovers the engine from the snapshot volume and the log.
- Alerts have no Alertmanager: look at Prometheus http://localhost:19090/alerts or Grafana. The rules are in `deploy/k8s/base/observability/alerts.yaml`, their unit tests next to them.
- The seed is not idempotent for Redis (`risktool fund` adds each time); the ledger deposit is. Delete and recreate the cluster to start clean.
- Network policy: only listed connections work. A new component that needs the network needs a rule in `deploy/k8s/base/networkpolicy.yaml`.

## Market data and bot
```bash
docker compose -f deploy/docker-compose.yml up -d --wait kafka
go run ./cmd/gateway -kafka localhost:9092 ...                         # the engine whose log is followed
go run ./cmd/marketdata -kafka localhost:9092 -symbol BTC-USD          # WebSocket on :8083/ws, metrics on :8083/metrics
go run ./cmd/bot -addr localhost:50051 -seed 1 -loop                   # synthetic flow from data/sample-prices.csv
go run ./cmd/wsload -url ws://localhost:8083/ws -clients 1000 -duration 30s
WS=1000 KAFKA=localhost:9092 scripts/loadtest.sh                       # load test with marketdata and 1,000 clients
```
Any WebSocket client works: `websocat ws://localhost:8083/ws` prints one JSON snapshot per update (asks and bids as `[price, qty]`, `trades`, `seq`, `ts_ns`).

Marketdata flags: `-depth`, `-trades`, `-interval` (conflation), `-write-timeout` (a client that does not accept a frame in this time is disconnected), `-max-clients` (503 beyond), `-max-orders` (size of the rebuilt book). It replays the whole log on start, so a restart takes as long as a clean recovery. If an event does not apply to the book it exits with an error rather than serve a wrong view.

Metrics: `mx_md_clients`, `mx_md_clients_accepted_total`, `mx_md_clients_refused_total`, `mx_md_messages_sent_total`, `mx_md_slow_disconnects_total` (clients that could not keep up), `mx_md_frame_build_seconds`, `mx_md_frame_bytes`, `mx_md_publish_lag_seconds` (engine event to the frame containing it), `mx_md_events_applied_total`.

Bot: `-feed file` (default, deterministic: same file, same `-seed`, same orders) or `-feed coinbase` (live, optional). `-loop` replays the file forever; use a new `-prefix` when restarting against a running gateway so client order ids do not repeat (a repeat returns the cached result). With risk enabled, fund the maker (`-maker`, default 1) and takers (`-taker-from` to `-taker-to`, default 2 to 9) first. The price file in `data/` is synthetic, see `data/README.md`.

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
