# mini-exchange

A low-latency **matching engine, order gateway and double-entry ledger** in Go, built to practise latency engineering and payment-style correctness (idempotency, exactly-once effect, reconciliation).

> **Scope statement.** This is a personal simulation project. There is no real money, no connection to any real exchange for order entry, and no regulatory or compliance functionality. It makes no claim of production scale. Every performance number in this repo comes from a reproducible benchmark and is reported with its environment; numbers in the spec are targets, not results.

The specification is [requirements.md](requirements.md). Significant decisions are recorded in [docs/adr](docs/adr). Measured results are in [BENCHMARKS.md](BENCHMARKS.md).

## Status

| Phase | Scope | State |
|---|---|---|
| 0 | Scaffold: module, CI, docker compose (Kafka, Redis, PostgreSQL, Prometheus) | done |
| 1 | Engine core: order book, matching rules, reference implementation, property and differential tests, benchmarks | done |
| 2 | gRPC gateway, idempotency, ring buffer into the engine, open-loop load generator | done |
| 3 | Kafka event log, snapshots, replay and recovery | done |
| 4 | Pre-trade risk in Redis (Lua) | done |
| 5 | Double-entry ledger in PostgreSQL, dead-letter topic, reconciliation | done |
| 6 | Market data over WebSocket, synthetic order flow | done |
| 7 | Kubernetes (k3d, Argo CD, Strimzi, CloudNativePG), dashboards | next |
| 8 | Optimization report, optional standby failover | planned |
| 9 | Minimal UI (stretch) | planned |

The engine, a gRPC gateway, the Kafka event log, Redis pre-trade risk the PostgreSQL ledger, a WebSocket market data feed and a synthetic order bot exist today (the ledger and the feed are fed from Kafka, not from the order path). The diagram below is the **target** architecture.

## Target architecture

```
Client -> gateway (gRPC) -> idempotency check -> risk (Redis Lua, reserve funds)
       -> engine input queue (preallocated ring buffer)
       -> matcher (single goroutine, no locks, no I/O)
       -> output ring buffer -> eventlog (async batch) -> Kafka
                                                        -> ledger -> PostgreSQL
                                                        -> marketdata -> WebSocket
       -> execution report back to client (from output buffer, not from Kafka)
```

Key decisions (details in the ADRs):

- **Event sourcing**: the Kafka event stream is the system of record; engine state is a snapshot plus replay.
- **Determinism**: the same input sequence always produces byte-identical output events. No wall clock and no map iteration inside the matcher; timestamps come from the sequencer.
- **Integer money**: prices are integer ticks and quantities are integer units. No floating point in the engine or ledger.
- **Hot path discipline**: from engine input to output there is no network or disk I/O, no mutex, and (aimed for) no steady-state heap allocation.

## What works today

### Matching engine (phase 1)

In `internal/engine`:

- Order types: LIMIT (GTC, IOC) and MARKET; operations: place and cancel.
- Price-time priority, partial fills, execution at the resting order's price.
- Self-trade prevention by cancelling the newest order.
- Typed rejects (invalid price, invalid quantity, unknown order, invalid order, duplicate order id).
- Order storage in a preallocated slab with intrusive FIFO lists, price levels in sorted slices, integer-keyed index ([ADR 0002](docs/adr/0002-order-book-structure.md)).

How it is verified:

- Unit tests per rule.
- Property tests (rapid): the book is never crossed, quantity is conserved per order, price-time priority holds, no self-trades, same input gives the same output hash.
- A differential test of 1,000,000 random operations against a deliberately naive reference implementation (`internal/refbook`).

Measured on one Apple M5 laptop (synthetic workload, in-process, no network; full method and caveats in [BENCHMARKS.md](BENCHMARKS.md)):

| | Result |
|---|---|
| Throughput, mixed workload | about 16M orders/s (`go test -bench`) |
| Per-order p99 latency | about 125 to 167 ns, timer-quantised |
| Allocations | 0 per order in the mixed scenario; a few hundred per 5M orders in the deep-book scenario (known gap, tracked in BENCHMARKS.md) |

These are service-time figures for the matcher alone, not end-to-end latencies.

### Order gateway (phase 2)

A gRPC API (`proto/exchange/v1/gateway.proto`, managed with `buf`) in front of the engine:

- `PlaceOrder`, `CancelOrder`, and `StreamExecutionReports` (per-account fills and status, delivered from the engine output buffer).
- `client_order_id` is mandatory and is the idempotency key (scoped to the account). A retry returns the original result and never creates a second order, even when retries arrive concurrently: an integration test fires 100 at once and observes exactly one order ([ADR 0006](docs/adr/0006-idempotency.md)).
- Handlers feed the engine goroutine through a bounded lock-free MPSC ring; results and reports come back through an SPSC output ring and a dispatcher, so the matcher path has no locks or channels ([ADR 0005](docs/adr/0005-gateway-engine-pipeline.md)).
- A full queue is answered with `RESOURCE_EXHAUSTED` (explicit backpressure), and a slow report subscriber is disconnected rather than silently losing reports.
- Prometheus histograms for gateway request latency, engine queue wait and match time.
- `cmd/loadgen` is an open-loop generator (fixed arrival rate, latency measured from the intended send time, HdrHistogram output).

Measured with the gateway and load generator on the same laptop over loopback at 10,000 orders/s (3 runs of 30 s): p50 about 80 µs and p99 about 1.7 to 2.8 ms, within the targets. The extreme tail (p99.9 and max) is noisy and not yet explained; see [BENCHMARKS.md](BENCHMARKS.md) for method, per-run results and caveats.

### Event log, snapshots and recovery (phase 3)

With `-kafka` the gateway persists every engine event to Kafka and can rebuild its state after a crash:

- **The log is the system of record** ([ADR 0007](docs/adr/0007-event-log-on-kafka.md)): one topic per symbol, one partition, one record per engine command (so a command is atomic in the log), schema-versioned Protobuf, infinite retention.
- **The producer is asynchronous and never blocks the matcher.** If Kafka is slow or down, events queue in a bounded buffer; past a watermark the gateway refuses new orders with `RESOURCE_EXHAUSTED`. Nothing is dropped silently.
- **Snapshots plus replay** ([ADR 0008](docs/adr/0008-snapshots-and-recovery.md)): a follower applies only Kafka-acknowledged events and snapshots that copy, so a snapshot can never be ahead of the log. Recovery loads the newest valid snapshot and replays the rest, and refuses to start on a gap, a duplicate, a snapshot ahead of the log, or an event that does not apply.
- **Verified**: replay equals the live book after every command (property test); against a real Kafka broker (Testcontainers) the log survives Kafka being stopped or killed with no event lost or duplicated; and two chaos scripts `kill -9` the engine and Kafka under load. After the engine kill, snapshot recovery gave exactly the same book hash as a clean replay of the whole log (`logtool compare`).
- **Measured** (one laptop, local single-node Kafka): clean replay of 1,000,000 events from Kafka takes about 0.5 s; recovery after the engine kill took 35 ms. See [BENCHMARKS.md](BENCHMARKS.md) for method and caveats, and [docs/runbook.md](docs/runbook.md) to run it.

Honest limits: client acknowledgements are sent before Kafka acknowledges, so a crash can lose the last few tens of milliseconds of events that clients were already told about (the window is measured and documented, not zero). Gateway-side state (the idempotency table, report subscriptions) is still in memory and is not recovered, so after a restart an order placed before the crash cannot be cancelled by its `client_order_id`.

### Pre-trade risk (phase 4)

With `-redis` the gateway checks every order against balances in Redis before the engine sees it ([ADR 0009](docs/adr/0009-pretrade-risk-in-redis.md)):

- **Atomic reservation in Lua.** A buy holds `qty * price` of the quote asset, a sell holds `qty` of the base asset (a market buy holds `qty * max price`, an upper bound on any execution price). In one script call it also applies the token-bucket rate limit and the max-open-orders limit. Max order size and price range are checked in Go first. Rejections are typed: `INSUFFICIENT_FUNDS`, `RATE_LIMITED`, `ORDER_TOO_LARGE`, `TOO_MANY_OPEN_ORDERS`.
- **Fills consume and cancels release, off the hot path.** The matcher never talks to Redis. The dispatcher turns engine events into settlement ops, and one worker applies them to Redis asynchronously, in event order, with retries. Each op carries its engine sequence number and Redis skips a sequence it already applied, so a retry after a timeout cannot double-apply.
- **Fails closed.** If Redis does not answer within the timeout the order is refused with `UNAVAILABLE`; if settlement falls behind, new orders get `RESOURCE_EXHAUSTED`.
- **Verified against a real Redis** (Testcontainers): 400 concurrent reservations against a balance that fits exactly 100 never overdraw and never leave available negative; Lua unit tests; a random end-to-end flow through the gateway conserves funds exactly; Redis stopped and restarted underneath a running gateway.
- **Measured** (one laptop, Redis on the same host, 10k orders/s): the check adds about 75 to 90 µs at p50 with a native Redis (p50 78 to 90 µs without risk, 152 to 175 µs with it, with or without the Kafka log). The tail is **not** under control: p99 met the 5 ms target in only some runs, and with Redis through Docker Desktop or with all 10 cores in use, orders were sometimes refused after the 100 ms Redis timeout (up to 7.7% in the worst run). Redis's own slowlog shows stalls inside Redis, which I attribute, as a hypothesis from one experiment, to CPU oversubscription on this laptop. Funds were conserved exactly in all 15 audited runs. Details and every run in [BENCHMARKS.md](BENCHMARKS.md).

Honest limits: Redis and the engine can disagree after a gateway crash (queued settlement ops are lost, and the client order id needed to rebuild them is not in the event log, the same gap as the idempotency table); orders reserved but never accepted keep their funds held with no expiry; there is no deposit flow (balances are seeded with `cmd/risktool fund`); the spec's Redis partition chaos test exists as an integration test (Redis stopped and restarted), not as a script.

### Ledger (phase 5)

`cmd/ledger` consumes the event log and settles every trade into PostgreSQL as balanced double-entry postings ([ADR 0010](docs/adr/0010-double-entry-ledger.md)):

- **Entries**: buyer and seller quote (cash), seller and buyer base (asset), and an optional fee to a fee account; a posting always sums to zero per asset, deposits come from an `external` account, and the schema makes a negative customer balance impossible to commit.
- **Idempotent**: the key is `<symbol>:<engine seq>`; replaying the topic from the start, any number of times, leaves the same balances.
- **Poison isolation**: a record that can never be applied (undecodable, unknown schema, wrong symbol, malformed trade, a trade that would make a balance negative) goes to `mx.events.<symbol>.dlq` with the reason and the consumer moves on. A database outage is retried, never skipped.
- **Reconciliation**: `ledger reconcile` compares the ledger with a recomputation from the event log and with the Redis balances, and checks the invariants; it prints a JSON report and exits non-zero on any discrepancy.
- **Measured** (one laptop, PostgreSQL through Docker Desktop, 10k orders/s, about 99,000 trades per run): all 6 reconciliations found zero discrepancies. Ledger lag is **not** small: 31 s mean with one transaction per record, about 4 to 5 s after grouping records per transaction, and I have not profiled further. Trading does not wait for the ledger. Details in [BENCHMARKS.md](BENCHMARKS.md).

Honest limits: with a non-zero fee the ledger can refuse a buyer who spent everything, because risk does not reserve fees (fees default to 0); deposits are not in the event log, so the recomputation trusts the ledger's deposit entries and Redis is seeded separately; the reconciliation re-reads the whole log and is not incremental; one symbol per consumer.

### Market data and bot (phase 6)

`cmd/marketdata` rebuilds the book from the Kafka log and serves depth and recent trades over WebSocket at `/ws` ([ADR 0011](docs/adr/0011-market-data-and-bot.md)):

- **Full state per frame**: top-N asks and bids, the last trades and the engine sequence; a client that missed frames needs nothing else.
- **Conflation**: at most one frame per client per 100 ms, built and encoded once and shared by all clients. A client that does not accept a frame within the write timeout is disconnected, never buffered; connections beyond a limit get 503.
- **Separate process**: it only reads Kafka, so a client cannot slow the matcher by construction.
- **Measured** (one laptop, everything on the same machine, 10k orders/s, 1,000 clients, 3 runs): no connection failures or drops, a median of 100 ms between frames, memory flat at about 185 MB. It is **not** free for the gateway: median latency was 15 µs higher (86 to 91 vs 103 to 105 µs) in all three runs and one run's p99 was 8.4 ms, which I attribute, as a hypothesis, to CPU contention on one machine. Details in [BENCHMARKS.md](BENCHMARKS.md).

`cmd/bot` sends synthetic flow to the gateway: a market maker that re-quotes three levels per side around a reference price every step, plus random taker orders. The reference price comes from a recorded file (default: `data/sample-prices.csv`, which is **synthetic**, a seeded random walk, not a market recording) or, with `-feed coinbase`, from the live public Coinbase ticker. With the file and a fixed `-seed`, two runs send the same orders and the engine emits the same events (tested through the real gateway, timestamps excluded). Tests never reach the internet: a guard fails any dial to a non-loopback host.

Honest limits: the bot is not a realistic market simulation (fixed spread, random takers, no inventory or risk model); marketdata replays the whole log at start; one symbol per process; the full-state frame trades bandwidth for simplicity.

## Quick start

Requirements: Go (version in `go.mod`), Docker with Compose, and `golangci-lint` for `make lint`.

```bash
make test          # unit and property tests with -race, random order (short differential run, no Docker)
make test-full     # also the full 1M-operation differential test and the Kafka and Redis integration tests (needs Docker)
make lint          # go vet + golangci-lint
make bench         # Go micro-benchmarks
make bench-report  # full reproducible engine report (writes bench/results/)
make loadtest      # end-to-end open-loop gRPC load test, 3 runs at 10k orders/s
make bench-recovery  # replay of a 1M-event log from Kafka (needs Docker)
make chaos         # kill -9 the engine and Kafka under load and verify (needs Docker)
REDIS=localhost:16379 make loadtest   # the same load test with pre-trade risk (funds 16 accounts, audits conservation)
KAFKA=localhost:9092 REDIS=localhost:16379 PG=1 make loadtest   # plus the ledger consumer, reconciled after each run
WS=1000 KAFKA=localhost:9092 make loadtest   # plus marketdata and 1,000 WebSocket clients
make proto         # regenerate gen/ from proto/ (needs buf and the protoc-gen-go plugins)

make up            # docker compose: kafka, redis, postgres, prometheus, gateway
make smoke         # checks the gateway /healthz and /metrics
make down          # stop the stack and remove its volumes
```

Host ports are chosen to avoid common clashes and can be overridden with environment variables: `KAFKA_PORT` (9092), `REDIS_PORT` (16379), `POSTGRES_PORT` (15432), `GATEWAY_PORT` (18080, HTTP), `GATEWAY_GRPC_PORT` (50051), `PROMETHEUS_PORT` (9090).
The local PostgreSQL password in `deploy/docker-compose.yml` is a development-only value.

## Repository layout

```
cmd/gateway          order gateway: gRPC API, /healthz, /readyz, /metrics
cmd/loadgen          open-loop gRPC load generator
cmd/logtool          event log checks: replay and continuity, snapshot-vs-clean comparison, generator
cmd/risktool         seed (fund) and audit the Redis balances
cmd/ledger           ledger service (run), deposit, reconcile
cmd/marketdata       WebSocket market data from the event log
cmd/bot              synthetic order flow (market maker and random takers)
cmd/wsload           WebSocket client load for the market data test
proto/               Protobuf API (buf managed); gen/ holds the generated Go code
internal/gateway     gRPC server, validation, idempotency
internal/pipeline    gateway-to-engine rings, engine goroutine, dispatcher, report hub
internal/ring        lock-free SPSC and MPSC bounded queues
internal/eventlog    Kafka publisher, snapshots, follower, recovery (kafkatest: Docker broker for tests)
internal/marketdata  depth and trade view from the log, WebSocket server with conflation
internal/bot         price sources, deterministic planner and runner
internal/nonet       test guard against external network access
internal/ledger      double-entry ledger: postings, PostgreSQL store (goose migrations), Kafka consumer with dead-letter topic, reconciliation (pgtest: Docker PostgreSQL for tests)
internal/risk        Redis pre-trade risk: Lua scripts, reservation, rate limit, async settlement (redistest: Docker Redis for tests)
internal/benchenv    environment capture for benchmark results
internal/engine      matching engine and order book
internal/refbook     naive reference implementation (differential tests, benchmarks)
internal/enginetest  deterministic random command generator
internal/httpserver  health and metrics HTTP surface
bench/               benchmark tools; results/ holds raw JSON and text output
deploy/              docker compose and Prometheus config
docs/adr             architecture decision records
docs/plan            per-phase plans and task lists
scripts/             bench.sh, loadtest.sh, bench-recovery.sh, chaos/ (engine and Kafka kill tests)
docs/runbook.md      how to run replay checks and chaos tests
```

## Relationship to cube_test

This project reuses the *style* of the author's earlier `cube_test` project (Java, Spring Boot, Kafka, k3d, Argo CD, Prometheus/Grafana) for local and cluster deployment. It is a separate repository and shares no code with it.
