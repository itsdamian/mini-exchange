# Mini Exchange: Low-Latency Matching Engine, Order Gateway and Ledger

Working repo name: `mini-exchange`

## 0. How to use this document (instructions for Claude Code)

- Treat this file as the source of truth. Work spec-driven: for each phase in section 9, write a short plan, break it into small tasks, implement one task at a time, run tests, then commit.
- Do not add features that are not listed. If something is ambiguous or a choice has real trade-offs, write a short ADR in `docs/adr/` and pick the simplest option that satisfies the acceptance criteria.
- Every performance claim must be backed by a reproducible benchmark committed to the repo. Never invent numbers. Numbers in section 5 are targets, not results.
- Keep the hot path (section 3.2) free of network I/O, disk I/O, locks, and unnecessary allocations.
- After each phase, run the full test suite and update `README.md` and `BENCHMARKS.md`.
- Use English for code, comments, commit messages, and docs.

## 1. Purpose and scope

### 1.1 Purpose
A personal portfolio project that demonstrates low-latency backend engineering in a finance setting: an in-memory matching engine, a gRPC order gateway, an event-sourced log on Kafka, pre-trade risk checks in Redis, and a double-entry ledger with idempotent settlement in PostgreSQL. The emphasis is on measured latency (p50/p99/p999), correctness under failure, and clean engineering practice.

### 1.2 Goals
- Show hands-on Go, gRPC/Protobuf, Kafka, Redis, PostgreSQL, and Kubernetes in one coherent system.
- Show payment-style correctness: idempotency, exactly-once effect on the ledger, reconciliation.
- Produce a benchmark report that explains what was measured, how, and which optimizations changed the numbers.

### 1.3 Non-goals
- No real money, no real exchange connectivity for order entry, no regulatory or compliance features.
- Single instrument family (spot pairs such as BTC-USD). No derivatives, no margin, no multi-asset portfolios.
- No claim of production scale. The README must label this a personal simulation project.

## 2. Reuse of existing work
The author already has a personal project `cube_test` (Java/Spring Boot, Kafka, PostgreSQL, React, k3d, Argo CD, Prometheus/Grafana) that ingests live Coinbase/Kraken trades. This project should:
- Reuse the same style of local environment: `docker compose up` for local, k3d + Argo CD for cluster deployment, kube-prometheus-stack for metrics.
- Optionally consume real trade prices from Coinbase WebSocket (public, no API key) to drive a synthetic market-maker bot, with a recorded-file fallback so tests and benchmarks are fully offline and deterministic.
- Be a separate repository. Do not modify `cube_test`.

## 3. Architecture

### 3.1 Components

| Component | Responsibility |
|---|---|
| `gateway` | gRPC API (PlaceOrder, CancelOrder, execution report stream), request validation, idempotency, hands orders to the engine |
| `risk` | Pre-trade checks using Redis (balance reservation, order limits, rate limit) |
| `engine` | Single-threaded deterministic matching per symbol, in-memory order book, emits events |
| `eventlog` | Publishes engine events to Kafka (batched, async), owns schemas and sequence numbers |
| `snapshot` | Periodic book snapshot plus replay-based recovery |
| `ledger` | Kafka consumer, double-entry bookkeeping in PostgreSQL, idempotent settlement, reconciliation |
| `marketdata` | Kafka consumer, builds depth and trade feed, serves WebSocket with conflation |
| `bot` | Synthetic order flow generator (market maker plus random takers), fed by recorded or live prices |
| `loadgen` | Open-loop load generator for benchmarks (fixed arrival rate, HdrHistogram output) |

### 3.2 Order flow and hot path

```
Client -> gateway (gRPC) -> idempotency check -> risk (Redis Lua, reserve funds)
       -> engine input queue (preallocated ring buffer)
       -> matcher (single goroutine, no locks, no I/O)
       -> output ring buffer -> eventlog (async batch) -> Kafka
                                                        -> ledger -> PostgreSQL
                                                        -> marketdata -> WebSocket
       -> execution report back to client (from output buffer, not from Kafka)
```

Hot path rule: from engine input queue to output ring buffer there is no network call, no disk access, no mutex, no channel with unbounded growth, and no heap allocation in steady state (verify with `-benchmem` and escape analysis).

### 3.3 Key design decisions (write ADRs for each)
- Event sourcing: the Kafka event stream is the system of record for engine state. State = snapshot + replay.
- Determinism: same input sequence must always produce byte-identical output events. No wall-clock reads or map iteration order inside the matcher; timestamps are assigned by the sequencer at input.
- Money and prices: integer ticks and integer quantity units only. No floating point anywhere in the engine or ledger.
- Partitioning: one engine instance owns one symbol; Kafka topics partitioned by symbol so ordering is preserved.

## 4. Functional requirements

### FR-1 Order book and matching
- Order types: LIMIT (GTC, IOC) and MARKET. Operations: place, cancel.
- Price-time priority, partial fills, self-trade prevention (cancel newest).
- Rejects with typed reasons (invalid price, invalid quantity, unknown order, insufficient funds, rate limited).
- Acceptance: unit tests cover each rule; property tests (section 7) pass; a naive reference implementation agrees with the optimized engine on 1M random operations.

### FR-2 Order gateway (gRPC + Protobuf)
- Protobuf definitions in `proto/`, managed with `buf` (lint and breaking-change check in CI).
- RPCs: `PlaceOrder`, `CancelOrder`, `StreamExecutionReports`.
- `client_order_id` is mandatory and is the idempotency key: a retried PlaceOrder with the same key returns the original result and never creates a second order.
- Acceptance: integration test retries the same request 100 times concurrently and observes exactly one order.

### FR-3 Pre-trade risk (Redis)
- Account balances held in Redis. Placing an order atomically reserves funds with a Lua script; cancel or fill releases or consumes the reservation.
- Limits: max order size, max open orders per account, token-bucket rate limit per account (Lua).
- Acceptance: concurrent reservation test never produces a negative available balance; Lua scripts have unit tests.

### FR-4 Event log (Kafka)
- Events: OrderAccepted, OrderRejected, OrderCanceled, TradeExecuted. Each has a monotonically increasing per-symbol sequence number and a schema version (Protobuf).
- Producer is asynchronous and batched, and never blocks the matcher. If Kafka is unavailable, events buffer to a bounded local queue; when full, the gateway sheds load with an explicit error (backpressure), it does not drop events silently.
- Acceptance: kill Kafka during a load test; no event is lost or duplicated after recovery (verify by sequence-number continuity check).

### FR-5 Snapshot and recovery
- Periodic snapshot of the order book plus last applied sequence number.
- On start, load the latest snapshot and replay events after it. Recovered state must equal the pre-crash state (compare book hash).
- Acceptance: chaos test kills the engine mid-load and restarts; final book hash equals the hash from a clean replay. Recovery time is recorded in `BENCHMARKS.md`.

### FR-6 Ledger and settlement (PostgreSQL)
- Double-entry bookkeeping: every trade produces balanced debit/credit entries across accounts (buyer cash, seller cash, buyer asset, seller asset, fee account).
- Consumer is idempotent: unique constraint on `event_id` with `ON CONFLICT DO NOTHING`; replaying the topic from the beginning yields the same balances.
- Poison-message isolation: a bad record goes to a dead-letter topic with the reason, and does not block the partition.
- Reconciliation job: compares ledger balances with Redis balances and with a recomputation from the event log; reports discrepancies.
- Invariants checked in tests and by the reconciliation job: sum of all entries is zero per asset; no negative customer balance; every trade has exactly one settlement.
- Acceptance: replay test (apply the full log twice) yields identical balances; reconciliation reports zero discrepancies after the chaos test.

### FR-7 Market data
- WebSocket feed with top-N depth and recent trades per symbol.
- Conflation: push the latest state at most every 100 ms per client; slow clients are disconnected rather than allowed to grow memory.
- Acceptance: load test with 1,000 simulated WebSocket clients shows stable memory and no impact on engine latency.

### FR-8 Synthetic flow and feed adapter
- `bot` generates a market-maker quoting around a reference price plus random taker orders.
- Reference price comes from either a recorded CSV/JSON file (default, deterministic) or the live Coinbase WebSocket (optional flag).
- Acceptance: with the recorded file and a fixed random seed, two runs produce identical event streams.

### FR-9 High availability (phase 8, may be stretch)
- A standby engine consumes the same event stream and keeps a warm book. On primary failure (health check or lease loss), the standby takes over.
- Acceptance: failover time measured and recorded; no duplicate or missing sequence numbers after takeover.

### FR-10 Minimal UI (stretch)
- Small React + TypeScript page showing depth, trades, and live latency percentiles. Optional; do not start before phase 8 is complete.

## 5. Performance targets and measurement

### 5.1 Targets (to be measured and reported honestly; adjust only with an ADR)
- Engine core, in-process benchmark (no network): p99 under 50 microseconds per order; at least 500k orders per second on a single symbol on the developer machine.
- Gateway end to end on localhost (gRPC request to execution report): p50 under 1 ms, p99 under 5 ms at 10k orders per second sustained.
- Recovery: replay of 1M events in under 10 seconds.
- Zero steady-state heap allocations in the matcher hot path.

### 5.2 Methodology (mandatory)
- Use an open-loop load generator with a fixed arrival rate to avoid coordinated omission; record latencies with HdrHistogram and export percentile distributions (p50, p90, p99, p99.9, max).
- Warm up before measuring; run each benchmark multiple times and report the spread.
- Record hardware, OS, Go version, GOMAXPROCS, GOGC/GOMEMLIMIT, and commit hash in every result.
- Use `pprof` (CPU, heap, allocs), `go test -bench -benchmem`, and `go tool trace` to find bottlenecks. Commit before/after numbers for each optimization.
- Expose Prometheus histograms for: gateway request latency, engine queue wait, match time, event publish lag, ledger settlement lag.

### 5.3 Optimization experiments to try and document (each as a before/after entry in `BENCHMARKS.md`)
1. Order book data structure: sorted slice of price levels vs B-tree vs skip list, benchmarked at different book depths.
2. Intrusive doubly linked list for orders within a price level; object pooling for orders.
3. Preallocated ring buffer (single-producer single-consumer) between gateway and engine vs a buffered channel.
4. Avoid interface boxing, `time.Now()`, `fmt`, and map lookups with string keys in the hot path; use integer IDs.
5. CPU cache effects: struct layout, padding to avoid false sharing between producer and consumer cursors.
6. GC tuning: `GOGC`, `GOMEMLIMIT`, and reducing pointer-heavy structures.
7. Kafka producer batching and linger settings vs publish lag.
8. Protobuf encoding cost on the gateway; reuse buffers.
9. Optional: pin the engine goroutine to a thread (`runtime.LockOSThread`) and compare tail latency.

## 6. Technology stack

- Language: Go (latest stable). Single module, standard layout.
- API: gRPC, Protobuf, `buf` for lint and breaking checks.
- Messaging: Apache Kafka (KRaft), using `franz-go` or `segmentio/kafka-go` (choose via ADR).
- Cache and risk: Redis 7 with Lua scripts, `go-redis`.
- Database: PostgreSQL 17, `pgx`, migrations with `goose` (or Flyway if reusing cube_test conventions; choose via ADR).
- WebSocket: `nhooyr.io/websocket` or `gorilla/websocket`.
- Observability: Prometheus client, Grafana dashboards as code, structured logging (`log/slog`), OpenTelemetry tracing optional.
- Packaging and deploy: Docker multi-stage, multi-arch images; `docker compose` for local; k3d + Argo CD + Strimzi (Kafka) + CloudNativePG for the cluster; Sealed Secrets; NetworkPolicy default-deny.
- CI: GitHub Actions running `go vet`, `staticcheck`, `golangci-lint`, unit and integration tests (Testcontainers-go), `buf lint/breaking`, Trivy image scan, gitleaks.
- Testing libraries: standard `testing`, `testify`, `rapid` or `gopter` for property tests, native Go fuzzing, Testcontainers-go.

## 7. Testing strategy

- Unit tests for matching rules, risk Lua scripts, ledger posting, and idempotency.
- Property-based tests on the engine: book is never crossed after any operation; total quantity is conserved (placed = filled + canceled + resting); price-time priority holds; the same input sequence always yields the same output hash.
- Differential test: optimized engine vs a slow, obviously correct reference implementation over millions of random operations.
- Fuzz tests for the order input decoder and gateway validation.
- Integration tests with Testcontainers (Kafka, Redis, PostgreSQL); a fake price feed server so tests never touch the real internet; a guard that fails tests that try to reach external hosts.
- Chaos tests (scripted, runnable locally): kill the engine, kill Kafka, kill PostgreSQL, partition Redis; after each, invariants and reconciliation must pass.
- Run tests in random order and with `-race`.

## 8. Repository layout (suggested)

```
mini-exchange/
  cmd/            gateway, engine, ledger, marketdata, bot, loadgen
  internal/       engine, orderbook, risk, eventlog, snapshot, ledger, marketdata, ...
  proto/          *.proto (buf managed)
  deploy/         docker-compose.yml, k8s manifests, Argo CD apps
  bench/          benchmark scenarios, results/
  docs/           adr/, architecture.md, runbook.md
  scripts/        chaos, replay, reconcile
  README.md
  BENCHMARKS.md
  requirements.md
```

## 9. Phased plan

Each phase ends with passing tests, updated docs, and a tagged commit.

- **Phase 0: Scaffold.** Repo, Go module, lint/CI, docker compose with Kafka, Redis, PostgreSQL, Prometheus. Acceptance: `docker compose up` brings everything up; CI green on an empty service.
- **Phase 1: Engine core.** Order book, matching rules, reference implementation, property tests, in-process benchmarks, first `BENCHMARKS.md` entry. Acceptance: FR-1 plus the engine target in 5.1 measured and reported.
- **Phase 2: Gateway.** Protobuf API, gRPC server, idempotency, ring buffer into the engine, execution report stream, load generator. Acceptance: FR-2, end-to-end latency histogram recorded.
- **Phase 3: Event log and recovery.** Kafka publishing, sequence numbers, snapshots, replay, determinism hash. Acceptance: FR-4 and FR-5, chaos test for engine kill.
- **Phase 4: Risk.** Redis balances, Lua reservation, limits, rate limiter. Acceptance: FR-3.
- **Phase 5: Ledger.** Double-entry schema, idempotent consumer, dead-letter topic, reconciliation job. Acceptance: FR-6.
- **Phase 6: Market data and bot.** WebSocket feed with conflation, synthetic flow, recorded-price replay. Acceptance: FR-7, FR-8.
- **Phase 7: Kubernetes and observability.** Manifests, Argo CD, Strimzi, CloudNativePG, dashboards, alerts, NetworkPolicy. Acceptance: one-command local cluster deploy, Grafana shows the latency histograms.
- **Phase 8: Optimization report.** Work through the experiments in 5.3, publish before/after results; implement FR-9 if time allows. Acceptance: `BENCHMARKS.md` is complete and reproducible with one script.
- **Phase 9 (stretch): UI.** FR-10.

## 10. Deliverables

- Working system runnable locally with one command and deployable to a local k3d cluster.
- `README.md`: purpose, architecture diagram, quick start, honest scope statement (simulation, personal project).
- `BENCHMARKS.md`: methodology, hardware, results, and optimization before/after tables.
- `docs/adr/`: one short ADR per significant decision.
- `docs/runbook.md`: how to run chaos tests, replay, and reconciliation.
- A 3 minute demo script (`scripts/demo.sh`) that starts the stack, runs a load test, kills a component, and shows recovery and reconciliation passing.

## 11. Definition of done (per task)

- Code compiles, `go vet`, linters, and `-race` tests pass.
- New behavior has tests; no hot-path allocation regressions (compare `-benchmem` before and after).
- Docs updated; commit message explains the why.
- No secrets in the repo; no calls to external services in tests.
