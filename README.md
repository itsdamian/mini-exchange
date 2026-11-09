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
| 3 | Kafka event log, snapshots, replay and recovery | next |
| 4 | Pre-trade risk in Redis (Lua) | planned |
| 5 | Double-entry ledger in PostgreSQL, dead-letter topic, reconciliation | planned |
| 6 | Market data over WebSocket, synthetic order flow | planned |
| 7 | Kubernetes (k3d, Argo CD, Strimzi, CloudNativePG), dashboards | planned |
| 8 | Optimization report, optional standby failover | planned |
| 9 | Minimal UI (stretch) | planned |

The engine and a gRPC gateway exist today; there is no Kafka, Redis or PostgreSQL in the order path yet, so nothing is persisted and a restart loses all state. The diagram below is the **target** architecture.

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

Known limits: idempotency records are in memory and unbounded, and nothing survives a restart until phase 3.

## Quick start

Requirements: Go (version in `go.mod`), Docker with Compose, and `golangci-lint` for `make lint`.

```bash
make test          # unit and property tests with -race, random order (short differential run)
make test-full     # includes the full 1M-operation differential test
make lint          # go vet + golangci-lint
make bench         # Go micro-benchmarks
make bench-report  # full reproducible engine report (writes bench/results/)
make loadtest      # end-to-end open-loop gRPC load test, 3 runs at 10k orders/s
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
proto/               Protobuf API (buf managed); gen/ holds the generated Go code
internal/gateway     gRPC server, validation, idempotency
internal/pipeline    gateway-to-engine rings, engine goroutine, dispatcher, report hub
internal/ring        lock-free SPSC and MPSC bounded queues
internal/benchenv    environment capture for benchmark results
internal/engine      matching engine and order book
internal/refbook     naive reference implementation (differential tests, benchmarks)
internal/enginetest  deterministic random command generator
internal/httpserver  health and metrics HTTP surface
bench/               benchmark tools; results/ holds raw JSON and text output
deploy/              docker compose and Prometheus config
docs/adr             architecture decision records
docs/plan            per-phase plans and task lists
scripts/             helper scripts (bench.sh)
```

## Relationship to cube_test

This project reuses the *style* of the author's earlier `cube_test` project (Java, Spring Boot, Kafka, k3d, Argo CD, Prometheus/Grafana) for local and cluster deployment. It is a separate repository and shares no code with it.
