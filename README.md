# mini-exchange

A **personal simulation project**: a low-latency matching engine, order gateway and double-entry ledger written in Go. It is a portfolio piece for practising latency engineering and payment-style correctness. It is **not** a real exchange: no real money, no real market connectivity, no regulatory features, and no claim of production scale.

The full specification is in [requirements.md](requirements.md). Architecture decisions are in [docs/adr](docs/adr).

## Architecture (target)

```
Client -> gateway (gRPC) -> idempotency check -> risk (Redis Lua, reserve funds)
       -> engine input queue (preallocated ring buffer)
       -> matcher (single goroutine, no locks, no I/O)
       -> output ring buffer -> eventlog (async batch) -> Kafka
                                                        -> ledger -> PostgreSQL
                                                        -> marketdata -> WebSocket
       -> execution report back to client (from output buffer, not from Kafka)
```

## Status

| Phase | Scope | State |
|---|---|---|
| 0 | Scaffold: module, CI, docker compose (Kafka, Redis, PostgreSQL, Prometheus) | done |
| 1 | Engine core: order book, matching rules, reference implementation, property and differential tests, benchmarks | in progress |
| 2-9 | Gateway, event log and recovery, risk, ledger, market data, Kubernetes, optimization report, UI | not started |

## Quick start

Requirements: Go (see `go.mod`), Docker with Compose, `golangci-lint` for `make lint`.

```bash
make test          # unit + property tests with -race, random order (short differential run)
make test-full     # includes the full 1M-operation differential test
make lint
make bench         # go micro-benchmarks

make up            # docker compose: kafka, redis, postgres, prometheus, gateway
make smoke         # checks the gateway /healthz and /metrics
make down
```

Host ports are chosen to avoid common clashes and can be overridden with `KAFKA_PORT`, `REDIS_PORT` (16379), `POSTGRES_PORT` (15432), `GATEWAY_PORT` (18080) and `PROMETHEUS_PORT` (9090).

## Layout

```
cmd/gateway     phase 0: empty service (health + metrics)
internal/engine matching engine and order book
internal/refbook  naive reference implementation (tests only)
internal/enginetest  deterministic random command generator
bench/          benchmark tools and results
deploy/         docker-compose and Prometheus config
docs/adr        architecture decision records
```
