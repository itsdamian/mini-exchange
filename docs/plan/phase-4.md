# Plan: phase 4 (pre-trade risk in Redis)

- [x] Proto: `ORDER_TOO_LARGE`, `TOO_MANY_OPEN_ORDERS` reject reasons (buf breaking check stays green: enum values added)
- [x] `internal/risk`: Lua scripts (reserve with token bucket and limits; settle: bind, fill, cancel, release), `Checker` (Reserve, Release, Deposit, Balance, Audit), asynchronous idempotent `Settler`, Prometheus metrics
- [x] Pipeline: dispatcher turns engine events into settlement ops (no change to the matcher)
- [x] Gateway: reserve before the engine, release when the order is refused before it, fail closed, risk rejects not cached
- [x] `cmd/gateway` flags (`-redis`, `-risk-*`), `cmd/risktool` (fund, audit), compose: Redis with AOF, gateway wired to it
- [x] Tests against a real Redis (Testcontainers): script unit tests, token bucket with an injected clock, concurrent reservations never overdraw, settlement lifecycle and duplicates, values near 2^52, end-to-end gRPC flow with funds conserved, Redis stopped and restarted
- [x] Load test with risk in the path (`REDIS=... scripts/loadtest.sh`) with a conservation audit after each run
- [x] ADR 0009, BENCHMARKS, README
- Acceptance: FR-3 (concurrent reservation test never produces a negative available balance; Lua scripts have unit tests).
