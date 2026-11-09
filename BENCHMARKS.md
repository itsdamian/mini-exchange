# Benchmarks

Numbers here are **measured**, not targets. Every table says which file in `bench/results/` it came from. Reproduce everything with `scripts/bench.sh` (see `ORDERS`/`RUNS` env vars for a quicker run). Targets are in `requirements.md` section 5.1.

## Phase 1: engine core (in-process, no network)

### Environment

| | |
|---|---|
| Hardware | Apple M5, 10 logical CPUs |
| OS | darwin/arm64 |
| Go | go1.27.3 |
| GOMAXPROCS | 10 |
| GOGC / GOMEMLIMIT | 100 (default) / unset |
| Commit | `d3e196b` at measurement time, clean tree. The history was later rewritten to fix the commit author; the same tree is now commit `29a8101`, which is the commit to check out to reproduce |
| Raw results | `bench/results/engine-mixed-20261109T043025Z-d3e196b.json`, `engine-deep-...json`, `gobench-...txt` |

### Method

See [ADR 0004](docs/adr/0004-benchmark-method-phase1.md). In short: a seeded command stream (`internal/enginetest`) is replayed after one unmeasured warm-up lap; `enginebench` times every `Apply` call into an HdrHistogram; 5 runs of 5M orders each.

- **mixed**: about 1,000 resting orders, 16 accounts, ~28% cancels, ~6% market, ~10% IOC, rest GTC limits (some crossing). About 0.28 trade events per operation.
- **deep**: about 20,000 resting orders, prices within ±500 ticks of mid.

This measures the matcher's service time driven back-to-back. It says nothing about queueing or the gateway; the open-loop method for that arrives in phase 2.

### Results

Throughput and allocations (`go test -bench -benchmem`, 3 runs each):

| Benchmark | ns/op | orders/s | allocs/op |
|---|---|---|---|
| ApplyMixed | 60.9 / 62.4 / 63.1 | 15.8M to 16.4M | 0 |
| ApplyDeepBook | 74.8 / 79.7 / 81.5 | 12.3M to 13.4M | 0 (1 to 2 B/op, see below) |
| ReferenceMixed (naive, for scale) | ~4,350 | ~230k | 0 |

Latency distribution (`enginebench`, per-order, 5 runs; ns; values are quantised to ~41.7 ns, see the timer note):

| Scenario | orders/s (5 runs) | p50 | p90 | p99 | p99.9 | max |
|---|---|---|---|---|---|---|
| mixed | 11.7M to 11.9M | 42 | 84 | 125 | 167 | 34 µs to 486 µs |
| deep | 9.9M to 10.2M | 83 | 84 | 125 to 167 | 208 to 333 | 102 µs to 372 µs |

**Timer note.** On this machine the clock ticks in ~41.7 ns steps and each `time.Now` / `time.Since` pair costs about that much, so per-order latencies of tens of nanoseconds are quantised and include timer cost (`enginebench` prints it: p90 42 ns). The `orders/s` figures from total elapsed time are lower than `go test -bench` because they include this timing cost. The max column is dominated by OS scheduling and GC noise, not by the matcher.

### Against the phase 1 targets

| Target (section 5.1) | Measured | Verdict |
|---|---|---|
| Engine p99 under 50 µs per order | 125 to 167 ns on this synthetic workload | met, with large margin |
| At least 500k orders/s, single symbol | 10M to 16M orders/s | met, with large margin |
| Zero steady-state allocations in the hot path | mixed: 0. deep: about 150 to 190 allocations per 5M orders | **not strictly met** |

The wide margins mostly reflect that the target was set for a conservative machine and that this workload is synthetic and CPU-cache friendly. They should not be read as a claim about production behaviour.

### Known gap: allocations in the deep scenario

A heap profile (`-memprofile -memprofilerate 1`) attributes the remaining allocations to the insertion `b.index[c.OrderID] = idx` in `Book.rest`: Go's built-in map occasionally allocates when churning through many inserts and deletes with a large live set. Replacing the `map[uint64]int32` with a preallocated open-addressing integer table is experiment 4 in section 5.3 and will be recorded here as a before/after entry in phase 8.

### Correctness checks that back these numbers

- `TestDifferentialAgainstReference`: 1,000,000 random operations (two seeds), identical events for every operation and identical book hash at checkpoints versus `internal/refbook`.
- Property tests (rapid): book never crossed, per-order quantity conserved (accepted = filled + cancelled + resting), price-time priority, no self-trades, same input gives the same output hash and book hash.

## Phase 2: gateway end to end (gRPC over loopback)

### Environment

| | |
|---|---|
| Hardware / OS / Go | Apple M5, 10 logical CPUs; darwin/arm64; go1.27.3 |
| GOMAXPROCS / GOGC / GOMEMLIMIT | 10 / 100 (default) / unset (gateway and loadgen both) |
| Commit | `790907a` (clean tree) |
| Topology | **gateway and load generator on the same machine over loopback**; no network, no Kafka, Redis or PostgreSQL in the path yet. They compete for the same 10 cores |
| Raw results | `bench/results/gateway-10000rps-20261109T051753Z-790907a-run{1,2,3}.json` (+ `.hgrm` full percentile distributions), `gateway-metrics-...-run{1,2,3}.txt` (server-side Prometheus histograms) |
| Reproduce | `make loadtest` (`scripts/loadtest.sh`; `RATE`, `DURATION`, `RUNS` env vars) |

### Method

- **Open loop, fixed arrival rate.** `cmd/loadgen` issues requests on a fixed schedule (10,000 per second) whether or not earlier responses have returned, and measures each latency from the request's **intended send time**, so a stall cannot hide the requests that should have been sent during it (no coordinated omission). Latencies are recorded in an HdrHistogram (3 significant digits).
- 5 s unmeasured warm-up, then 30 s measured (300,000 requests), 3 runs, each against a freshly started gateway.
- Workload: the seeded mixed stream of `internal/enginetest` (about 65% places, 35% cancels; some malformed orders for typed rejects; 16 accounts), 4 gRPC connections. Cancels that overtake their place under concurrency come back as `UNKNOWN_ORDER`; that is an expected outcome, counted in the raw JSON. No request failed with a gRPC error in any run.
- The loadgen reports its own scheduling lag next to the result so you can judge whether the generator kept up.

### Results: 10,000 orders/s sustained

Client-observed latency, microseconds, from intended send time:

| Run | achieved/s | p50 | p90 | p99 | p99.9 | max |
|---|---|---|---|---|---|---|
| 1 | 10,000 | 81 | 140 | 1,650 | 10,671 | 19,215 |
| 2 | 10,000 | 82 | 146 | 2,753 | 37,887 | 77,631 |
| 3 | 10,000 | 85 | 147 | 2,401 | 13,455 | 26,719 |

Loadgen scheduling lag (how late it actually sent versus the schedule), microseconds: p50 1, p99 85 to 290, p99.9 1,642 to 10,127, max 4,511 to 21,215.

Server-side histograms (`mx_engine_*`, upper bucket bounds, so values are "at most"):

| Run | match time p99 / p99.9 / p99.99 | engine queue wait p99 / p99.9 / p99.99 |
|---|---|---|
| 1 | 1.6 µs / 12.8 µs / 51.2 µs | 51 µs / 819 µs / 6.6 ms |
| 2 | 1.6 µs / 12.8 µs / 51.2 µs | 51 µs / 3.3 ms / 13.1 ms |
| 3 | 1.6 µs / 12.8 µs / 51.2 µs | 51 µs / 819 µs / 3.3 ms |

### Against the phase 2 targets (10k orders/s, localhost)

| Target (section 5.1) | Measured | Verdict |
|---|---|---|
| p50 under 1 ms | 81 to 85 µs | met |
| p99 under 5 ms | 1.65 to 2.75 ms | met, on this machine and topology |

### What these numbers do not say

- **The tail is not clean.** p99.9 is 10 to 38 ms and the maximum up to 78 ms. Part of that is the load generator itself stalling (its p99.9 scheduling lag reached 10 ms in run 2) on a machine shared with the gateway, and part is on the server side: engine queue wait at p99.9 is up to 3.3 ms. The matcher is not the cause (match time stays within tens of microseconds). The remaining suspects are goroutine wake-up latency after the engine parks, scheduler contention between the two processes, and GC (25 short cycles per run, each about 0.1 ms, so unlikely). I have not isolated which dominates; separating them (pinning the engine thread, a separate load generator host, longer spinning before parking) belongs with the phase 8 experiments.
- Loopback on one machine says nothing about a real network.
- Only one request mix and one rate were measured. Higher rates and saturation behaviour are not characterised yet.
- The idempotency records are in memory and unbounded (ADR 0006); long runs grow gateway memory.
