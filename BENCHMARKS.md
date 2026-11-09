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
