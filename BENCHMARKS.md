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

## Phase 3: event log, recovery and chaos

Same machine as above (Apple M5, 10 logical CPUs, darwin/arm64, go1.27.3, default GOGC, GOMAXPROCS 10). Kafka is a single-node apache/kafka 3.9.0 in Docker on the same laptop, so every Kafka number here is loopback-to-a-local-container and says nothing about a real cluster, a real network, replication or disk behaviour at scale.

### 3.1 Gateway latency with the event log in the path

Same open-loop method as phase 2 (10,000 orders/s, 5 s warm-up, 30 s measured, 3 runs, fresh gateway and fresh log each run), now with `-kafka` enabled: every command's events go through the output ring, the bounded queue and an acknowledged Kafka write. Clients are acknowledged **before** Kafka acknowledges (ADR 0007), so Kafka latency is not in the request path; the event log still costs CPU and memory bandwidth on the same host.

| Run | achieved/s | p50 µs | p90 µs | p99 µs | p99.9 µs | max µs | errors |
|---|---|---|---|---|---|---|---|
| 1 | 10,000 | 91 | 150 | 793 | 10,623 | 30,159 | 0 |
| 2 | 10,000 | 93 | 155 | 2,283 | 13,791 | 38,911 | 0 |
| 3 | 10,000 | 91 | 152 | 1,695 | 9,455 | 21,151 | 0 |

Compared with phase 2 without the log (p50 81 to 85 µs, p99 1.65 to 2.75 ms) p50 is about 10 µs higher and p99 is in the same range; the tail (p99.9, max) is noisy for the same unexplained reasons as before and I do not claim a difference there.

Event publish lag (`mx_eventlog_publish_lag_seconds`: engine stamps the event, Kafka acknowledges its batch; upper bucket bounds): p50 at most 6.4 ms, p99 at most 12.8 ms, p99.9 between 25.6 and 102.4 ms. The producer linger is 5 ms, which dominates the median. Linger and batching are the phase 8 experiment. At the end of each run the engine's last sequence equalled the Kafka-acknowledged sequence, and no order was refused.

Raw: `bench/results/gateway-kafka-10000rps-20261109T055217Z-10a6a36-run{1,2,3}.json` (+ `.hgrm`), `gateway-metrics-kafka-...txt`. Reproduce: `KAFKA=localhost:9092 scripts/loadtest.sh`.

### 3.2 Recovery time

Target (requirements 5.1): replay of 1M events in under 10 seconds.

**Clean replay of a 1,000,000-event log from Kafka** (fetch, protobuf decode, validate, rebuild state), `scripts/bench-recovery.sh`, commit `10a6a36`, 5 runs:

| Run | total | events/s |
|---|---|---|
| 1 | 551 ms | 1.84M |
| 2 | 484 ms | 2.10M |
| 3 | 484 ms | 2.10M |
| 4 | 559 ms | 1.81M |
| 5 | 457 ms | 2.22M |

Verdict: **met**, with about 20x margin, on this setup. The replayed book hash (`6f8f2f92bb284ceb`) is identical to the hash of the book that produced the events. The events came from the seeded mixed workload (about 1.3 events per command, about 400 resting orders at the end); a log with very deep books or very different shapes would behave differently.

The state-rebuild floor with no Kafka and no decoding (`BenchmarkApplyEventsReplay`, 918,895 events per operation): 33.2 to 35.6 ms, 25.8M to 27.7M events/s, 21 allocations per operation (setup only). Most of the recovery time above is therefore Kafka fetch and protobuf decoding, not the order book.

**Recovery in the chaos run** (engine killed with SIGKILL under 5,000 orders/s; see 3.3): newest snapshot at sequence 80,001, replay of the 14,883 events after it, recovered at sequence 94,884: snapshot load 1 ms, replay 24 ms, **35 ms total**; 144 ms from process start to `/readyz`. The book held only about 400 orders, so this says little about snapshot load time for a large book; that is not characterised.

### 3.3 Chaos tests

Each script ran **once** on the commit shown (not repeated, so no spread). Raw output is in `bench/results/chaos-*.txt`.

**Engine kill -9 under load** (`scripts/chaos/engine-kill.sh`, commit `5293df6`): 5,000 orders/s for 40 s, SIGKILL at about 15 s, immediate restart.

| Check | Result |
|---|---|
| Recovered state at restart | from snapshot (seq 80,001) plus 14,883 replayed events, seq 94,884 |
| Snapshot recovery vs clean replay of the whole log (`logtool compare`) | same book hash (`703fd6a718cbcfb5`) and same last sequence (267,073): **equal** |
| Engine's final sequence vs last sequence in the log | 267,073 = 267,073 |
| The log across the restart | replays cleanly with no gap or duplicate; everything the restarted engine produced applied on top of the recovered state |
| Client-visible effect | 5,052 of 200,000 requests failed `UNAVAILABLE` during the restart (about 1 s); p50 80 µs, p99 1.15 ms, p99.9 5.5 ms |
| Events at risk in a crash | the unacknowledged window sampled just before the kill was at most 119 events (an upper bound, about 24 ms of flow at this rate). **The events actually lost in the crash were not measured**, only the window they live in |

**Kafka kill -9 under load** (`scripts/chaos/kafka-kill.sh`, commit `55268bd`): 5,000 orders/s for 45 s, Kafka killed at 12 s and restarted 12 s later, load kept running throughout.

| Check | Result |
|---|---|
| During the outage | the unacknowledged backlog reached the 4,096-batch watermark; the engine had advanced from seq 76,539 to 82,440 while Kafka acknowledged only up to 77,108 |
| Load shedding | 92,683 of 225,000 requests (41%) were refused with explicit `RESOURCE_EXHAUSTED`, covering the outage and the time to drain the backlog afterwards; no other errors |
| After recovery | engine last seq 123,277 = Kafka-acknowledged seq 123,277, zero publish errors |
| No loss, no duplicate | clean replay of the log: 123,277 events in the log = last sequence 123,277, contiguous |
| Latency while healthy | p50 66 µs, p99 0.89 ms |

The same two failure modes are also asserted hermetically in `go test ./internal/eventlog` (Testcontainers; Kafka stopped and killed, engine crash and resume).

### What these numbers do not say
- One local single-node broker: no replication, no network, no multi-broker failover.
- The crash tests prove consistency of the recovered state with the log. They do **not** make client acknowledgements durable: an order acknowledged to a client in the last tens of milliseconds before a crash can be missing after recovery (ADR 0007).
- Idempotency records and report subscriptions are not recovered (ADR 0007), so after a restart pre-crash orders cannot be cancelled through the gateway by `client_order_id`.
- Snapshot cost for a large book (millions of resting orders) and replay of a log with many more orders are not measured.

## Phase 4: pre-trade risk in Redis

Same laptop as above (Apple M5, 4 performance + 6 efficiency cores = 10 logical CPUs, darwin/arm64, go1.27.3, default GOGC). Method as phase 2: open-loop, 10,000 orders/s, 5 s warm-up, 30 s measured, fresh gateway each run, **gateway, load generator and Redis on the same host**. Each run funds 16 accounts generously (so the test measures the cost of the check, not rejections), clears their risk keys first, and afterwards runs `risktool audit`: no negative balance and available plus held equal to exactly the funded total, for both assets. **All 15 audited runs below passed the audit** (the baseline has no Redis, so nothing to audit). The limits are set high so they do not interfere (`-risk-rate 1000000`); the limits themselves are covered by tests.

Two Redis setups were used. "Docker Redis" is `redis:7-alpine` from the compose file, reached through Docker Desktop's port forwarding. "Native Redis" is Homebrew `redis-server` 8.10.1 on the host, no persistence. Docker Desktop adds a VM and a proxy: `redis-benchmark` on this machine gave about 7.4k PING/s on one connection and about 50k/s on 50 connections through Docker.

Commit: the working tree that became the phase 4 commit (the recorded hash in the JSON is the parent, `a034041-dirty`). Raw results: `bench/results/gateway-*risk*-10000rps-*.json` (+ `.hgrm`), `gateway-metrics-*.txt`, `risk-audit-*.txt`. Reproduce: `REDIS=localhost:16379 scripts/loadtest.sh` (`KAFKA=...` adds the event log; `GOMAXPROCS=4` as below).

### 4.1 Results (3 runs each; every run's numbers, not averages)

| Setup | GOMAXPROCS | p50 µs | p99 µs | p99.9 µs | max µs | refused (`UNAVAILABLE`) of 300k |
|---|---|---|---|---|---|---|
| Baseline: no risk, no log (phase 2 code path) | 4 | 78 / 81 / 90 | 508 / 725 / 17,135 | 9.8k / 7.2k / 143k | 21k / 19k / 178k | 0 / 0 / 0 |
| Risk, native Redis, no log | 4 | 152 / 155 / 165 | 1,245 / 3,657 / 15,487 | 7.8k / 90.8k / 60.3k | 27k / 129k / 84k | 0 / 0 / 0 |
| Risk, native Redis, **with Kafka log** | 4 | 163 / 175 / 173 | 1,523 / 33,535 / 15,087 | 5.9k / 104k / 107k | 13k / 130k / 157k | 0 / 131 / 0 |
| Risk, native Redis, no log | 10 | 180 / 203 / 173 | 10,687 / 157,311 / 29,055 | 89k / 271k / 87k | 116k / 386k / 110k | 0 / 3,178 / 0 |
| Risk, Docker Redis, per-op settlement (first version) | 10 | 3,623 / 747 / 639 | 154,879 / 135,551 / 131,967 | 219k / 201k / 270k | 302k / 393k / 355k | 23,174 / 10,988 / 3,259 |
| Risk, Docker Redis, per-account settlement | 10 | 527 / 558 / 519 | 35,295 / 141,695 / 76,543 | 71k / 320k / 132k | 91k / 483k / 252k | 0 / 5,180 / 914 |

(Order of the three numbers in a cell is run 1 / 2 / 3. The Docker per-op rows are the first version of the settler, kept as the "before" of the optimisation described in ADR 0009.)

### 4.2 What these numbers say, and what they do not

- **The median cost of the check is about one Redis round trip.** With native Redis and the same settings, p50 goes from 78 to 90 µs (no risk) to 152 to 165 µs (risk): about 75 to 85 µs added, with Kafka about 90 µs. Through Docker Desktop the same check costs about 450 µs more. The p50 target (under 1 ms) is met in every setup except the first Docker run of the first version.
- **The tail is not under control, and I have not found the cause.** The p99 target (under 5 ms) was met in 2 of 3 runs of the no-risk baseline, 2 of 3 runs with risk and native Redis, and 1 of 3 with Kafka as well; one baseline run without any Redis has p99 17 ms. So a large part of the tail is present without Redis. It is much worse with Redis on the Docker path or with `GOMAXPROCS=10`.
- **Evidence that Redis itself stalls, from Redis's own slowlog** (native Redis, `GOMAXPROCS=10`): 86 commands took more than 5 ms *inside Redis*, up to 123 ms, and the slowlog also lists single-key O(1) commands run by the scripts, such as an `HSET` (51 ms) and a plain `GET` (21 ms), not only whole scripts. With the same setup and `GOMAXPROCS=4` it was 14 commands, maximum 32 ms, and no refused orders. My working hypothesis is CPU oversubscription: the gateway's engine and dispatcher goroutines spin-wait, the load generator spins too, and with 6 of the 10 cores being efficiency cores the Redis thread is sometimes not scheduled for tens of milliseconds. That is one A/B experiment on one machine, not a proof; thread pinning and spin tuning belong to phase 8 together with the older unexplained p99.9/max in phases 2 and 3.
- **Fail-closed shows up as measurable refusals.** When Redis does not answer within the 100 ms timeout the order is refused with `UNAVAILABLE` (nothing admitted). That happened in 7 of the 15 risk runs, up to 7.7% of orders in the worst case, and in none of the baseline runs. These are not matching-engine failures; they are the safety behaviour working while the host is overloaded. A deployment would size Redis and the host so this does not happen at the target rate; I have not shown a setup on this laptop where it never happens across repeated runs.
- **Cost inside Redis** (`INFO commandstats`): about 14 µs of Redis time per script call on native Redis (11.6 µs with `GOMAXPROCS=4`) and about 28 to 35 µs through Docker. At the first version's 22k calls/s that was already about 60% of the single Redis thread on Docker.
- **Settlement keeps up.** In the two Docker runs whose metrics I examined (batched settlement) the queue was empty at the end and the mean time from engine event to applied in Redis was 3.0 and 5.0 ms; across all 15 load runs `mx_risk_settle_dropped_total` was 0 and `mx_risk_settle_anomalies_total` was never incremented (no op found a missing or too-small reservation).

### 4.3 Correctness checks (FR-3), against a real Redis

- `TestConcurrentReservationsNeverOverdraw`: 400 goroutines reserve against a balance that fits exactly 100 orders; exactly 100 succeed, and a sampler asserts throughout that available is never negative and available plus held always equals the deposit.
- Lua unit tests: reserve holds the right amount and rejects underfunded, oversize, too-many-open and rate-limited orders (token bucket with an injected clock: burst, per-account isolation, refill, cap at the burst after a long pause); settlement lifecycle (price improvement released, partial fills, cancel, reject, duplicate delivery of the same event applied once); values near the 2^52 bound stay exact through Lua's doubles.
- End to end through the real gateway and pipeline (`TestRiskConservationEndToEnd`): 3,000 random orders (limit, IOC, market, cancels) over 8 accounts: never a negative balance, base and quote exactly conserved, nothing left held and no open reservation once everything is cancelled.
- Redis stopped and restarted under the gateway: `UNAVAILABLE` while it is down, and orders trade and settle correctly after it returns, including Redis having lost its script cache.
