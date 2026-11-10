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

A heap profile (`-memprofile -memprofilerate 1`) attributes the remaining allocations to the insertion `b.index[c.OrderID] = idx` in `Book.rest`: Go's built-in map occasionally allocates when churning through many inserts and deletes with a large live set. Replacing the `map[uint64]int32` with a preallocated open-addressing integer table is experiment 4 in section 5.3. **Done in phase 8: see section 8.1 (the allocations are gone and `Apply` is about 22 to 27% faster).**

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

## Phase 5: double-entry ledger in PostgreSQL

Setup: same laptop as phase 4 (Apple M5, 10 logical CPUs), `GOMAXPROCS=4`, native Redis 8.10.1 on port 6390 without persistence, Kafka `apache/kafka:3.9.0` and PostgreSQL `postgres:17-alpine` from the compose file **through Docker Desktop** (a VM plus port forwarding, like the Docker Redis of phase 4). Gateway, load generator, ledger consumer and the reconciliation all run on this one machine, 10,000 orders/s for 30 s after a 5 s warm-up, 3 runs, `scripts/loadtest.sh` with `KAFKA=… REDIS=… PG=1` (each run: fresh topic, fresh Redis balances, truncated ledger, 16 accounts funded identically in Redis and the ledger). Fees are 0. Raw files: `bench/results/*risk-ledger-v1*` and `*risk-ledger-v2*`.

### 5.1 Results (every run)

| Setup | Run | p50 µs | p99 µs | p99.9 µs | Trades settled | Mean ledger lag | Ledger caught up after the load stopped | Reconciliation |
|---|---|---|---|---|---|---|---|---|
| v1: one transaction per log record | 1 | 296 | 6,275 | 19,311 | 99,124 | 31.3 s | 49.9 s | 0 discrepancies |
| v1 | 2 | 292 | 4,619 | 30,831 | 99,228 | 30.7 s | 49.0 s | 0 |
| v1 | 3 | 284 | 5,203 | 38,367 | 99,289 | 30.0 s | 48.6 s | 0 |
| v2: records of one poll share a transaction (up to 256) | 1 | 293 | 4,207 | 13,815 | 99,199 | 3.9 s | 5.0 s | 0 |
| v2 | 2 | 305 | 7,151 | 20,207 | 99,257 | 4.6 s | 6.2 s | 0 |
| v2 | 3 | 316 | 5,399 | 15,959 | 99,178 | 5.2 s | 7.1 s | 0 |

"Mean ledger lag" is the mean of `mx_ledger_settle_lag_seconds` (engine event to commit in PostgreSQL), taken as sum over count of the histogram. "Caught up" is the elapsed time of the reconciliation, which re-checks every 0.5 s until clean; each attempt also re-reads the whole log, so it is an upper bound with up to 0.5 s of polling and a log scan added.

### 5.2 What these numbers say, and what they do not
- **The ledger is not in the order path**: the engine and the gateway never wait for PostgreSQL, and a database outage does not stop trading (tested). The gateway latency in this table (p50 284 to 316 µs) is higher than phase 4's with risk and Kafka (163 to 175 µs, native Redis, 4 of 10 cores); here the machine also runs PostgreSQL in Docker, the ledger consumer and (after the load) the reconciliation. I did not run a controlled A/B to separate those causes, so I do not claim the ledger costs the gateway nothing, only that it is not waited for.
- **Lag is the open problem.** v1 settled one log record per database transaction and fell behind from the start: about 31 s mean lag and 49 s to catch up after 30 s of load. Grouping the records of a poll into one transaction cut that to 4 to 5 s mean and 5 to 7 s to catch up, about 7 times better. It is still seconds, not milliseconds, behind a 10,000 orders/s flow (about 3,300 trades/s). I have not profiled where the time goes (PostgreSQL through Docker Desktop, the per-trade `apply_posting` statements, Kafka fetch size) so I cannot say which would help next; that belongs to phase 8. The engine does not depend on it, but anything that needs the ledger to be current (reconciliation) must wait.
- **Correctness held in every run**: 6 of 6 reconciliations against the event log and against Redis reported zero discrepancies, and every trade in the log has exactly one settlement (99,124 to 99,289 per run).
- These are single runs on a laptop, three per row, not a statistical claim; I did not establish whether the p99 and p99.9 differences between v1 and v2 are more than run-to-run noise.

### 5.3 Correctness checks (FR-6), against real PostgreSQL, Kafka and Redis
- **Replay**: applying the same 2,000 random trades twice leaves identical balances, with fees (`TestApplyingTheSameTradesTwiceYieldsIdenticalBalances`). Replaying a 5,000-command log from offset 0 with a new consumer group into the same ledger settles nothing again (every trade counted as a duplicate), and into a brand-new ledger funded the same way gives identical balances (`TestReplayFromTheBeginningYieldsIdenticalBalances`).
- **Poison isolation**: a log with undecodable bytes, a batch of another symbol, an unknown schema version, a trade for an unfunded account and a zero-quantity trade, all in the middle of valid records, ends with 5 records in the dead-letter topic with their reasons and every valid trade settled (`TestPoisonRecordsGoToTheDeadLetterTopicAndDoNotBlock`).
- **Chaos**: PostgreSQL stopped while orders keep flowing through the real gRPC gateway, Redis risk, engine and Kafka, restarted after the consumer provably hit the outage; the reconciliation against the log and against Redis (with 2 and 1 bps fees) reports zero discrepancies (`TestWholePathWithPostgresOutageReconcilesToZero`). A PostgreSQL stop and restart under a replayed log loses and duplicates nothing (`TestPostgresOutageLosesAndDuplicatesNothing`).
- **The reconciliation detects problems**: an edited balance, a deleted settlement and an orphan settlement each produce the expected discrepancy kinds, and are gone when restored (`TestReconcileFindsCorruption`).
- **Invariants** (entries sum to zero per asset, no negative customer balance, one settlement per trade) are enforced by the schema and re-checked by every reconciliation; the posting builder is also property tested to balance for any price, quantity and fee.

## Phase 6: market data over WebSocket

Setup: same laptop (Apple M5, 10 logical CPUs), `GOMAXPROCS=4` for the gateway, Kafka `apache/kafka:3.9.0` from the compose file through Docker Desktop, no Redis and no ledger, 10,000 orders/s for 30 s after a 5 s warm-up, 3 runs per row, `scripts/loadtest.sh` with `KAFKA=…` and, for the second row, `WS=1000`. The gateway, load generator, marketdata process and 1,000 WebSocket clients (`cmd/wsload`, one process) all run on this machine over loopback, so the clients compete with the gateway for CPU; that is a harsher test of "no impact" than a separate host would be. Marketdata uses the defaults: 10 levels, 50 trades, 100 ms conflation. Raw files: `bench/results/*md-off*` and `*md-ws1000*`.

### 6.1 Gateway latency with and without 1,000 WebSocket clients (every run)

| Setup | Run | p50 µs | p90 µs | p99 µs | p99.9 µs | max µs |
|---|---|---|---|---|---|---|
| Kafka log, no market data | 1 | 91 | 151 | 1,769 | 11,175 | 27,215 |
| | 2 | 89 | 148 | 1,680 | 28,719 | 58,079 |
| | 3 | 86 | 143 | 2,569 | 82,431 | 112,767 |
| Kafka log, marketdata + **1,000 clients** | 1 | 105 | 239 | 8,351 | 30,015 | 59,935 |
| | 2 | 103 | 190 | 1,548 | 40,351 | 72,703 |
| | 3 | 103 | 214 | 2,933 | 31,183 | 60,255 |

### 6.2 The clients and the process (1,000 clients, 3 runs)

| Run | Connect failures | Dropped by server | Frames per client per second | Frame age p50 / p99 (ms) | Gap between frames p50 / p99 (ms) | Marketdata RSS at start / end |
|---|---|---|---|---|---|---|
| 1 | 0 | 0 | 8.27 | 48.8 / 72.7 | 100.0 / 107.4 | 36 MB / 186 MB |
| 2 | 0 | 0 | 8.29 | 33.8 / 52.6 | 100.0 / 101.6 | 35 MB / 188 MB |
| 3 | 0 | 0 | 8.27 | 31.5 / 53.5 | 100.0 / 106.3 | 38 MB / 189 MB |

"Frame age" is engine event time to the client receiving the frame that contains it (same host clock). "Frames per client" is below 10 because the measurement includes the first seconds of connection and the quiet periods of the run. RSS is sampled every 2 s: it rises while the clients connect and the load starts, then stays between about 170 and 190 MB for the rest of the run.

### 6.3 What these numbers say, and what they do not
- **The 100 ms conflation works**: the median gap between frames is 100.0 ms and the p99 is 101.6 to 107.4 ms; no client got more than about one frame per 100 ms while the engine produced about 30,000 events per second.
- **Memory is stable, not small**: about 185 MB with 1,000 clients and a busy market, flat after the clients connected. About 150 MB of that is above the idle process (36 MB), roughly 150 KB per client (goroutine, WebSocket buffers, and the Go runtime's own headroom); I did not break it down further. The hermetic test (1,000 clients, heap measured after GC while 20,000 more events arrive) saw no growth.
- **No clients were lost or dropped**, and a client that stops reading is disconnected after the write timeout (tested with a client that never reads; `mx_md_slow_disconnects_total` was 0 in these runs, as expected, because every load-test client read).
- **"No impact on engine latency" is not exactly what I measured.** Median gateway latency rose in all three runs, from 86 to 91 µs to 103 to 105 µs (about 15 µs, 17%), and p90 rose from 143 to 151 µs to 190 to 239 µs. The p99 was 1.5 to 8.4 ms with the clients and 1.7 to 2.6 ms without; one run with clients had p99 of 8.4 ms, the other two were within the spread of the baseline. p99.9 and max are dominated by the unexplained tail already recorded in phases 2 to 4 and are not distinguishable between the rows with three runs each. The marketdata process cannot slow the matcher by design (it only reads Kafka), so I attribute the median shift to CPU contention on one laptop (the 1,000 client goroutines and the marketdata process share the 10 cores with the gateway and the load generator, 6 of which are efficiency cores) and not to anything in the engine. That is a hypothesis; I did not run a control with the clients pinned elsewhere, and I did not run on separate machines.
- **Frame age is 30 to 50 ms at the median** (event to client), made of the Kafka publish delay, up to one 100 ms build interval and up to one 100 ms client interval; the 50 to 70 ms p99 is lower than the sum of the two intervals because they are not aligned. It is a lag of the view, not of the engine.
- These are single-machine runs, three each; no statistical claim.

### 6.4 Correctness checks (FR-7, FR-8)
- **Depth**: the frame's top-5 levels equal the reference book's after 5,000 random commands, trades are in order, and the consumer on real Kafka rebuilds the same depth from a log that is half there before it starts and half arriving while it runs (`TestFeedDepthMatchesTheReferenceBook`, `TestConsumerRebuildsThenFollowsTheLog`).
- **Conflation**: a client sees at most one frame per 100 ms while tens of thousands of events are applied (`TestClientGetsStateAtOnceAndConflatedUpdates`).
- **1,000 clients, hermetic**: all receive updates, heap is flat while a client that never reads is connected, and a client that stops reading is disconnected and not buffered (`TestThousandClientsStableMemoryAndSlowClientDropped`, `TestSlowClientIsDisconnectedNotBuffered`); the client limit answers 503.
- **Identical event streams**: through the real gRPC gateway and engine, two runs of the bot over the same 400 prices of the price file with seed 42 produce identical event streams (timestamps excluded, they come from the wall clock), including more than 100 trades and more than 1,000 cancels; seed 43 gives a different stream (`TestSameFileAndSeedGiveIdenticalEventStreams`).
- **Offline**: the Coinbase source is tested against a fake server, and a test asserts that dialing the real feed URL is refused before any traffic (`internal/nonet`). The price file in `data/` is synthetic, not a recording.

## Phase 7: Kubernetes (k3d) and observability

This phase is about deployment, not speed. The numbers below only show that the dashboards have data; they are **not** comparable to the localhost benchmarks above.

Setup: Apple M5, 10 logical CPUs, Docker Desktop with 16 GB; one k3d node (k3s v1.35.5) running Argo CD, Strimzi (Kafka 4.3.1, one KRaft node), CloudNativePG (PostgreSQL 17, one instance), kube-prometheus-stack and the application; the gateway has `GOMAXPROCS=2` and no CPU limit; the load generator runs as a Job in the same cluster, so it shares the 10 cores with the whole stack.

### 7.1 What was measured
- **Deployment check** (`scripts/k8s-test.sh`, after seeding and 60 s of load at 2,000 orders/s): 27 of 27 checks pass: the five Argo CD applications Synced and Healthy; gateway, Redis, ledger and market data ready with no restarts; Kafka and PostgreSQL Ready; a second engine replica refused; a stranger pod cannot reach Redis and a pod from another namespace cannot reach PostgreSQL or Kafka; Prometheus scrapes the three application targets; the PlaceOrder latency, engine match time and ledger lag histograms have data; the alert rules are loaded; Grafana has the "Mini Exchange" dashboard and its Prometheus datasource returns the PlaceOrder latency query.
- **Recovery**: deleting the gateway pod, it came back ready from its snapshot volume and the log, and the engine sequence continued (337,903 before and after; no restart from 1).
- **In-cluster load, 2,000 orders/s for 60 s, 120,000 requests, 0 errors** (two runs): p50 535 and 539 µs, p99 4,427 and 5,731 µs, p99.9 74,175 and 36,127 µs. This is the load generator in a pod through the k3d network, with the gateway on 2 cores, next to everything else.
- **Memory** (working set, after the load): Kafka 663 MiB, PostgreSQL 171 MiB, gateway 75 MiB, marketdata 37 MiB, ledger 14 MiB, Redis 10 MiB. The whole k3d node (operators, Argo CD, Prometheus, Grafana included) was **5.0 GiB**.

### 7.2 What this does not say
- Time from nothing to Healthy was not measured. The working run, with the images already pulled once, took on the order of 20 to 30 minutes by my watch, most of it image pulls; the first attempt failed on the git protocol (ADR 0012) and the first healthy deployment had one start-up restart per service before the init containers were added. I did not time a clean run end to end.
- The latency figures are one cluster on one laptop, two runs, not a result about the system's speed.
- The alert rules are unit-tested with promtool but were not made to fire on a real fault; reconciliation against the cluster's databases and a browser look at the dashboard were not done (ADR 0012).

## Phase 8: optimization report

Every experiment of requirements section 5.3, with what was measured and what was decided. Everything is reproducible with one script, `scripts/bench-all.sh` (a full run takes about 45 minutes; `QUICK=1` runs the whole thing in about 10 minutes to check it works). The raw files are in `bench/results/phase8/` and `bench/results/gateway-*p8-*`.

### 8.0 Setup and a warning about this machine

Apple M5 (10 logical CPUs, 4 performance and 6 efficiency cores), macOS, go1.27.3, commit `08060db` as the baseline (the last commit before this phase) and the working tree of this phase as "after". The Go micro-benchmarks ran 5 times of 1 s each (`-count 5`); the gateway experiments ran the load test at 10,000 orders/s, 30 s measured after a 5 s warm-up, 3 runs per setting (5 interleaved pairs for rings against channels), with the Kafka event log in the path and no Redis or ledger, the load generator and the gateway on the same host over loopback. `GOMAXPROCS` is the default (10) except where a row says otherwise. Environment, load average and commit are in `bench/results/phase8/env-*.txt`.

**The machine is noisy, and this matters for every tail number below.** The load average was about 7 while idle (the Docker VM runs other containers, among other things). The control in 8.6 shows what the host alone does. Medians (p50) are stable between runs; p99.9 and max are not, and differences in them between settings are mostly not distinguishable from run-to-run variation.

### 8.1 Experiment 4 (and the phase 1 gap): integer order ids and an open-addressing id index

**Change adopted.** The engine looked orders up in a built-in `map[uint64]int32`, which phase 1 identified as the source of the allocations in the deep-book scenario. It now uses a preallocated open-addressing table with linear probing and backward-shift deletion (`internal/engine/idtable.go`): no tombstones, no allocation in steady state.

| Benchmark (`go test -bench -benchmem`, 5 runs) | before (map) | after (table) | change |
|---|---|---|---|
| ApplyMixed, ns/op | 58.8 to 59.2 | 42.7 to 42.9 | about 27% faster |
| ApplyDeepBook, ns/op | 70.4 to 71.2 | 54.7 to 59.9 (four runs 54.7 to 55.4, one 59.9) | about 22% faster |
| allocations in `Apply` (deep, allocation profile at rate 1) | about 150 to 190 per 5M orders | none: the profile lists only set-up code (generator, `NewBook`) | gap closed |

Orders per second: mixed 16.9M to 17.0M becomes 23.3M to 23.4M; deep 14.0M to 14.2M becomes 16.7M to 18.3M. The differential test against the reference implementation (1,000,000 operations) and the property tests still pass, and the table has its own property test against a Go map (including collisions, deletes in the middle of probe runs and growth) and a no-allocation churn test. Files: `engine-before-manual-20261110.txt`, `engine-after-manual-20261110.txt` (and `scripts/bench-all.sh` stage 1 repeats it against a worktree of the baseline commit). Phase 1's "not strictly met" on zero hot-path allocations is now met.

The rest of experiment 4 is in the micro-benchmark table of 8.2: the cost of the things the hot path avoids.

### 8.2 Micro-experiments (isolated mechanisms, `internal/experiments`)

These measure one mechanism in isolation, not the matcher. A mechanism that is 10 times faster in isolation can be invisible in the whole system, and the other way round (8.4).

**Experiment 1: price-level index** (sorted slice = what the engine uses)

| Levels per side | sorted slice | B-tree (degree 32) | skip list |
|---|---|---|---|
| 10 | 14.5 (14.4 to 14.6) | 36.1 (35.9 to 36.8) | 43.5 (43.5 to 44.2) |
| 100 | 22.3 (22.2 to 22.6) | 62.2 (62.0 to 62.6) | 63.9 (63.7 to 67.3) |
| 1000 | 48.8 (48.4 to 49.4) | 78.9 (78.1 to 82.3) | 74.8 (74.6 to 77.3) |
| 10000 | 317.7 (316.6 to 323.3) | 109.7 (109.3 to 112.4) | 97.4 (97.2 to 104.4) |

All three: 0 allocs/op in steady state.

**Experiment 2: order queue inside a price level** (1,000 resting orders; each step adds one and cancels one)

| Structure | median ns/op (min to max of 5) | allocs/op |
|---|---|---|
| intrusive list in a preallocated slab (engine) | 2.4 (2.4 to 2.4) | 0 |
| container/list, one heap order per element | 18.9 (18.9 to 19.2) | 2 |
| hand-written list of heap nodes | 13.1 (12.7 to 14.0) | 1 |
| hand-written list of nodes from sync.Pool | 9.1 (9.1 to 9.2) | 0 |

**Experiments 3 and 5: handing 48-byte items between goroutines**

| Queue | median ns/op (min to max of 5) | allocs/op |
|---|---|---|
| 1 producer, 1 consumer: buffered channel | 21.6 (21.1 to 22.3) | 0 |
| 1p1c: ring as in internal/ring today | 72.7 (69.6 to 73.1) | 0 |
| 1p1c: ring, cursors on separate cache lines | 87.4 (86.5 to 88.3) | 0 |
| 1p1c: ring, separate cache lines and cached opposite cursor | 53.9 (53.3 to 54.4) | 0 |
| 4 producers, 1 consumer: buffered channel | 30.0 (29.4 to 30.2) | 0 |
| 4p1c: MPSC ring (CAS per push) | 210.2 (205.2 to 236.0) | 0 |
| round trip: channels | 186.4 (182.2 to 193.6) | 0 |
| round trip: rings, both sides spin | 211.9 (209.4 to 220.2) | 0 |
| round trip: rings, spin then runtime.Gosched() | 1100.0 (1026.0 to 1110.0) | 0 |

**Experiment 4: costs the hot path avoids**

| Operation | median ns/op (min to max of 5) | allocs/op |
|---|---|---|
| time.Now() | 26.4 (26.3 to 26.5) | 0 |
| time.Since() | 11.8 (11.7 to 12.0) | 0 |
| a counter handed in by the sequencer (what the matcher stamps) | 0.5 (0.5 to 0.5) | 0 |
| order id as text with fmt.Sprintf | 29.3 (29.0 to 30.0) | 2 |
| order id as text with strconv.Itoa | 9.8 (9.6 to 10.2) | 0 |
| integer order id | 1.2 (1.2 to 1.2) | 0 |
| lookup in map[string] (100k live) | 8.3 (8.3 to 8.5) | 0 |
| lookup in map[uint64] (100k live) | 5.5 (5.5 to 5.6) | 0 |
| event boxed in an interface (escapes) | 9.2 (9.2 to 9.2) | 1 |
| event as a concrete struct | 1.2 (1.2 to 1.2) | 0 |

**Experiment 8: encoding the events of one command for the Kafka log** (batches of a realistic workload)

| Encoder | median ns/op (min to max of 5) | allocs/op |
|---|---|---|
| new messages, proto.Marshal | 201.8 (200.7 to 202.9) | 4 |
| new messages, reused byte buffer (the publisher before phase 8) | 205.6 (203.8 to 208.0) | 4 |
| reused messages and buffer (the publisher now) | 159.5 (158.9 to 161.1) | 1 |

What they say:
- **1, price levels.** The sorted slice the engine uses is the fastest up to about 1,000 levels per side (14 to 49 ns against 36 to 79 ns for the B-tree and 44 to 75 ns for the skip list) and is the slowest at 10,000 (318 ns against about 100 ns), where inserting in the middle copies a large slice. Real books in the workloads used here have tens to about a thousand levels per side (the deep scenario: about 1,000), so the slice stays; with a book of ten thousand levels a B-tree would be the better choice. The crossover is between 1,000 and 10,000 levels and was not located more precisely.
- **2, order queue.** The intrusive list in a slab is about 8 times faster than `container/list` (2.4 against 18.9 ns) and the only one that never allocates and never touches pointers the GC must scan; a `sync.Pool` of nodes removes the allocation but is still about 4 times slower than the slab.
- **3 and 5, hand-off.** A surprise: in isolation the buffered channel beats the lock-free ring here (21.6 against 72.7 ns for one producer; 30 against 210 ns for four producers), and padding the cursors onto separate cache lines made the ring slower (87 ns) until each side also cached the other's cursor (53.9 ns). The MPSC ring's cost is a CAS on one shared counter per push, and this benchmark has the producers retry at once when the ring is full, which the gateway does not (it refuses the request instead); that exaggerates the contention. The cursor layout only matters when the queue is saturated, which it never is in the gateway, so I did not change `internal/ring`. Round trip: the spinning ring is about as fast as channels (212 against 186 ns) and a ring that yields while idle is 6 times slower (1,100 ns). Struct layout: the engine's order is 48 bytes; the same fields in a careless order take 56 bytes, a cache-line-padded one 64 (`TestOrderLayoutSizes`).
- **4, hot-path costs.** `time.Now` costs 26 ns and `time.Since` 12 ns where the matcher takes the sequencer's number for 0.5 ns; `fmt.Sprintf` for an id 29 ns and 2 allocations against 1.2 ns for an integer; a `map[string]` lookup 8.3 ns against 5.5 ns for `map[uint64]`; boxing an event in an interface 9 ns and 1 allocation against 1.2 ns. All are what the design already avoids; nothing was changed because of them.
- **8, protobuf encoding.** Reusing the `EventBatch` messages as well as the buffer takes the encoding of one command's events from 201 ns and 4 allocations to 159 ns and 1 (the allocation left is the copy the Kafka client needs). Adopted in the publisher (`eventlog.FillProtoBatch`). It runs on the publisher goroutine, off the engine's path, and saves about 40 ns per command: a real but small change. The gateway's own gRPC encoding is inside grpc-go; the CPU profile (8.7) shows protobuf below the 0.5% cut-off.

### 8.3 Experiment 6: GC settings (gateway, 3 runs each)

| tag | run | p50 us | p90 us | p99 us | p99.9 us | max us | GC cycles | publish lag p50 / p99 (ms, bucket bound) |
|---|---|---|---|---|---|---|---|---|
| p8-baseline | 1 | 77 | 125 | 194 | 644 | 3243 | 26 | 3.20 / 12.80 |
| p8-baseline | 2 | 73 | 120 | 183 | 544 | 3417 | 26 | 3.20 / 6.40 |
| p8-baseline | 3 | 76 | 127 | 305 | 6183 | 18191 | 26 | 3.20 / 6.40 |
| p8-gogc400 | 1 | 77 | 130 | 545 | 10783 | 26111 | 7 | 3.20 / 6.40 |
| p8-gogc400 | 2 | 76 | 125 | 214 | 2517 | 18751 | 7 | 3.20 / 6.40 |
| p8-gogc400 | 3 | 76 | 125 | 213 | 1167 | 7999 | 7 | 3.20 / 6.40 |
| p8-gc-off-limit | 1 | 77 | 127 | 330 | 3189 | 7211 | 1 | 3.20 / 6.40 |
| p8-gc-off-limit | 2 | 78 | 128 | 306 | 6007 | 17423 | 1 | 3.20 / 6.40 |
| p8-gc-off-limit | 3 | 78 | 129 | 347 | 6075 | 11863 | 1 | 3.20 / 6.40 |

`GOGC=400` cuts the GC cycles in a run from 26 to 7 and `GOGC=off` with `GOMEMLIMIT=2GiB` to 1, with no visible effect on the median (76 to 78 µs) and no improvement of the tail (p99.9 between 0.5 and 10.8 ms in every setting, the baseline included). **Garbage collection is not what produces the tail here.** The gateway allocates little (the matcher none); I did not reduce pointer-heavy structures further because nothing pointed at them. The default was left unchanged.

### 8.4 Experiment 3 at system level: rings or channels between gateway, engine and dispatcher

`-queue-channels` replaces the lock-free input and output rings by buffered channels of the same capacity (the consumers then block instead of spinning). Interleaved so that drift of the machine hits both (ring, channel, ring, ...), 5 pairs:

| tag | run | p50 us | p90 us | p99 us | p99.9 us | max us | GC cycles | publish lag p50 / p99 (ms, bucket bound) |
|---|---|---|---|---|---|---|---|---|
| ab-ring | 1 | 80 | 133 | 618 | 50079 | 105727 | 26 | 0.10 / 6.40 |
| ab-ring | 1 | 78 | 132 | 375 | 6623 | 16511 | 26 | 3.20 / 6.40 |
| ab-ring | 1 | 87 | 159 | 4755 | 53951 | 78911 | 26 | 3.20 / 12.80 |
| ab-ring | 1 | 80 | 135 | 444 | 11855 | 37343 | 26 | 3.20 / 25.60 |
| ab-ring | 1 | 76 | 129 | 283 | 2044 | 9903 | 26 | 3.20 / 6.40 |
| ab-chan | 1 | 63 | 127 | 1140 | 19711 | 42143 | 24 | 3.20 / 6.40 |
| ab-chan | 1 | 61 | 144 | 15431 | 208255 | 232319 | 24 | 3.20 / 12.80 |
| ab-chan | 1 | 60 | 123 | 1700 | 27471 | 47903 | 24 | 3.20 / 12.80 |
| ab-chan | 1 | 57 | 106 | 238 | 5483 | 21311 | 24 | 3.20 / 6.40 |
| ab-chan | 1 | 56 | 102 | 170 | 610 | 3115 | 24 | 3.20 / 6.40 |

CPU time of the gateway process per run (about 45 s each): rings 58, 57, 57, 56, 55 s; channels 32, 31, 31, 31, 31 s.

- **Median**: channels are faster in all five pairs (56 to 63 µs against 76 to 87 µs, about 25%).
- **CPU**: channels use about 45% less (about 0.7 against 1.25 cores on average), because the ring consumers spin and yield while idle: `runtime.Gosched` and the scheduler account for about 38% of the gateway's CPU samples in 8.7.
- **Tail**: not distinguishable. Both settings have bad runs in this batch (rings: p99.9 of 50 and 54 ms; channels: 20, 208 and 27 ms), and the best runs are channels'. Whether a parked goroutine wakes later than a spinning one under a loaded host is exactly what this machine cannot answer (8.6).
- **Decision: the rings stay the default**, the channels are an option (`-queue-channels`, same behaviour, tested). Switching would be right if a quiet machine showed the tail is no worse: the median and CPU gain is real here. I did not switch on a median-only result for a system whose targets include a tail.

### 8.5 Experiments 7 and 9: Kafka linger, and pinning the engine to a thread

Kafka producer linger against the publish lag (engine event to Kafka acknowledgement; `mx_eventlog_publish_lag_seconds`; quantiles are the upper bound of the power-of-two bucket, so they overstate by up to 2x):

| tag | run | p50 us | p90 us | p99 us | p99.9 us | max us | GC cycles | publish lag p50 / p99 (ms, bucket bound) |
|---|---|---|---|---|---|---|---|---|
| p8-linger-0 | 1 | 118 | 183 | 1606 | 20303 | 31455 | 34 | 0.80 / 12.80 |
| p8-linger-0 | 2 | 113 | 162 | 473 | 3593 | 13791 | 34 | 0.80 / 3.20 |
| p8-linger-0 | 3 | 114 | 166 | 915 | 18127 | 42655 | 34 | 0.80 / 6.40 |
| p8-linger-1ms | 1 | 82 | 130 | 312 | 26991 | 59423 | 26 | 0.80 / 3.20 |
| p8-linger-1ms | 2 | 89 | 150 | 2669 | 27055 | 56991 | 26 | 1.60 / 6.40 |
| p8-linger-1ms | 3 | 82 | 133 | 282 | 2004 | 10575 | 26 | 0.80 / 1.60 |
| p8-linger-5ms | 1 | 76 | 126 | 264 | 4431 | 13951 | 26 | 3.20 / 6.40 |
| p8-linger-5ms | 2 | 77 | 132 | 341 | 2445 | 9455 | 26 | 3.20 / 6.40 |
| p8-linger-5ms | 3 | 74 | 125 | 236 | 1837 | 13967 | 26 | 3.20 / 6.40 |
| p8-linger-20ms | 1 | 72 | 125 | 275 | 2251 | 12343 | 26 | 12.80 / 25.60 |
| p8-linger-20ms | 2 | 71 | 121 | 247 | 1916 | 6099 | 26 | 12.80 / 25.60 |
| p8-linger-20ms | 3 | 75 | 132 | 539 | 63551 | 106367 | 26 | 12.80 / 25.60 |

- Lag follows linger: p50 of 0.8, 0.8, 3.2 and 12.8 ms (bucket bounds) for 0, 1, 5 and 20 ms. The default of 5 ms costs about 3 ms of event-log lag for the lowest CPU use.
- `linger=0` sends every batch alone: client p50 rises from about 76 to 114 µs and the GC cycles from 26 to 34, with the lowest lag. **1 ms is the cheaper way to a low lag**: lag p50 0.8 to 1.6 ms for a p50 of 82 to 89 µs. 20 ms gives the same client p50 (71 to 75 µs) and 13 to 26 ms of lag. The default stays at 5 ms; `-kafka-linger 1ms` is the setting if the lag matters more than 6 µs of median.

Pinning the engine goroutine to an OS thread (`-lock-engine-thread`, `runtime.LockOSThread`):

| tag | run | p50 us | p90 us | p99 us | p99.9 us | max us | GC cycles | publish lag p50 / p99 (ms, bucket bound) |
|---|---|---|---|---|---|---|---|---|
| p8-lock-thread | 1 | 118 | 159 | 284 | 1784 | 14583 | 26 | 3.20 / 6.40 |
| p8-lock-thread | 2 | 118 | 157 | 234 | 704 | 5591 | 26 | 3.20 / 6.40 |
| p8-lock-thread | 3 | 119 | 162 | 441 | 10055 | 24991 | 26 | 3.20 / 6.40 |
| p8-gomaxprocs4 | 1 | 77 | 125 | 289 | 3569 | 10735 | 25 | 3.20 / 6.40 |
| p8-gomaxprocs4 | 2 | 76 | 125 | 230 | 2837 | 18255 | 25 | 3.20 / 6.40 |
| p8-gomaxprocs4 | 3 | 75 | 121 | 200 | 5123 | 15703 | 25 | 3.20 / 6.40 |

Pinning **raised the median from about 76 to 118 µs** in all three runs and did not improve the tail (p99.9 0.7 to 10 ms against 0.5 to 6 ms). With the thread locked, every wake-up of the engine goroutine goes through the OS scheduler instead of the Go scheduler's run queue. Not adopted. `GOMAXPROCS=4` for the gateway (the setting phases 4 and later used for the whole test) changes nothing measurable for the gateway alone.

### 8.6 The tail: the machine, not the gateway

The p99.9 and max of phases 2 to 4 were left unexplained. A control: `TestHostJitter` runs goroutines that do nothing but read the clock in a loop (no allocation, no locks, no I/O, nothing for the GC to do) and records the longest gap between two reads. A gap of milliseconds there can only be the goroutine not running. 20 s each, GOMAXPROCS 10, same machine and time as the A/B runs (`host-jitter-*.txt`):

| Spinning goroutines | longest gap | gaps over 1 ms | over 5 ms | over 10 ms | over 50 ms |
|---|---|---|---|---|---|
| 4 | 4.4 ms | 14 | 0 | 0 | 0 |
| 10 (all cores) | 76 ms | 5,216 | 750 | 164 | 4 |

Code with nothing to garbage-collect and nothing to wait for is stalled for up to 76 ms when it uses all the cores of this machine. The gateway, the load generator and Kafka together use more than 4 cores. Together with 8.3 (turning the GC off changes nothing) and 8.5 (pinning changes nothing), this is my explanation of the tail: **scheduling on this host (shared cores, efficiency cores, a busy Docker VM), not the engine, the GC or the queues**. It is an explanation backed by a control and by three experiments that exclude the alternatives, not a proof: I have not measured the gateway on a quiet or dedicated machine, which would be the confirmation.

### 8.7 CPU profile of the gateway under load (20 s of 10,000 orders/s)

`go tool pprof -top bench/results/gateway-cpu-p8-profile-*.pprof`. Of the CPU samples: `engineLoop` 0.6%, `dispatchLoop` 0.6%, GC background workers about 1%, protobuf and `mallocgc` below the 0.5% cut-off, gRPC stream handling 8%, reads and writes (network syscalls) 19%, `runtime.Gosched` 38%, `usleep` 17%. **Matching is a rounding error; the gateway's CPU goes into waiting and waking** (the spin-then-yield loops of the engine and dispatcher and the network). That is why the channel version in 8.4 uses 45% less CPU, and where a next optimization of the system would have to look, not at the matcher.

### 8.8 Against the section 5.1 targets, and what was not done

| Target | Result |
|---|---|
| Engine p99 under 50 µs per order; 500k orders/s | met by a wide margin (phase 1; now 23M orders/s on the mixed workload) |
| Gateway p50 under 1 ms, p99 under 5 ms at 10k orders/s on localhost | met in these runs (Kafka log, no risk): p50 56 to 119 µs across all settings (about 76 µs at the default), p99 0.15 to 0.55 ms in most runs; a few runs had p99 above 1 ms (up to 15 ms) when the host stalled. With Redis risk and the ledger the figures are in phases 4 and 5 |
| Recovery of 1M events under 10 s | measured in phase 3, not repeated here |
| Zero steady-state heap allocations in the matcher | met (8.1) |

Not done: the optional high availability of FR-9 (a standby engine on the same stream); the experiments could not show where a tail improvement would come from on this machine, so the hot-standby work was not started either. The measurements that would settle open points need a quiet or dedicated machine: the tail of rings against channels (8.4) and the host explanation (8.6).

