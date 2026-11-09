# ADR 0005: Gateway to engine pipeline

Status: accepted (phase 2)

## Context
Many gRPC handler goroutines must feed one engine goroutine, and the matcher hot path may not use locks, channels or I/O (requirements section 3.2). Request completion and execution report fan-out need channels and locks.

## Decision
```
handlers --Submit--> MPSC input ring --> engine goroutine (owns the Book)
                                             |
                                     SPSC output ring
                                             |
                                  dispatcher goroutine
                              (completes requests, fans out reports)
```
- **Input**: a bounded multi-producer single-consumer ring (Vyukov per-slot sequence queue). Handlers never block on it: when it is full, or the in-flight limit is reached, `Submit` returns `ErrOverloaded` and the gateway answers `RESOURCE_EXHAUSTED`. This is the explicit backpressure of FR-4, applied at the gateway door. A single-producer queue would need an extra sequencer goroutine and hop; MPSC avoids it.
- **Engine goroutine**: pop, stamp time (`cmd.Ts`, from a monotonic clock plus a base, taken *outside* `Book.Apply`), assign the order id for places, `Book.Apply`, push events to the output ring. Nothing else. If the output ring is full it yields and retries; events are never dropped.
- **Dispatcher**: the only place with locks and channels. It completes the waiting request (a pooled ticket with a one-slot channel, tagged on the first event of a command), tracks live orders to label reports with `client_order_id` and leaves quantity, and fans reports out to subscribers.
- **Execution reports come from the output ring**, never from Kafka (target architecture).
- **Idle strategy**: spin, then `Gosched`, then park on a channel with a flag-and-recheck protocol that cannot lose a wakeup. This keeps latency low under load and CPU near zero when idle.
- **Slow report subscribers** are disconnected with `RESOURCE_EXHAUSTED` when their buffer overflows. Reports are never silently dropped.

## Consequences
- The rings are plain (no cache-line padding, no cached cursors): the baseline for the phase 8 layout experiments, together with a buffered-channel comparison.
- Two extra goroutine hops (handler to engine, engine to dispatcher) cost latency versus a direct call; measured in BENCHMARKS.md.
- A `Submit` racing with `Stop` after the input ring has drained could wait forever. The gateway stops gRPC (no new handlers) before stopping the pipeline, which closes that window.
