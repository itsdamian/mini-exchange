# ADR 0013: What phase 8 changed, and what it deliberately did not

Status: accepted for phase 8

## Context
Requirements section 5.3 lists nine experiments, each to be a before/after entry in BENCHMARKS.md. The rule of the project is that a performance claim needs a reproducible benchmark with the environment recorded. This ADR records the decisions the measurements led to; the numbers are in BENCHMARKS.md section 8.

## Decisions

**Adopted** (measured improvement, tests pass):
- *Open-addressing id table in the engine* instead of `map[uint64]int32` (experiment 4). `Apply` is 22 to 27% faster and the deep-book allocations of phase 1 are gone. Correctness: the 1M-operation differential test against the reference book, the property tests, and a property test of the table against a Go map.
- *Reused protobuf messages in the event-log publisher* (experiment 8): 4 allocations become 1 and about 40 ns per command are saved, on the publisher goroutine. Small and low risk; the bytes are checked to be identical to the old encoder.

**Kept, because the experiment did not beat it:**
- *Sorted slice for price levels* (experiment 1): fastest up to about 1,000 levels per side. It would lose to a B-tree around ten thousand levels; the engine's configuration documents the size it is meant for.
- *Intrusive list in a preallocated slab for orders* (experiment 2): 8 times faster than `container/list` and allocation-free.
- *Rings between gateway, engine and dispatcher* (experiment 3), **but see below**.
- *The ring's plain cursors* (experiment 5): padding alone made it slower and padding with cached cursors only helps a saturated queue, which the gateway never has.
- *Default GC settings* (experiment 6): `GOGC=400` and `GOGC=off` do not change the median or the tail.
- *Default linger of 5 ms* (experiment 7): lag follows linger; 1 ms is the documented alternative.
- *No thread pinning* (experiment 9): it raised the median by about 40 µs.

**Added as options, off by default** (so the comparison can be repeated, and a better machine can decide): `-queue-channels` (channels instead of rings), `-lock-engine-thread`, `-pprof-addr` (profiling endpoints on a separate listener, never on the public ports). Each has tests or a documented use in `scripts/bench-all.sh`.

**The one decision that is not a clean win: rings against channels.** Channels were 25% faster at the median and used 45% less CPU in five interleaved pairs, because the ring consumers spin and yield while idle (the profile puts `runtime.Gosched` and the scheduler at the top). The tail could not be compared on this machine (the host alone stalls spinning code for tens of milliseconds, BENCHMARKS.md 8.6). The target of the project includes a tail, so the default stays on the rings until a quiet machine shows the channels' tail is no worse; the flag exists to run exactly that comparison. This is a judgement, not a measurement, and it could reasonably go the other way.

## Consequences and limits
- **The tail is explained, not fixed.** A control (code that only reads the clock) shows the same kind of stalls, GC and thread pinning do not change it, and the profile shows the matcher is not where time goes. The explanation is the host; the confirmation (a quiet or dedicated machine) has not been done.
- **The micro-experiments are isolated mechanisms.** Where they and the system disagree, the system measurement wins (rings against channels is an example: the isolated benchmark made the ring look 3 to 7 times slower, because it saturates the queue, while the whole gateway differed by about 25% at the median).
- **Results are one laptop, 3 to 5 runs.** Medians are stable, p99.9 and max are not. No statistical claim is made.
- **FR-9 (a hot standby engine) was not started.** It is marked optional in the requirements, and the experiments gave no reason to expect it to fit in this phase.
- **The stage-1 comparison uses the previous commit as the baseline** (`BASELINE` in the script); it keeps working only while that commit exists in the history.
