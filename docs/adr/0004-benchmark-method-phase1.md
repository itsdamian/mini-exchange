# ADR 0004: How phase 1 engine numbers are measured

Status: accepted (phase 1)

- Two tools: `go test -bench -benchmem` (ns/op, allocs/op) and `bench/enginebench` (per-order latency in an HdrHistogram, several runs, JSON result with environment).
- The command stream is generated up front from a seed (`internal/enginetest`) and replayed; one unmeasured warm-up lap precedes measurement.
- This is a **service-time** measurement of the matcher driven back-to-back, with no arrival process, so it says nothing about queueing. The open-loop, fixed-arrival-rate method (to avoid coordinated omission) applies to the gateway benchmark in phase 2.
- Timing each call with `time.Now`/`time.Since` costs time itself, and on Apple Silicon macOS the clock ticks in ~41.7 ns steps (24 MHz counter). Latencies of tens of nanoseconds are therefore quantised and include timer cost; `enginebench` measures and prints the timer overhead next to the results. The throughput figures (orders/s) come from total elapsed time and are not affected by per-call timing in `go test -bench`.
