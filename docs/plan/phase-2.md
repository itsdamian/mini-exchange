# Plan: phase 2 (gateway)

- [x] Protobuf API in `proto/exchange/v1`, managed with `buf` (lint, format, breaking check and generated-code drift check in CI)
- [x] SPSC and MPSC lock-free bounded rings (`internal/ring`), race-tested
- [x] Pipeline: MPSC input ring, engine goroutine, SPSC output ring, dispatcher, report hub (`internal/pipeline`)
- [x] gRPC server: PlaceOrder, CancelOrder, StreamExecutionReports; validation; idempotency; backpressure (`internal/gateway`)
- [x] Prometheus histograms: gateway request latency, engine queue wait, match time
- [x] Open-loop load generator with HdrHistogram output (`cmd/loadgen`, `scripts/loadtest.sh`)
- [x] Fuzz test for request validation
- Acceptance FR-2: 100 concurrent retries observe exactly one order (`TestIdempotencyConcurrentRetriesCreateExactlyOneOrder`); end-to-end latency histogram recorded in `BENCHMARKS.md`.
