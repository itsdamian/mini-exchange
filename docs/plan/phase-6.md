# Plan: phase 6 (market data and bot)

- [x] Engine: read-only `Book.Depth` (matcher untouched)
- [x] `internal/marketdata`: Feed (book rebuilt from the log, shared encoded frame, recent trades), WebSocket server (conflation, slow client disconnect, client limit, metrics), Kafka consumer (recover, then follow)
- [x] `internal/bot`: price sources (CSV/JSON file, Coinbase ticker), deterministic planner, sequential runner
- [x] `internal/nonet`: guard that fails tests dialing external hosts
- [x] `cmd/marketdata`, `cmd/bot`, `cmd/wsload`; `loadtest.sh` `WS=N` mode (marketdata + N clients during the load)
- [x] Tests: depth equals the reference book, conflation (at most one frame per 100 ms), 1,000 clients with flat memory and a dropped slow client, client limit, consumer rebuilds then follows the log on real Kafka; bot: file formats, planner determinism, identical event streams through the real gateway for the same file and seed, Coinbase source against a fake server, external-host guard
- [x] Load test A/B: gateway latency with and without 1,000 WebSocket clients (3 runs each), marketdata memory
- [x] ADR 0011, BENCHMARKS, README, runbook
- Acceptance: FR-7 (1,000 clients: stable memory, no impact on engine latency), FR-8 (same file and seed give identical event streams).
