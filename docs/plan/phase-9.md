# Plan: phase 9 (minimal UI, stretch)

- [x] `GET /api/latency` in marketdata: scrape the gateway's histogram, percentiles over the last N seconds from the difference of two scrapes (`-gateway-metrics`)
- [x] `ui/`: React + TypeScript page: depth ladder, recent trades, gateway latency tiles and sparkline, connection status; reconnect with backoff; stale detection; light and dark themes; phone width
- [x] marketdata serves the built page (`-ui-dir`); Vite dev server proxies to it
- [x] Tests: Go (tracker, quantiles, routing of the process), TypeScript (34 tests: pure functions, components with a fake WebSocket and fetch)
- [x] CI job `ui` (typecheck, tests, build); Makefile targets
- [x] Checked in a real browser against the running stack
- [x] ADR 0014, README, runbook, BENCHMARKS note
- Acceptance (FR-10): a page showing depth, trades and live latency percentiles.
- Not done: the UI in the Kubernetes deployment.
