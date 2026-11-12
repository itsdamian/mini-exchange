# ADR 0014: Minimal UI

Status: accepted for phase 9 (optional, stretch)

## Context
FR-10: a small React + TypeScript page showing depth, trades and live latency percentiles. It is optional and was only to be started after phase 8. The market data feed (ADR 0011) already carries depth and trades; latency percentiles exist only as Prometheus histograms inside the gateway.

## Decision

**Depth and trades come from the existing WebSocket feed, unchanged.** A frame is the whole state, so the page keeps no delta logic: each frame replaces the screen, a reconnect needs no resynchronisation, and a frame that is not a well-formed snapshot is ignored instead of crashing the page. Reconnection uses exponential backoff (0.5 s to 10 s); while disconnected the last book stays on screen and the header says so. A feed that sends nothing for 3 s is shown as stale.

**Latency comes from the market data process, not from the browser.** A browser cannot read the gateway's `/metrics` without CORS headers on the gateway, and it should not query Prometheus directly. Instead `cmd/marketdata` takes an optional `-gateway-metrics URL`, scrapes it once per second (`LatencyTracker`), keeps the cumulative histogram buckets of `mx_gateway_request_seconds{rpc="PlaceOrder"}` summed over status codes, and serves `GET /api/latency`: p50, p99, p99.9 and orders per second over the last N seconds from the *difference* between two scrapes (not since process start, which would hide a recent change), plus a series of 5-second windows for the sparkline. Quantiles are interpolated inside the bucket like `histogram_quantile`, so they are estimates (the buckets are powers of two). The page says what the number is: the PlaceOrder handler time inside the gateway, not what a client sees (the load test measures that, with a different method and much higher load).

**One origin.** In development Vite proxies `/ws` and `/api` to a local marketdata process; in use marketdata serves the built page itself (`-ui-dir ui/dist`). No CORS anywhere, no new service.

**Small on purpose.** React and a few hundred lines, no component library, no state library, plain CSS with light and dark themes from `prefers-color-scheme`. The production bundle is 228 kB (72 kB gzipped). Prices are shown as ticks times a price-per-tick (default 0.01, `?tick=` overrides) because the feed carries only integer ticks.

## Consequences and limits
- **Not deployed to the Kubernetes cluster.** The marketdata image does not contain the UI (the Dockerfile builds one Go binary per image and I did not add a Node stage to all of them), and the cluster manifests do not pass `-gateway-metrics` or allow marketdata to reach the gateway. Run it locally against the compose or local stack (docs/runbook.md).
- **The latency numbers are only as good as the buckets** and only exist while the gateway is scraped; when it is not, the panel says why instead of showing zeros.
- **A real bug the unit tests could not see**: the first version matched the RPC label as `/…/PlaceOrder`, the gateway labels it `PlaceOrder`, and the tests used the same wrong assumption in their fixtures. It was found by running the real stack. The fixtures now use the real labels and a test accepts both forms.
- **No authentication and no history**: anyone who can reach marketdata sees the feed; the page keeps only what is on screen and the last 2 minutes of the latency series (held by the server).
- **Tested** with unit tests of the pure functions (formatting, depth ladder, parsing), component tests with a fake WebSocket and fetch (frames, latency, stale feed, reconnect, bad frames, unmount), Go tests of the tracker and of the process's HTTP routing, and a run in a real browser against a running gateway, marketdata and bot (desktop, phone width, dark theme, marketdata killed and restarted).
