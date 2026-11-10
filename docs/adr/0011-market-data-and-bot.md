# ADR 0011: Market data feed and synthetic order flow

Status: accepted for phase 6

## Context
FR-7: a WebSocket feed with top-N depth and recent trades, conflated to at most one push per 100 ms per client, slow clients disconnected rather than buffered, and no effect on engine latency with 1,000 clients. FR-8: a bot that quotes around a reference price and sends random taker orders, with the reference price from a recorded file (default, deterministic) or the live Coinbase WebSocket, and identical event streams from two runs with the same file and seed.

## Decision

**Market data is a separate process that only reads Kafka.** `cmd/marketdata` rebuilds the book with the same recovery the engine uses (`eventlog.Recover` with no snapshot, so the view is exact, not an approximation from order events), then follows the log from the next offset. The engine and gateway never see a market data client, so a client cannot slow the matcher by construction; the load test measures that this also holds in practice on one machine. If an event does not apply to the book (a gap, an inconsistent event) the consumer stops and the process exits: a wrong view is worse than none. Depth is read from the rebuilt book (`Book.Depth`, a read-only method added to the engine; the matcher is not touched).

**The message is the whole state, not a delta.** Each frame holds the top-N asks and bids as `[price, qty]`, the last K trades, the last engine sequence and its timestamp. A client that missed frames, because of conflation or a reconnect, needs nothing else, and there is no per-client replay state to keep.

**Conflation in two places, both bounded.** The `Feed` builds and encodes at most one frame per interval (default 100 ms) and only if something changed, and all clients share the encoded bytes, so the cost of a frame does not grow with the number of clients. Each client goroutine looks at the latest frame once per interval and writes it only if its version differs from the one it last sent: at most one frame per 100 ms per client, and the newest state, never a queue. A new client gets the current frame at once.

**Slow clients are disconnected.** A write that is not accepted within the write timeout (default 1 s) closes the connection (`mx_md_slow_disconnects_total`). Clients only listen (anything they send is discarded), and connections beyond `-max-clients` get 503. Memory per client is therefore a goroutine and the socket buffers, with no application queue.

**Library.** `github.com/coder/websocket` (the maintained successor of `nhooyr.io/websocket`, same API) rather than gorilla: context-aware reads and writes, which the write timeout and shutdown rely on.

**The bot is a deterministic planner plus a thin runner.** `Planner.Step(price)` returns the actions for one reference price: cancel the previous quotes, quote `Levels` levels per side around the mid with a fixed half-spread, and attempt a few taker orders (IOC limits priced through every quoted level, or MARKET) from random taker accounts. It uses only the config, a seeded PCG generator and the price sequence: no wall clock, no map iteration, no goroutines. The runner sends the actions one at a time and waits for each answer, so the order the engine sees is the order the planner produced; client order ids are derived from the step number. With a fixed seed and file the gateway therefore receives the same requests in the same order and the engine emits the same events; the only field that differs between runs is the event timestamp, which the sequencer takes from the wall clock, so the test compares streams with timestamps zeroed (the same exception the determinism hash of ADR 0001 makes).

**Price sources.** A file (CSV with a `price` column, or the last column; or a JSON array of numbers or `{"price": ...}` objects) is the default. `-feed coinbase` subscribes to the public ticker channel of one product and takes each ticker price as a step; it is optional and is never used by tests. The file in `data/` is **synthetic** (a seeded random walk), not a recording; `data/README.md` says so.

**Tests never touch the internet.** `internal/nonet` replaces the default HTTP transport's dialer so that any dial to a non-loopback host fails immediately with `ErrExternal`; the bot tests install it in `TestMain` and one test asserts that dialing the real Coinbase URL is refused. The Coinbase source is tested against a fake WebSocket server.

## Consequences and limits
- **Depth is as consistent as the log**: the view lags the engine by the Kafka publish delay plus up to one conflation interval, and `mx_md_publish_lag_seconds` reports it. It is not a quote you could trade against with the engine's timing.
- **Startup replays the whole log** (no snapshot), so a restart of marketdata takes as long as a clean recovery (BENCHMARKS phase 3). Reusing the snapshot store would shorten it; not done.
- **One symbol per marketdata process**, like the gateway.
- **Frame size is the cost of the full-state design**: with the default 10 levels and 50 trades a frame is a few KB, times clients times 10 per second. At 1,000 clients that is tens of MB/s over loopback. Deltas would cut it and add per-client state; I chose the simpler protocol.
- **The bot is not a realistic market simulation.** It quotes a fixed spread around the reference and trades at random; fills, inventory and risk limits are not modelled, and with risk enabled the accounts must be funded for what it sends.
- **The recorded-feed path is only as realistic as the file.** The shipped file is synthetic.
- **`cmd/wsload`** exists only to run the 1,000-client acceptance test; it is not a general client.
