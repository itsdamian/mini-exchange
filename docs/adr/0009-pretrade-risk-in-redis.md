# ADR 0009: Pre-trade risk in Redis

Status: accepted for phase 4

## Context
FR-3: balances live in Redis; placing an order atomically reserves funds with a Lua script; cancel or fill releases or consumes the reservation; limits are max order size, max open orders per account and a token-bucket rate limit per account. The matcher must stay free of I/O.

## Decision

**Where the check runs.** In the gateway, before the order enters the engine ring (`Risk.Reserve`). The matcher never talks to Redis. One Redis round trip is added to the request path: that is the cost of a pre-trade check and is measured in BENCHMARKS.md.

**What is reserved.** A buy holds `qty * price` of the quote asset; a sell holds `qty` of the base asset. A MARKET buy has no price, so it holds `qty * MaxPrice`, where `MaxPrice` is also the largest limit price the gateway accepts. Because no resting order can be above it, it is a true upper bound on every execution price, so a fill can never cost more than was reserved. The excess is released as the order fills or ends. The consequence is that a market buy locks a lot of quote for the instant it lives; keep `-risk-max-price` close to the real price range.

**Balances.** Per account, one hash `{id}:bal` with `b_avail, b_held, q_avail, q_held, open` (and the token bucket). All values are integers. Redis Lua numbers are doubles, exact below 2^53, so the design bounds every balance by `MaxBalance = 2^52` and requires `MaxOrderQty * MaxPrice <= 2^52`; settlement conserves totals, so sums cannot grow past what was deposited. Costs are computed in Go with that bound, never in Lua. One symbol per instance means one base and one quote balance per account; a multi-symbol design would need per-asset balances.

**Reserve script (atomic).** In order: take a rate-limit token (a limited attempt still spends its token), check open orders, check available funds, then move funds from available to held and count the order. It returns a typed reason. Static checks (quantity and price ranges, max order size) are done in Go first and never touch Redis. A repeated `client_order_id` finds its reservation and does not hold twice.

**Settlement is asynchronous and idempotent.** The pipeline's dispatcher (not the matcher) turns each engine event into an op and hands it to a bounded queue; one worker applies them to Redis in event order:
- Accepted: bind the reservation from the client order id to the engine order id.
- Trade: each side consumes part of its reservation. A buyer spends `qty * fill price` and the difference to its reserved price is released.
- Canceled (user cancel, IOC remainder, market remainder, self-trade prevention): release what is still held.
- Rejected by the engine: release the whole reservation.

Every op carries the engine event sequence. Per account and symbol Redis remembers the last sequence applied and skips anything not above it, so the worker can safely retry a batch after a timeout when it does not know whether Redis applied it. Ops for one account in a batch go in one Lua call (see below). The gateway's own release for an order refused before the engine (queue full, shutting down) is idempotent because the reservation is deleted.

**Failure behaviour.**
- Redis down or too slow: the order is refused with `UNAVAILABLE` (fail closed). Nothing is cached for the key, so the client can retry. The call is bounded by `-risk-timeout` (default 100 ms).
- Settlement behind: new orders are refused with `RESOURCE_EXHAUSTED` once the queue passes its high watermark or while Redis is failing. If the queue ever overflows, the op is dropped, counted (`mx_risk_settle_dropped_total`) and logged loudly, and the balances need reconciliation.
- Risk rejections are returned as typed rejects (`INSUFFICIENT_FUNDS`, `RATE_LIMITED`, `ORDER_TOO_LARGE`, `TOO_MANY_OPEN_ORDERS`, plus `INVALID_PRICE` / `INVALID_QUANTITY` for out-of-range values). They are decided before the engine, so they are **not in the event log**, and they are **not cached** by the idempotency store: no order exists, so a retry with the same key is evaluated again (a rate-limited or underfunded client may succeed later) and can still never create two orders.

**Why one Lua call per account per batch.** The first version applied one script call per op. Measured on the compose Redis at 10k orders/s that was about 22k script calls/s at about 28 µs of Redis time each, and Redis could not keep up: reserve calls took 35 ms on average and 7.7% of orders in the first run were refused as `UNAVAILABLE` after the 100 ms timeout. Grouping a batch's ops per account cut the settlement calls from one per op to about one per 1.9 ops at this load (calls per run 665k to 514k; more grouping when a backlog builds up), and the p50 over three runs went from 639 to 3,623 µs to 519 to 558 µs, but the tail stayed bad on that setup (BENCHMARKS.md has both). So this is a measured improvement of load, not a fix of the tail; the remaining tail is attributed to the environment, see BENCHMARKS.md. The settle script builds the reservation keys itself from the `{id}` prefix instead of declaring them as KEYS; this is fine on a single node and stays inside one slot with the hash tag, but would need review before use on Redis Cluster.

## Known limitations (stated, not hidden)
- **Redis and the engine can diverge on a crash.** Ops that were queued but not yet applied when the gateway dies are lost, so Redis can still hold funds for orders the recovered book has already filled or cancelled. Because ops are keyed by engine order id and carry sequences, replaying the log after a restart could repair this for orders whose bind was applied, but the bind needs the client order id, which is not in the event log (the same gap as the idempotency table, ADR 0006/0008). Closing both means writing `client_order_id` into the log. Not done in this phase; reconciliation (phase 5) will detect the drift.
- **Orders reserved but never accepted** (the gateway died between Reserve and the engine) keep their funds held under the client-id key. No expiry yet.
- **The event sequence restarts at 1 for a new topic**, but Redis remembers the last applied sequence per symbol. Running `-redis` against a fresh log without clearing the risk keys would make settlement skip ops. `loadtest.sh` clears them; the gateway warns when `-redis` is given without `-kafka`.
- **Deposits** are an admin operation (`risktool fund`); there is no deposit flow in the spec.
- Redis durability is the operator's choice: the compose file turns on AOF with `appendfsync everysec`, which can lose about one second of writes on a crash.
- The rate limit takes its clock from the gateway, so several gateways with skewed clocks would skew refill; one gateway per symbol is assumed.

## Alternatives rejected
- **Reserving inside the engine.** Would put balance state and I/O or a second source of truth in the matcher. Rejected by the hot-path rule.
- **Synchronous settlement in the dispatcher.** A Redis stall would stall every execution report and the output ring, and with it the engine.
- **A pessimistic "reserve everything" market buy.** Simpler, but then a market buy that only needs a little would lock the account's whole balance and could be refused when a smaller exposure would have been fine. Reserving `qty * MaxPrice` bounds the lock by the order itself.
