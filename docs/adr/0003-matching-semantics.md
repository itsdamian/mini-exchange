# ADR 0003: Matching and event semantics

Status: accepted (phase 1)

- **Order types**: LIMIT (GTC rests; IOC cancels remainder) and MARKET (never rests, price ignored, remainder cancelled).
- **Execution price** is the resting (maker) order's price.
- **Self-trade prevention = cancel newest**: when the taker would match a resting order of the same account, the taker's remaining quantity is cancelled (`EvCanceled`) and matching stops. The resting order is untouched, and the remainder never rests, even for GTC. Trades already done before reaching the own order stay.
- **Event order for one place command**: `Accepted`, then zero or more `Trade`, then at most one `Canceled` (IOC / MARKET / STP remainder). A rejected command produces a single `Rejected`. A fully filled order has no separate "filled" event; its trades say it.
- **Validation order**: side/type/tif (`RejectInvalidOrder`), then price (`RejectInvalidPrice`), then quantity (`RejectInvalidQuantity`), then duplicate id of a live order (`RejectDuplicateOrderID`).
- **Cancel**: unknown id, or an id owned by another account, gives `RejectUnknownOrder` (the owner check avoids revealing that another account's order exists).
- `RejectInsufficientFunds` and `RejectRateLimited` are in the shared enum but only the risk layer (phase 4) emits them.
- `RejectInvalidOrder` and `RejectDuplicateOrderID` are additions beyond the reasons listed in FR-1. They cover malformed enums and id reuse, which the engine must handle to keep its index consistent.
