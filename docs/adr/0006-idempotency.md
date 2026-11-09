# ADR 0006: Idempotency of PlaceOrder and CancelOrder

Status: accepted for phase 2; durability revisited in phase 3

## Decision
- The key is `(account_id, client_order_id)`. `client_order_id` is mandatory: 1 to 64 printable ASCII characters.
- Single-flight: the first request with a key executes; every duplicate, including duplicates arriving *while the first is in flight*, waits for and returns the same outcome. 100 concurrent retries create exactly one order (integration test).
- The outcome includes engine-level rejects. A retry of a rejected order returns the original rejection even if the retry's fields are now valid. The key identifies the request, not its parameters; a retry with different parameters gets the original result.
- Once a request has been enqueued, the engine call is detached from the caller's cancellation, so the outcome is always recorded and a retry after a client timeout returns the real result instead of hanging.
- If nothing was enqueued (overload, shutdown), the key is forgotten so the client can retry it.
- Cancel is identified by the `client_order_id` of the order. A cancel retry returns the original cancel outcome. A cancel for a key that is not (yet) a known accepted order returns `UNKNOWN_ORDER` without being cached.
- An engine order id is never exposed across accounts: a cancel for another account's key looks up under the caller's own account and finds nothing.

## Known limitation
The idempotency records live in process memory and grow without bound. They do not survive a restart. A durable, expiring idempotency record belongs with the event log and recovery work (phase 3) and is not claimed here.
