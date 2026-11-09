# ADR 0010: Double-entry ledger in PostgreSQL

Status: accepted for phase 5

## Context
FR-6: every trade produces balanced entries; the consumer is idempotent (replaying the topic gives the same balances); a bad record goes to a dead-letter topic without blocking the partition; a reconciliation job compares the ledger with Redis and with a recomputation from the event log; invariants hold (entries sum to zero per asset, no negative customer balance, exactly one settlement per trade).

## Decision

**Schema (goose migrations, embedded in the binary).** `settlements` (one row per settled event, primary key `event_id`), `entries` (the lines: `entry_kind`, account, asset, signed `delta`), `accounts` (running balance per `(acct_kind, owner, asset)`). `delta` is the effect on the account's balance: a debit is negative, the matching credit positive. Account kinds: `customer`, `fee`, `external`. `external` is the counterparty of deposits, so its balance is minus everything deposited and the sum over all accounts of an asset is zero. A table check makes a negative customer balance impossible to commit.

**Migrations with goose, not Flyway.** The ledger is a Go binary; goose embeds the SQL and runs from the same process (no JVM, no second image). `cube_test` uses Flyway because it is a Spring Boot project; there is no shared database, so there is nothing to keep consistent.

**Postings.** A trade at price `p` and quantity `q` moves `q*p` quote from buyer to seller and `q` base from seller to buyer, at the maker's resting price (what Redis settles at too). An optional fee (basis points of the notional, floor, in the quote asset) is charged to each side, taker and maker rates separately, and credited to the fee account. Fees default to 0. Entries are built in Go (`TradePosting`), checked to balance before they are written, and the same function is used by the reconciliation recomputation, so the two cannot drift apart.

**Idempotency.** The key is the event id `<symbol>:<engine seq>` (unique per trade because the engine sequence is). `apply_posting`, a PL/pgSQL function, inserts the settlement with `ON CONFLICT (event_id) DO NOTHING`; if the row already exists it returns false and does nothing else, otherwise it writes the entries and updates the balances in the same transaction. Applying the same events again, in any number of passes, changes nothing: that is what makes at-least-once delivery an exactly-once effect. The function exists because a `CHECK` is evaluated on the proposed row of an `INSERT .. ON CONFLICT` before the conflict is resolved, so a negative delta cannot be written as a plain upsert; balance rows are created at zero and then updated in key order (which also keeps two writers from deadlocking).

**Consumer.** One consumer group per symbol reads the single-partition event topic from the start. Offsets are committed after the database transaction, never before. A poll's records are decoded and the trades of up to 256 records are applied in one transaction (one round trip of statements), so the cost of a commit is shared; if that transaction is refused as poison, the records are re-applied one by one so only the bad record is dead-lettered. A record is atomic: all trades of an engine command settle together or not at all, like the log itself.

**Two kinds of failure, two behaviours.**
- Transient (database down, connection reset, timeout): retried forever with backoff, offset not committed. Nothing is skipped and nothing is applied twice. `mx_ledger_retries_total` counts it.
- Poison (a record that can never be applied): published to `mx.events.<symbol>.dlq` with the original bytes and headers `reason`, `source-topic`, `source-partition`, `source-offset`, and the consumer moves on. Poison is: bytes that do not decode, an unknown schema version, a batch for another symbol, a malformed trade (non-positive price or quantity, invalid account, overflow), and a database integrity or data error (class 23/22), notably a trade that would make a customer balance negative. The reason is the error text, so an operator can tell them apart.

**Reconciliation (`ledger reconcile`).** Reads the whole event log and compares:
1. Recomputation: every trade in the log is posted again in memory, deposits (which are not in the log) are added from the ledger, and the result is compared with the ledger balances per account and asset.
2. Settlements against the log: every trade in the log has exactly one settlement, and no settlement exists for a trade that is not in the log.
3. Internal invariants: each balance equals the sum of its entries; each event's entries sum to zero per asset; no settlement without entries; the balances of an asset sum to zero; no negative customer balance.
4. Redis: for every customer account in the ledger, Redis available plus held equals the ledger balance (for the quote asset plus the fees the ledger took, since the risk layer does not charge fees).
The log is also checked for sequence gaps. It exits non-zero and prints a JSON report on any discrepancy. The ledger and Redis settle asynchronously behind the engine, so `-wait` repeats the check until it is clean or the time is up and reports the last attempt.

## Consequences and limits
- **Fees are not reserved by risk.** Redis holds `qty*price` and nothing for fees, so with a fee above zero a buyer who spends everything cannot pay the fee: the ledger refuses the trade (negative balance), it is dead-lettered and shows up as `missing_settlement`. The trade did happen in the engine. Fixing it means holding the worst-case fee in the reservation (a phase 4 change); until then fees default to 0 and the end-to-end tests keep balances large.
- **Deposits are not in the event log**, so the recomputation trusts the ledger's own deposit entries. Redis is funded separately (`risktool fund`), and the reconciliation against Redis is what catches the two being funded differently. There is no deposit flow beyond `ledger deposit`.
- **The reconciliation scans the whole log** and keeps the trade sequence list in memory. That is fine up to millions of trades and is not incremental; a production version would reconcile from a checkpoint.
- **Redis accounts are those the ledger knows.** An account funded in Redis that never traded and was never deposited in the ledger is not compared (Redis cannot be enumerated cheaply, and the scan of all keys is what `risktool audit` is for).
- **One symbol per ledger consumer**, like the rest of the system; accounts hold one base and one quote asset per symbol (`BASE-QUOTE`).
- **A poison record stops the log from being recomputed** (`eventlog.Scan` fails on a record it cannot decode, as recovery does); that is deliberate, the log is the system of record and an undecodable record in it is an incident, not noise. The dead-letter topic is for what the ledger refuses, and the offending bytes stay in the log.
- **Ledger lag is a metric, not a guarantee.** `mx_ledger_settle_lag_seconds` measures engine event to commit; the engine does not wait for the ledger. See BENCHMARKS.md.
