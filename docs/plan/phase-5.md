# Plan: phase 5 (ledger)

- [x] Schema (goose, embedded): settlements, entries, accounts, `apply_posting` (idempotent, one transaction); negative customer balance impossible
- [x] Postings: trade (cash, asset, optional fee) and deposit, built and checked in Go; property test that they balance
- [x] Consumer: group on the event topic, offsets committed after the transaction, records of a poll grouped into one transaction, transient errors retried forever, poison to `mx.events.<symbol>.dlq` with the reason
- [x] Reconciliation: recompute from the log, settlements against log trades, ledger invariants, Redis comparison; `-wait` for asynchronous catch-up
- [x] `cmd/ledger` (run, deposit, reconcile), loadtest `PG=` mode, ledger metrics
- [x] Tests against real PostgreSQL, Kafka, Redis: idempotency, replay twice, poison, PostgreSQL outage, whole path with outage and zero discrepancies, corruption detection
- [x] ADR 0010, BENCHMARKS (before and after grouping), README, runbook
- Acceptance: FR-6 (replay twice gives identical balances; reconciliation reports zero discrepancies after the chaos test).
