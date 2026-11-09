# Plan: phase 3 (event log and recovery)

- [x] Engine: `ApplyEvents` (replay of recorded outcomes), `Orders`, `RestoreOrder`/`SetState`, `Clone`; property test that replay equals the live book after every command
- [x] Protobuf `EventBatch` / `Snapshot` (schema versioned), codec with validation
- [x] Publisher: asynchronous, batched, idempotent producer; bounded queue; watermark admission control; metrics (publish lag, pending, acked sequence)
- [x] Pipeline: one batch per command to the log, restore from recovered state, refusal when the log is backlogged
- [x] Follower + snapshot store (atomic files, hash verified, fallback to older)
- [x] Recovery: snapshot + replay, refusing inconsistent state (`eventlog.Recover`)
- [x] `cmd/logtool`: check, compare, gen
- [x] Integration tests with Testcontainers: recovery equals clean replay, snapshot ahead of log refused, Kafka stop and kill, engine crash and resume
- [x] Chaos scripts against real processes: engine kill -9, Kafka kill -9
- [x] Recovery-time benchmark (1M events)
- Acceptance: FR-4 (kill Kafka under load: no event lost or duplicated, checked by sequence continuity and event count) and FR-5 (engine killed mid-load: book hash equals a clean replay; recovery time recorded in BENCHMARKS.md).
