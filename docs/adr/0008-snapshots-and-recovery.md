# ADR 0008: Snapshots and recovery

Status: accepted (phase 3)

## Decisions
- **A snapshot may only contain events Kafka has acknowledged.** A `Follower` keeps a second copy of the book built solely from acknowledged batches and snapshots that copy. A snapshot taken from the engine's own book could be ahead of the log (it would include events still in flight); after a crash that state would be unrecoverable from the log. With the follower a snapshot can never be ahead of the log, and recovery treats a snapshot that is ahead as corruption and refuses to start.
- **Snapshots never touch the matcher.** The follower copies its book under its own lock and writes the file on another goroutine. The cost is a second book in memory and a second goroutine applying events. (Applying acknowledged events in order also exercises the replay path continuously.)
- **Snapshot contents**: resting orders in priority order (the same order `Book.Hash` uses), last sequence, the Kafka offset of the record holding it, highest order id, and the book hash. The hash is verified on load; a mismatch, an unknown schema version or the wrong symbol makes the snapshot unusable and recovery falls back to an older one (the newest three are kept) or to a clean replay.
- **Files**, not Kafka: written atomically (temp file, fsync, rename, directory fsync). For a standby engine (FR-9) a shared store would be needed; not required now.
- **When**: every `-snapshot-every` acknowledged events (default 100,000), and once more at graceful shutdown so a clean restart replays nothing.
- **Recovery** (`eventlog.Recover`): load the newest valid snapshot, read the log end offset, replay every record after the snapshot's offset up to that end. It refuses to continue, rather than guess, if the snapshot is ahead of the log, the log was truncated before the point it needs, a sequence number has a gap or a duplicate, or an event does not apply to the current book.
- The order id counter resumes at the highest id in the log plus one, and sequence numbers resume at the last durable sequence plus one.
- `logtool compare` is the acceptance check: it recovers once from the newest snapshot and once from a clean replay of the whole log and requires the same book hash and the same last sequence.

## What recovery does not cover
Recovery rebuilds the matching state only. Gateway-side state (idempotency table, report subscriptions) starts empty after a restart; see ADR 0007.
