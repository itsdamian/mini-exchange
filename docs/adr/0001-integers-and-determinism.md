# ADR 0001: Integer money and a deterministic matcher

Status: accepted (phase 1)

## Context
The event stream is meant to be the system of record: state = snapshot + replay.
That only works if replaying the same commands always yields byte-identical events.

## Decision
- Prices are `int64` ticks and quantities are `int64` units. There is no floating point in the engine or ledger.
- The matcher reads no wall clock. Every `Command` carries a timestamp assigned by the sequencer; events copy it.
- The matcher never iterates a Go map. The `id -> slot` map is used only for point lookups; all ordering comes from the price-sorted level slices and per-level FIFO lists.
- Event sequence numbers are assigned by the book, contiguous, starting at 1.
- `engine.HashEvent` / `Book.Hash` provide the determinism and state-equality checks used by tests (and later by recovery).

## Consequences
Determinism is enforced by property tests (same input => same output hash and same book hash) and by the differential test against the reference implementation.
