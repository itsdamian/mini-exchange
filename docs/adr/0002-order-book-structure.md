# ADR 0002: Order book data structure

Status: accepted for phase 1; revisited by the phase 8 experiments (BENCHMARKS.md)

## Decision
- Price levels per side live in a slice sorted so the **best level is the last element** (bids ascending, asks descending). Taking the best level, and removing it when it empties, is O(1); locating a level is a binary search; inserting or removing a middle level is a `copy`.
- Orders live in a preallocated slab (`[]order`) and are addressed by `int32` index. Orders of one level form an intrusive doubly linked list through `prev`/`next` indices (FIFO = time priority; O(1) cancel from the middle). Free slots form a free list through `next`. No per-order heap allocation, no pointers for the GC to scan.
- `map[uint64]int32` (integer key, preallocated to `MaxOrders`) maps order id to slab slot.

## Why
A sorted slice is the simplest structure that is cache friendly and has no allocation in steady state. Real books are concentrated near the touch, so the insert `copy` is short. Alternatives (B-tree, skip list) are scheduled as before/after experiments in phase 8; this ADR is the baseline they are compared with.

## Known limits
Growing past `Config.MaxOrders` / `MaxLevels` allocates (slab doubles, level slices grow). Size them for peak load.
