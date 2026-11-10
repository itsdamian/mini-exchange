package engine

import "math/bits"

// idTable maps order ids to slab indices. It replaces a built-in map[uint64]int32
// because Go's map occasionally allocates while it is churned with many inserts
// and deletes under a large live set (phase 1 measured about 150 to 190
// allocations per 5M orders in the deep scenario), and the matcher's steady
// state should allocate nothing.
//
// Open addressing with linear probing and backward-shift deletion (no
// tombstones, so a long-running table does not degrade). Capacity is a power of
// two, preallocated for twice the expected number of live orders and doubled
// only if that is exceeded (growing allocates, like exceeding the slab does).
// Key 0 cannot be stored in the table (it marks an empty slot) and has its own
// field.
type idTable struct {
	keys  []uint64
	vals  []int32
	mask  uint64
	shift uint
	n     int // entries in keys/vals (not counting key 0)

	hasZero bool
	zeroVal int32
}

func newIDTable(maxOrders int) *idTable {
	c := 8
	for c < 2*maxOrders {
		c <<= 1
	}
	return &idTable{keys: make([]uint64, c), vals: make([]int32, c), mask: uint64(c - 1), shift: uint(64 - bits.Len(uint(c-1)))}
}

// slot returns the home slot of k (Fibonacci hashing: sequential ids spread
// evenly instead of clustering).
func (t *idTable) slot(k uint64) uint64 { return (k * 0x9E3779B97F4A7C15) >> t.shift }

func (t *idTable) len() int {
	if t.hasZero {
		return t.n + 1
	}
	return t.n
}

func (t *idTable) get(k uint64) (int32, bool) {
	if k == 0 {
		return t.zeroVal, t.hasZero
	}
	for i := t.slot(k); ; i = (i + 1) & t.mask {
		switch t.keys[i] {
		case k:
			return t.vals[i], true
		case 0:
			return 0, false
		}
	}
}

func (t *idTable) put(k uint64, v int32) {
	if k == 0 {
		t.hasZero, t.zeroVal = true, v
		return
	}
	if 2*(t.n+1) > len(t.keys) {
		t.grow()
	}
	for i := t.slot(k); ; i = (i + 1) & t.mask {
		switch t.keys[i] {
		case k:
			t.vals[i] = v
			return
		case 0:
			t.keys[i], t.vals[i] = k, v
			t.n++
			return
		}
	}
}

func (t *idTable) del(k uint64) {
	if k == 0 {
		t.hasZero, t.zeroVal = false, 0
		return
	}
	i := t.slot(k)
	for t.keys[i] != k {
		if t.keys[i] == 0 {
			return
		}
		i = (i + 1) & t.mask
	}
	// Backward-shift: pull later entries of the same probe run into the hole.
	j := i
	for {
		j = (j + 1) & t.mask
		if t.keys[j] == 0 {
			break
		}
		home := t.slot(t.keys[j])
		// Move j into the hole i unless its home lies cyclically in (i, j].
		if (j-home)&t.mask >= (j-i)&t.mask {
			t.keys[i], t.vals[i] = t.keys[j], t.vals[j]
			i = j
		}
	}
	t.keys[i], t.vals[i] = 0, 0
	t.n--
}

func (t *idTable) grow() {
	old := *t
	c := 2 * len(old.keys)
	t.keys, t.vals = make([]uint64, c), make([]int32, c)
	t.mask, t.shift, t.n = uint64(c-1), uint(64-bits.Len(uint(c-1))), 0
	for i, k := range old.keys {
		if k != 0 {
			t.put(k, old.vals[i])
		}
	}
}
