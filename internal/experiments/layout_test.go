package experiments

import (
	"testing"
	"unsafe"
)

// Experiment 5 (struct layout): the engine's resting order, 48 bytes, against the same
// fields in a careless order and a cache-line-padded version. Sizes only: they decide how
// many orders fit in a cache line and in the slab.

type orderPacked struct { // the engine's layout: 8-byte fields first, then the 4-byte links, then the 1-byte side
	id, account, price, qty uint64
	prev, next              int32
	side                    uint8
}

type orderCareless struct { // the same fields with the small ones in between
	side    uint8
	id      uint64
	prev    int32
	account uint64
	next    int32
	price   uint64
	qty     uint64
}

type orderPadded struct { // one order per 64-byte cache line
	orderPacked
	_ [64 - 48]byte
}

func TestOrderLayoutSizes(t *testing.T) {
	packed, careless, padded := unsafe.Sizeof(orderPacked{}), unsafe.Sizeof(orderCareless{}), unsafe.Sizeof(orderPadded{})
	t.Logf("packed %d bytes, careless %d bytes, padded %d bytes", packed, careless, padded)
	if packed != 48 {
		t.Fatalf("the engine's order layout is expected to be 48 bytes, got %d", packed)
	}
	if careless <= packed {
		t.Fatalf("the careless layout should be larger, got %d", careless)
	}
	if padded != 64 {
		t.Fatalf("padded layout should fill a cache line, got %d", padded)
	}
}
