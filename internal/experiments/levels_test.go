package experiments

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/google/btree"
)

// Experiment 1: the price-level index. Three structures hold the levels of one
// side of the book: the engine's sorted slice, a B-tree (google/btree, degree
// 32) and a skip list. The workload keeps about `depth` levels alive: each step
// finds or creates the level at a price and, half of the time, removes a level
// (the book empties levels as orders trade or cancel). 80% of the touched
// prices are within the best five levels (activity is near the touch), 20% are
// uniform over the whole depth.
//
// This measures only the level index. The engine's order queues, matching and
// event emission are not part of it.

func levelPrices(depth, n int) (prices []int64, removes []bool) {
	rng := rand.New(rand.NewPCG(7, 7))
	prices = make([]int64, n)
	removes = make([]bool, n)
	for i := range prices {
		if rng.IntN(5) < 4 {
			prices[i] = int64(depth - rng.IntN(5)) // near the best (highest) price
		} else {
			prices[i] = int64(1 + rng.IntN(depth))
		}
		removes[i] = rng.IntN(2) == 0
	}
	return
}

func BenchmarkLevelIndex(b *testing.B) {
	for _, depth := range []int{10, 100, 1000, 10000} {
		prices, removes := levelPrices(depth, 1<<16)
		mask := len(prices) - 1

		b.Run(fmt.Sprintf("sortedslice/depth=%d", depth), func(b *testing.B) {
			s := &sliceSide{lv: make([]Level, 0, 2*depth)}
			for p := 1; p <= depth; p++ {
				s.findOrInsert(int64(p))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				l := s.findOrInsert(prices[i&mask])
				l.Qty++
				if removes[i&mask] {
					s.remove(prices[(i+1)&mask])
				}
			}
		})
		b.Run(fmt.Sprintf("btree/depth=%d", depth), func(b *testing.B) {
			t := btree.NewG(32, func(a, b Level) bool { return a.Price < b.Price })
			for p := 1; p <= depth; p++ {
				t.ReplaceOrInsert(Level{Price: int64(p), Head: -1, Tail: -1})
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				p := prices[i&mask]
				l, ok := t.Get(Level{Price: p})
				if !ok {
					l = Level{Price: p, Head: -1, Tail: -1}
				}
				l.Qty++
				t.ReplaceOrInsert(l)
				if removes[i&mask] {
					t.Delete(Level{Price: prices[(i+1)&mask]})
				}
			}
		})
		b.Run(fmt.Sprintf("skiplist/depth=%d", depth), func(b *testing.B) {
			s := newSkipList()
			for p := 1; p <= depth; p++ {
				s.findOrInsert(int64(p))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				l := s.findOrInsert(prices[i&mask])
				l.Qty++
				if removes[i&mask] {
					s.remove(prices[(i+1)&mask])
				}
			}
		})
	}
}

// All three must agree with a plain map on what they hold.
func TestLevelStructuresAgree(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	sl, sk := &sliceSide{}, newSkipList()
	bt := btree.NewG(8, func(a, b Level) bool { return a.Price < b.Price })
	ref := map[int64]bool{}
	for i := 0; i < 20000; i++ {
		p := int64(1 + rng.IntN(300))
		if rng.IntN(3) == 0 {
			sl.remove(p)
			sk.remove(p)
			bt.Delete(Level{Price: p})
			delete(ref, p)
			continue
		}
		sl.findOrInsert(p)
		sk.findOrInsert(p)
		bt.ReplaceOrInsert(Level{Price: p, Head: -1, Tail: -1})
		ref[p] = true
	}
	if len(sl.lv) != len(ref) || bt.Len() != len(ref) {
		t.Fatalf("sizes: slice %d btree %d map %d", len(sl.lv), bt.Len(), len(ref))
	}
	n := 0
	for x := sk.head.next[0]; x != nil; x = x.next[0] {
		n++
		if !ref[x.lv.Price] {
			t.Fatalf("skip list holds %d which the map does not", x.lv.Price)
		}
	}
	if n != len(ref) {
		t.Fatalf("skip list has %d levels, map %d", n, len(ref))
	}
}
