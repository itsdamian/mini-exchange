package experiments

import (
	"container/list"
	"math/rand/v2"
	"sync"
	"testing"
)

// Experiment 2: the order queue inside a price level. About 1,000 orders are
// resting; each step adds an order at the tail and cancels another one from
// anywhere in the queue (by handle), which keeps the size constant. Compared:
//
//	intrusive: orders in a preallocated slab linked by int32 indices, with a
//	           free list (what internal/engine does)
//	list:      container/list with one heap-allocated order per element
//	pooled:    a hand-written doubly linked list of *node taken from sync.Pool

type ord struct {
	id, account, price, qty int64
}

// account and price are read so the struct keeps the size of a real order (32 bytes).
var _ = ord{account: 1, price: 1}

const queueSize = 1000

type slabOrder struct {
	ord
	prev, next int32
}

type slabQueue struct {
	slab       []slabOrder
	free       int32
	head, tail int32
}

func newSlabQueue(n int) *slabQueue {
	q := &slabQueue{slab: make([]slabOrder, n), head: -1, tail: -1}
	for i := range q.slab {
		q.slab[i].next = int32(i + 1)
	}
	q.slab[n-1].next = -1
	return q
}

func (q *slabQueue) push(o ord) int32 {
	i := q.free
	q.free = q.slab[i].next
	q.slab[i] = slabOrder{ord: o, prev: q.tail, next: -1}
	if q.tail >= 0 {
		q.slab[q.tail].next = i
	} else {
		q.head = i
	}
	q.tail = i
	return i
}

func (q *slabQueue) remove(i int32) {
	o := &q.slab[i]
	if o.prev >= 0 {
		q.slab[o.prev].next = o.next
	} else {
		q.head = o.next
	}
	if o.next >= 0 {
		q.slab[o.next].prev = o.prev
	} else {
		q.tail = o.prev
	}
	o.next = q.free
	q.free = i
}

type node struct {
	ord
	prev, next *node
}

type ptrQueue struct {
	head, tail *node
	pool       *sync.Pool
}

func (q *ptrQueue) push(o ord) *node {
	var n *node
	if q.pool != nil {
		n = q.pool.Get().(*node)
	} else {
		n = &node{}
	}
	*n = node{ord: o, prev: q.tail}
	if q.tail != nil {
		q.tail.next = n
	} else {
		q.head = n
	}
	q.tail = n
	return n
}

func (q *ptrQueue) remove(n *node) {
	if n.prev != nil {
		n.prev.next = n.next
	} else {
		q.head = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	} else {
		q.tail = n.prev
	}
	if q.pool != nil {
		q.pool.Put(n)
	}
}

func BenchmarkOrderQueue(b *testing.B) {
	picks := make([]int, 1<<16)
	rng := rand.New(rand.NewPCG(5, 5))
	for i := range picks {
		picks[i] = rng.IntN(queueSize)
	}
	mask := len(picks) - 1

	b.Run("intrusive-slab", func(b *testing.B) {
		q := newSlabQueue(queueSize + 8)
		h := make([]int32, queueSize)
		for i := range h {
			h[i] = q.push(ord{id: int64(i)})
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k := picks[i&mask]
			q.remove(h[k])
			h[k] = q.push(ord{id: int64(i), qty: 1})
		}
	})
	b.Run("container-list", func(b *testing.B) {
		l := list.New()
		h := make([]*list.Element, queueSize)
		for i := range h {
			h[i] = l.PushBack(&ord{id: int64(i)})
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k := picks[i&mask]
			l.Remove(h[k])
			h[k] = l.PushBack(&ord{id: int64(i), qty: 1})
		}
	})
	b.Run("heap-nodes", func(b *testing.B) {
		q := &ptrQueue{}
		h := make([]*node, queueSize)
		for i := range h {
			h[i] = q.push(ord{id: int64(i)})
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k := picks[i&mask]
			q.remove(h[k])
			h[k] = q.push(ord{id: int64(i), qty: 1})
		}
	})
	b.Run("sync.Pool-nodes", func(b *testing.B) {
		q := &ptrQueue{pool: &sync.Pool{New: func() any { return &node{} }}}
		h := make([]*node, queueSize)
		for i := range h {
			h[i] = q.push(ord{id: int64(i)})
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k := picks[i&mask]
			q.remove(h[k])
			h[k] = q.push(ord{id: int64(i), qty: 1})
		}
	})
}
