// Package ring provides preallocated, lock-free, bounded queues.
//
// Both queues have a power-of-two capacity, never grow, never allocate after
// construction, and never block: Push reports false when the queue is full so
// the caller can apply backpressure explicitly.
//
// They are deliberately plain (no cache-line padding, no cached cursors): the
// layout experiments are phase 8 work and this is their baseline.
package ring

import "sync/atomic"

func checkSize(size int) {
	if size < 2 || size&(size-1) != 0 {
		panic("ring: size must be a power of two >= 2")
	}
}

// SPSC is a single-producer single-consumer queue. Exactly one goroutine may
// call Push and exactly one (possibly different) goroutine may call Pop.
type SPSC[T any] struct {
	buf  []T
	mask uint64
	head atomic.Uint64 // next index to pop, written by the consumer
	tail atomic.Uint64 // next index to push, written by the producer
}

// NewSPSC returns a queue with the given capacity (a power of two).
func NewSPSC[T any](size int) *SPSC[T] {
	checkSize(size)
	return &SPSC[T]{buf: make([]T, size), mask: uint64(size - 1)}
}

// Push appends v, or returns false if the queue is full.
func (q *SPSC[T]) Push(v T) bool {
	t := q.tail.Load()
	if t-q.head.Load() == uint64(len(q.buf)) {
		return false
	}
	q.buf[t&q.mask] = v
	q.tail.Store(t + 1)
	return true
}

// Pop removes the oldest element, or returns false if the queue is empty.
func (q *SPSC[T]) Pop() (v T, ok bool) {
	h := q.head.Load()
	if h == q.tail.Load() {
		return v, false
	}
	v = q.buf[h&q.mask]
	q.head.Store(h + 1)
	return v, true
}

// Len returns an instantaneous element count (approximate under concurrency).
func (q *SPSC[T]) Len() int { return int(q.tail.Load() - q.head.Load()) }

type mpscSlot[T any] struct {
	seq atomic.Uint64
	val T
}

// MPSC is a multi-producer single-consumer bounded queue (Vyukov's bounded
// queue with a per-slot sequence number). Any number of goroutines may Push;
// exactly one goroutine may Pop.
type MPSC[T any] struct {
	slots []mpscSlot[T]
	mask  uint64
	enq   atomic.Uint64
	deq   uint64 // consumer-only
}

// NewMPSC returns a queue with the given capacity (a power of two).
func NewMPSC[T any](size int) *MPSC[T] {
	checkSize(size)
	q := &MPSC[T]{slots: make([]mpscSlot[T], size), mask: uint64(size - 1)}
	for i := range q.slots {
		q.slots[i].seq.Store(uint64(i))
	}
	return q
}

// Push appends v, or returns false if the queue is full.
func (q *MPSC[T]) Push(v T) bool {
	for {
		pos := q.enq.Load()
		s := &q.slots[pos&q.mask]
		switch dif := int64(s.seq.Load()) - int64(pos); {
		case dif == 0:
			if q.enq.CompareAndSwap(pos, pos+1) {
				s.val = v
				s.seq.Store(pos + 1) // publish to the consumer
				return true
			}
		case dif < 0:
			return false // the slot still holds an unconsumed element: full
		}
		// dif > 0: another producer claimed this position; reload and retry.
	}
}

// Pop removes the oldest element, or returns false if none is ready.
func (q *MPSC[T]) Pop() (v T, ok bool) {
	s := &q.slots[q.deq&q.mask]
	if s.seq.Load() != q.deq+1 {
		return v, false
	}
	v = s.val
	s.seq.Store(q.deq + q.mask + 1) // hand the slot back to producers
	q.deq++
	return v, true
}
