package experiments

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/itsdamian/mini-exchange/internal/ring"
)

// Experiments 3 and 5: handing items from one goroutine to another.
//
// 3: the preallocated lock-free ring buffer against a buffered channel.
// 5: the effect of the memory layout of the ring's two cursors: as in
//    internal/ring today (adjacent atomics, which share a cache line), padded
//    so each cursor has a cache line of its own, and padded with each side
//    caching the other side's cursor so it reads it only when it must.
//
// The item is 48 bytes, the size of the engine's output item.

type item struct{ a, b, c, d, e, f uint64 }

// b to f are only there to give the item the size of the engine's output item (48 bytes).
var _ = item{b: 1, c: 1, d: 1, e: 1, f: 1}

const cacheLine = 64

// spscPadded: head and tail on separate cache lines.
type spscPadded struct {
	_    [cacheLine]byte
	head atomic.Uint64
	_    [cacheLine - 8]byte
	tail atomic.Uint64
	_    [cacheLine - 8]byte
	buf  []item
	mask uint64
}

func newPadded(n int) *spscPadded { return &spscPadded{buf: make([]item, n), mask: uint64(n - 1)} }

func (q *spscPadded) push(v item) bool {
	t := q.tail.Load()
	if t-q.head.Load() == uint64(len(q.buf)) {
		return false
	}
	q.buf[t&q.mask] = v
	q.tail.Store(t + 1)
	return true
}

func (q *spscPadded) pop() (v item, ok bool) {
	h := q.head.Load()
	if h == q.tail.Load() {
		return v, false
	}
	v = q.buf[h&q.mask]
	q.head.Store(h + 1)
	return v, true
}

// spscCached: padded, and each side keeps a private copy of the other side's
// cursor, refreshed only when the queue looks full (producer) or empty (consumer).
type spscCached struct {
	_          [cacheLine]byte
	head       atomic.Uint64
	_          [cacheLine - 8]byte
	tail       atomic.Uint64
	_          [cacheLine - 8]byte
	cachedHead uint64 // producer's view of head
	_          [cacheLine - 8]byte
	cachedTail uint64 // consumer's view of tail
	_          [cacheLine - 8]byte
	buf        []item
	mask       uint64
}

func newCached(n int) *spscCached { return &spscCached{buf: make([]item, n), mask: uint64(n - 1)} }

func (q *spscCached) push(v item) bool {
	t := q.tail.Load()
	if t-q.cachedHead == uint64(len(q.buf)) {
		q.cachedHead = q.head.Load()
		if t-q.cachedHead == uint64(len(q.buf)) {
			return false
		}
	}
	q.buf[t&q.mask] = v
	q.tail.Store(t + 1)
	return true
}

func (q *spscCached) pop() (v item, ok bool) {
	h := q.head.Load()
	if h == q.cachedTail {
		q.cachedTail = q.tail.Load()
		if h == q.cachedTail {
			return v, false
		}
	}
	v = q.buf[h&q.mask]
	q.head.Store(h + 1)
	return v, true
}

const qsize = 1024

// run starts a producer goroutine that sends b.N items through push and times
// the consumer (this goroutine) receiving them all.
func run(b *testing.B, push func(item) bool, pop func() (item, bool)) {
	b.ReportAllocs()
	var wg sync.WaitGroup
	wg.Add(1)
	b.ResetTimer()
	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			it := item{a: uint64(i)}
			for !push(it) {
			}
		}
	}()
	for got := 0; got < b.N; {
		if v, ok := pop(); ok {
			if v.a != uint64(got) {
				b.Fatalf("out of order: got %d want %d", v.a, got)
			}
			got++
		}
	}
	wg.Wait()
}

func BenchmarkSPSCThroughput(b *testing.B) {
	b.Run("channel", func(b *testing.B) {
		ch := make(chan item, qsize)
		b.ReportAllocs()
		var wg sync.WaitGroup
		wg.Add(1)
		b.ResetTimer()
		go func() {
			defer wg.Done()
			for i := 0; i < b.N; i++ {
				ch <- item{a: uint64(i)}
			}
		}()
		for got := 0; got < b.N; got++ {
			if v := <-ch; v.a != uint64(got) {
				b.Fatalf("out of order")
			}
		}
		wg.Wait()
	})
	b.Run("ring-current", func(b *testing.B) {
		q := ring.NewSPSC[item](qsize)
		run(b, q.Push, q.Pop)
	})
	b.Run("ring-padded", func(b *testing.B) {
		q := newPadded(qsize)
		run(b, q.push, q.pop)
	})
	b.Run("ring-padded-cached", func(b *testing.B) {
		q := newCached(qsize)
		run(b, q.push, q.pop)
	})
}

// Four producers, one consumer (the gateway's many request goroutines into the
// single engine goroutine).
func BenchmarkMPSCThroughput(b *testing.B) {
	const producers = 4
	b.Run("channel", func(b *testing.B) {
		ch := make(chan item, qsize)
		b.ReportAllocs()
		var wg sync.WaitGroup
		b.ResetTimer()
		for p := 0; p < producers; p++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := p; i < b.N; i += producers {
					ch <- item{a: uint64(i)}
				}
			}()
		}
		for got := 0; got < b.N; got++ {
			<-ch
		}
		wg.Wait()
	})
	b.Run("ring-mpsc", func(b *testing.B) {
		q := ring.NewMPSC[item](qsize)
		b.ReportAllocs()
		var wg sync.WaitGroup
		b.ResetTimer()
		for p := 0; p < producers; p++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := p; i < b.N; i += producers {
					for !q.Push(item{a: uint64(i)}) {
					}
				}
			}()
		}
		for got := 0; got < b.N; {
			if _, ok := q.Pop(); ok {
				got++
			}
		}
		wg.Wait()
	})
}

// Round trip: this goroutine sends an item and waits for the other goroutine to
// send it back. It measures the wake-up latency of the hand-off, where a
// spinning consumer and a parked (blocking) one differ most.
func BenchmarkPingPong(b *testing.B) {
	b.Run("channel", func(b *testing.B) {
		ping, pong := make(chan item), make(chan item)
		go func() {
			for v := range ping {
				pong <- v
			}
		}()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ping <- item{a: uint64(i)}
			<-pong
		}
		close(ping)
	})
	b.Run("ring-spin", func(b *testing.B) {
		ping, pong := ring.NewSPSC[item](2), ring.NewSPSC[item](2)
		var stop atomic.Bool
		done := make(chan struct{})
		go func() {
			defer close(done)
			for !stop.Load() {
				if v, ok := ping.Pop(); ok {
					for !pong.Push(v) {
					}
				}
			}
		}()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for !ping.Push(item{a: uint64(i)}) {
			}
			for {
				if _, ok := pong.Pop(); ok {
					break
				}
			}
		}
		stop.Store(true)
		<-done
	})
	b.Run("ring-spin-then-yield", func(b *testing.B) {
		ping, pong := ring.NewSPSC[item](2), ring.NewSPSC[item](2)
		var stop atomic.Bool
		done := make(chan struct{})
		go func() {
			defer close(done)
			for idle := 0; !stop.Load(); idle++ {
				if v, ok := ping.Pop(); ok {
					idle = 0
					for !pong.Push(v) {
					}
				} else if idle > 100 {
					runtime.Gosched()
				}
			}
		}()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for !ping.Push(item{a: uint64(i)}) {
			}
			for idle := 0; ; idle++ {
				if _, ok := pong.Pop(); ok {
					break
				} else if idle > 100 {
					runtime.Gosched()
				}
			}
		}
		stop.Store(true)
		<-done
	})
}
