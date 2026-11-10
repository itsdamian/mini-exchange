package experiments

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

// Experiment 4: costs the hot path avoids. Each benchmark is the avoided
// operation next to what the engine does instead. These are isolated costs, not
// a measurement of the whole matcher.

var sinkI uint64
var sinkS string
var sinkAny any

func BenchmarkClock(b *testing.B) {
	b.Run("time.Now", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			sinkI += uint64(time.Now().UnixNano())
		}
	})
	b.Run("time.Since", func(b *testing.B) {
		t0 := time.Now()
		for i := 0; i < b.N; i++ {
			sinkI += uint64(time.Since(t0))
		}
	})
	b.Run("sequencer-counter", func(b *testing.B) { // what the matcher stamps: a value it was handed
		var ts uint64
		for i := 0; i < b.N; i++ {
			ts++
			sinkI += ts
		}
	})
}

func BenchmarkOrderIDAsText(b *testing.B) {
	b.Run("fmt.Sprintf", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sinkS = fmt.Sprintf("%d", i)
		}
	})
	b.Run("strconv.Itoa", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sinkS = strconv.Itoa(i)
		}
	})
	b.Run("integer-id", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sinkI += uint64(i)
		}
	})
}

func BenchmarkOrderLookup(b *testing.B) {
	const live = 100000
	strKeys := make([]string, live)
	m1 := make(map[string]int32, live)
	m2 := make(map[uint64]int32, live)
	for i := range strKeys {
		strKeys[i] = "ord-" + strconv.Itoa(i)
		m1[strKeys[i]] = int32(i)
		m2[uint64(i)] = int32(i)
	}
	b.Run("map[string]", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sinkI += uint64(m1[strKeys[i%live]])
		}
	})
	b.Run("map[uint64]", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sinkI += uint64(m2[uint64(i%live)])
		}
	})
}

type event struct{ a, b, c, d, e, f uint64 }

var _ = event{b: 1, c: 1, d: 1, e: 1, f: 1} // the extra fields give it a realistic size

func BenchmarkEventPassing(b *testing.B) {
	b.Run("boxed-in-interface", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var x any = event{a: uint64(i)} // escapes: the value is copied to the heap
			sinkAny = x
		}
	})
	b.Run("concrete-struct", func(b *testing.B) {
		b.ReportAllocs()
		var out event
		for i := 0; i < b.N; i++ {
			out = event{a: uint64(i)}
			sinkI += out.a
		}
	})
}
