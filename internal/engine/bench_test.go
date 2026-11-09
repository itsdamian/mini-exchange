package engine_test

import (
	"testing"

	"github.com/itsdamian/mini-exchange/internal/engine"
	"github.com/itsdamian/mini-exchange/internal/enginetest"
	"github.com/itsdamian/mini-exchange/internal/refbook"
)

func deepProfile() enginetest.Profile {
	p := enginetest.DefaultProfile()
	p.Depth, p.TargetBook = 500, 20_000
	return p
}

// benchApply replays a pre-generated stream in a loop. The book is warmed up
// with one full pass first, so the timed region measures steady state. Only
// Apply is inside the timed loop; commands are generated beforehand.
func benchApply(b *testing.B, p enginetest.Profile) {
	const n = 1 << 20
	cmds := enginetest.Generate(n, 7, p)
	bk := engine.NewBook(engine.Config{MaxOrders: p.TargetBook * 2, MaxLevels: int(p.Depth) * 4})
	out := make([]engine.Event, 0, 4096)
	var ts int64
	run := func(start, count int) {
		for i := start; i < start+count; i++ {
			c := cmds[i&(n-1)]
			// The stream wraps: reissuing ids that are still live would only
			// produce duplicate rejects, so shift ids per lap, keep order.
			c.OrderID += uint64(i/n) * (1 << 41)
			ts++
			c.Ts = ts
			out = bk.Apply(&c, out[:0])
		}
	}
	run(0, n) // warm-up lap, not measured
	b.ReportAllocs()
	b.ResetTimer()
	run(n, b.N)
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "orders/s")
}

func BenchmarkApplyMixed(b *testing.B)    { benchApply(b, enginetest.DefaultProfile()) }
func BenchmarkApplyDeepBook(b *testing.B) { benchApply(b, deepProfile()) }
func BenchmarkReferenceMixed(b *testing.B) {
	cmds := enginetest.Generate(1<<16, 7, enginetest.DefaultProfile())
	ref := refbook.New()
	var out []engine.Event
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c := cmds[i&(1<<16-1)]
		c.OrderID += uint64(i>>16) * (1 << 41)
		out = ref.Apply(&c, out[:0])
	}
}

// BenchmarkApplyEventsReplay measures rebuilding a book from recorded events
// (the recovery path) with no Kafka and no decoding: the floor under recovery
// time. Each iteration replays about one million events into a fresh book.
func BenchmarkApplyEventsReplay(b *testing.B) {
	cmds := enginetest.Generate(700_000, 7, enginetest.DefaultProfile())
	src := engine.NewBook(engine.Config{MaxOrders: 4096, MaxLevels: 512})
	var batches [][]engine.Event
	events := 0
	for i := range cmds {
		c := cmds[i]
		c.Ts = int64(i + 1)
		evs := src.Apply(&c, nil)
		events += len(evs)
		batches = append(batches, evs)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bk := engine.NewBook(engine.Config{MaxOrders: 4096, MaxLevels: 512})
		for _, evs := range batches {
			if err := bk.ApplyEvents(evs); err != nil {
				b.Fatal(err)
			}
		}
		if bk.Hash() != src.Hash() {
			b.Fatal("replayed book differs")
		}
	}
	b.ReportMetric(float64(events)*float64(b.N)/b.Elapsed().Seconds(), "events/s")
	b.ReportMetric(float64(events), "events/op")
}
