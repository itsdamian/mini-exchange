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
