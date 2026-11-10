package experiments

import (
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/itsdamian/mini-exchange/internal/engine"
	"github.com/itsdamian/mini-exchange/internal/enginetest"
	"github.com/itsdamian/mini-exchange/internal/eventlog"

	exchangev1 "github.com/itsdamian/mini-exchange/gen/exchange/v1"
)

// Experiment 8: the cost of encoding an engine command's events for the Kafka
// event log. Compared, for the batches a realistic workload produces:
//
//	naive:   build a new EventBatch (eventlog.ToProtoBatch) and proto.Marshal it
//	current: the publisher's way: a new EventBatch, MarshalAppend into a reused
//	         buffer, then one copy because the Kafka client keeps the slice
//	reused:  additionally reuse the EventBatch and its EngineEvent messages
//
// The gateway's gRPC request and response encoding is inside grpc-go and is
// looked at with a CPU profile instead (BENCHMARKS.md).

func batches(n int) [][]engine.Event {
	cmds := enginetest.Generate(n, 11, enginetest.DefaultProfile())
	book := engine.NewBook(engine.Config{MaxOrders: 4096, MaxLevels: 256})
	var out [][]engine.Event
	for i := range cmds {
		c := cmds[i]
		c.Ts = int64(i + 1)
		if evs := book.Apply(&c, nil); len(evs) > 0 {
			out = append(out, append([]engine.Event(nil), evs...))
		}
	}
	return out
}

func BenchmarkEventBatchEncode(b *testing.B) {
	bs := batches(20000)
	const symbol = "BTC-USD"
	b.Run("naive", func(b *testing.B) {
		b.ReportAllocs()
		var n int
		for i := 0; i < b.N; i++ {
			raw, err := proto.Marshal(eventlog.ToProtoBatch(symbol, bs[i%len(bs)]))
			if err != nil {
				b.Fatal(err)
			}
			n += len(raw)
		}
		sinkI += uint64(n)
	})
	b.Run("current-reused-buffer", func(b *testing.B) {
		b.ReportAllocs()
		var buf []byte
		var n int
		for i := 0; i < b.N; i++ {
			buf = buf[:0]
			buf, _ = proto.MarshalOptions{}.MarshalAppend(buf, eventlog.ToProtoBatch(symbol, bs[i%len(bs)]))
			value := append([]byte(nil), buf...)
			n += len(value)
		}
		sinkI += uint64(n)
	})
	b.Run("reused-messages", func(b *testing.B) {
		b.ReportAllocs()
		var buf []byte
		var n int
		msg := &exchangev1.EventBatch{}
		for i := 0; i < b.N; i++ {
			eventlog.FillProtoBatch(msg, symbol, bs[i%len(bs)])
			buf = buf[:0]
			buf, _ = proto.MarshalOptions{}.MarshalAppend(buf, msg)
			value := append([]byte(nil), buf...)
			n += len(value)
		}
		sinkI += uint64(n)
	})
}

// The three encoders must produce the same bytes.
func TestEncodersAgree(t *testing.T) {
	bs := batches(3000)
	msg := &exchangev1.EventBatch{}
	for _, evs := range bs {
		want, _ := proto.Marshal(eventlog.ToProtoBatch("S", evs))
		eventlog.FillProtoBatch(msg, "S", evs)
		got, _ := proto.MarshalOptions{}.MarshalAppend(nil, msg)
		if string(got) != string(want) {
			t.Fatalf("reused encoder differs for %d events", len(evs))
		}
	}
}
