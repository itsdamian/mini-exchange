// Command logtool inspects and verifies the Kafka event log.
//
//	logtool check   -kafka B -symbol S                       clean replay of the whole log
//	logtool compare -kafka B -symbol S -data-dir D           snapshot+tail recovery vs clean replay
//	logtool gen     -kafka B -symbol S -events N             publish N generated events (empty topic only)
//
// check verifies sequence continuity (no gap, no duplicate) and that every
// event applies; it prints the last sequence, the event count and the book
// hash. compare exits non-zero unless recovery from the newest snapshot gives
// exactly the same book hash as a clean replay (the FR-5 acceptance check).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/itsdamian/mini-exchange/internal/engine"
	"github.com/itsdamian/mini-exchange/internal/enginetest"
	"github.com/itsdamian/mini-exchange/internal/eventlog"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "logtool:", err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		usage()
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	kafka := fs.String("kafka", "localhost:9092", "comma-separated Kafka brokers")
	symbol := fs.String("symbol", "BTC-USD", "symbol")
	dataDir := fs.String("data-dir", "./data", "snapshot directory (compare)")
	events := fs.Int("events", 1_000_000, "approximate events to generate (gen)")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	maxOrders := fs.Int("max-orders", 1<<17, "book capacity")
	_ = fs.Parse(os.Args[2:])

	brokers := strings.Split(*kafka, ",")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	cfg := engine.Config{MaxOrders: *maxOrders, MaxLevels: 4096}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	switch cmd {
	case "check":
		return check(ctx, brokers, *symbol, cfg, log, *jsonOut)
	case "compare":
		return compare(ctx, brokers, *symbol, *dataDir, cfg, log, *jsonOut)
	case "gen":
		return gen(ctx, brokers, *symbol, *events, cfg, log)
	}
	usage()
	return nil
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: logtool check|compare|gen [flags]")
	os.Exit(2)
}

type report struct {
	Mode         string  `json:"mode"`
	LastSeq      uint64  `json:"last_sequence"`
	Events       int     `json:"events_replayed"`
	Batches      int     `json:"batches_replayed"`
	LogEnd       int64   `json:"log_end_offset"`
	Resting      int     `json:"resting_orders"`
	BookHash     string  `json:"book_hash"`
	FromSnapshot bool    `json:"from_snapshot"`
	SnapshotSeq  uint64  `json:"snapshot_sequence,omitempty"`
	TotalMs      float64 `json:"total_ms"`
	ReplayMs     float64 `json:"replay_ms"`
	EventsPerSec float64 `json:"replay_events_per_sec,omitempty"`
}

func mk(mode string, b *engine.Book, st *eventlog.RecoverStats) report {
	r := report{
		Mode: mode, LastSeq: st.LastSeq, Events: st.Events, Batches: st.Batches, LogEnd: st.LogEnd, Resting: b.Len(),
		BookHash: fmt.Sprintf("%016x", st.BookHash), FromSnapshot: st.FromSnapshot, SnapshotSeq: st.SnapshotSeq,
		TotalMs: float64(st.Total.Microseconds()) / 1000, ReplayMs: float64(st.Replay.Microseconds()) / 1000,
	}
	if st.Replay > 0 && st.Events > 0 {
		r.EventsPerSec = float64(st.Events) / st.Replay.Seconds()
	}
	return r
}

func show(r report, asJSON bool) {
	if asJSON {
		b, _ := json.Marshal(r)
		fmt.Println(string(b))
		return
	}
	fmt.Printf("%-18s last_seq=%d events=%d batches=%d resting=%d book_hash=%s total=%.1fms replay=%.1fms",
		r.Mode, r.LastSeq, r.Events, r.Batches, r.Resting, r.BookHash, r.TotalMs, r.ReplayMs)
	if r.FromSnapshot {
		fmt.Printf(" (from snapshot at seq %d)", r.SnapshotSeq)
	}
	if r.EventsPerSec > 0 {
		fmt.Printf(" %.0f events/s", r.EventsPerSec)
	}
	fmt.Println()
}

func check(ctx context.Context, brokers []string, symbol string, cfg engine.Config, log *slog.Logger, asJSON bool) error {
	b, st, err := eventlog.Recover(ctx, brokers, symbol, nil, cfg, log)
	if err != nil {
		return fmt.Errorf("CHECK FAILED: %w", err)
	}
	show(mk("clean-replay", b, st), asJSON)
	return nil
}

func compare(ctx context.Context, brokers []string, symbol, dir string, cfg engine.Config, log *slog.Logger, asJSON bool) error {
	cb, cst, err := eventlog.Recover(ctx, brokers, symbol, nil, cfg, log)
	if err != nil {
		return fmt.Errorf("clean replay failed: %w", err)
	}
	sb, sst, err := eventlog.Recover(ctx, brokers, symbol, &eventlog.SnapshotStore{Dir: dir, Symbol: symbol}, cfg, log)
	if err != nil {
		return fmt.Errorf("snapshot recovery failed: %w", err)
	}
	show(mk("clean-replay", cb, cst), asJSON)
	show(mk("snapshot+replay", sb, sst), asJSON)
	if cst.BookHash != sst.BookHash || cst.LastSeq != sst.LastSeq {
		return fmt.Errorf("MISMATCH: clean replay %016x@%d, snapshot recovery %016x@%d", cst.BookHash, cst.LastSeq, sst.BookHash, sst.LastSeq)
	}
	fmt.Println("OK: snapshot recovery equals clean replay")
	return nil
}

// gen runs the engine over a generated command stream and publishes every
// event batch, as fast as Kafka acknowledges. It needs an empty topic so the
// sequence starts at 1.
func gen(ctx context.Context, brokers []string, symbol string, wantEvents int, cfg engine.Config, log *slog.Logger) error {
	producer, err := eventlog.NewProducerClient(brokers, 5*time.Millisecond, 1<<16)
	if err != nil {
		return err
	}
	if err := eventlog.EnsureTopic(ctx, producer, symbol, 1); err != nil {
		return err
	}
	_, st, err := eventlog.Recover(ctx, brokers, symbol, nil, cfg, log)
	if err != nil {
		return err
	}
	if st.LogEnd > 0 {
		return fmt.Errorf("topic %s already holds %d records; gen needs an empty log", eventlog.Topic(symbol), st.LogEnd)
	}

	var acked atomic.Uint64
	pub := eventlog.NewPublisher(producer, eventlog.PublisherConfig{
		Symbol: symbol, HighWatermark: 1 << 14, QueueCapacity: 1 << 15,
		OnDurable: func(evs []engine.Event, _ int64) { acked.Add(uint64(len(evs))) },
	})

	// Every command yields at least one event, so wantEvents commands always
	// reach wantEvents events (the last batch may overshoot slightly).
	cmds := enginetest.Generate(wantEvents, 21, enginetest.DefaultProfile())
	book := engine.NewBook(cfg)
	start := time.Now()
	baseTs := start.UnixNano()
	var out []engine.Event
	var produced uint64
	for i := range cmds {
		c := cmds[i]
		c.Ts = baseTs + int64(i)*1000
		out = book.Apply(&c, out[:0])
		produced += uint64(len(out))
		for pub.Backlogged() { // honour the same backpressure the gateway does
			time.Sleep(time.Millisecond)
		}
		pub.Append(append([]engine.Event(nil), out...))
		if produced >= uint64(wantEvents) {
			break
		}
	}
	if err := pub.Close(ctx); err != nil {
		return err
	}
	el := time.Since(start)
	fmt.Printf("published %d events (last seq %d, book hash %016x) in %.2fs = %.0f events/s; acked %d\n",
		produced, book.Seq(), book.Hash(), el.Seconds(), float64(produced)/el.Seconds(), acked.Load())
	return nil
}
