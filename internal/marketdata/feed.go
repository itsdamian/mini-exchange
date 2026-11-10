// Package marketdata builds a depth and trade view of one symbol from the
// event log and serves it over WebSocket with conflation (FR-7).
//
// It is a separate process that only reads Kafka, so it cannot slow the
// engine: the matcher never sees a market data client. Every client gets the
// latest state at most once per conflation interval, never a queue of
// updates, and a client that cannot keep up is disconnected, so memory per
// client is bounded.
package marketdata

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/itsdamian/mini-exchange/internal/engine"
)

// Trade is one recent execution.
type Trade struct {
	Seq   uint64 `json:"seq"`
	Price int64  `json:"price"`
	Qty   int64  `json:"qty"`
	Side  string `json:"side"` // taker side: buy or sell
	TsNs  int64  `json:"ts_ns"`
}

// Snapshot is the message sent to clients: the whole current state, not a
// delta, so a client that missed frames (conflation) needs nothing else.
// Depth levels are [price, quantity], asks from best to worst, then bids.
type Snapshot struct {
	Type   string     `json:"type"` // "snapshot"
	Symbol string     `json:"symbol"`
	Seq    uint64     `json:"seq"` // last engine event sequence included
	TsNs   int64      `json:"ts_ns"`
	Asks   [][2]int64 `json:"asks"`
	Bids   [][2]int64 `json:"bids"`
	Trades []Trade    `json:"trades"` // most recent last
}

// Frame is an encoded Snapshot shared by all clients.
type Frame struct {
	Version uint64 // increases whenever the state changed
	Data    []byte
}

// FeedConfig configures a Feed.
type FeedConfig struct {
	Depth      int // price levels per side (default 10)
	KeepTrades int // recent trades kept (default 50)
	Registerer prometheus.Registerer
}

// Feed owns a book rebuilt from the event log and the latest encoded frame.
type Feed struct {
	symbol string
	depth  int
	keep   int

	mu     sync.Mutex
	book   *engine.Book
	trades []Trade
	dirty  bool
	lastTs int64

	version uint64
	cur     atomic.Pointer[Frame]

	events  prometheus.Counter
	build   prometheus.Histogram
	lag     prometheus.Histogram
	frameSz prometheus.Gauge
}

// NewFeed wraps a book (already caught up to the log). The Feed owns it from
// now on.
func NewFeed(symbol string, book *engine.Book, cfg FeedConfig) *Feed {
	if cfg.Depth <= 0 {
		cfg.Depth = 10
	}
	if cfg.KeepTrades <= 0 {
		cfg.KeepTrades = 50
	}
	reg := promauto.With(cfg.Registerer)
	f := &Feed{
		symbol: symbol, depth: cfg.Depth, keep: cfg.KeepTrades, book: book, dirty: true,
		events: reg.NewCounter(prometheus.CounterOpts{Name: "mx_md_events_applied_total", Help: "Engine events applied to the market data book."}),
		build: reg.NewHistogram(prometheus.HistogramOpts{Name: "mx_md_frame_build_seconds", Help: "Time to build and encode one frame.",
			Buckets: prometheus.ExponentialBuckets(1e-5, 2, 14)}),
		lag: reg.NewHistogram(prometheus.HistogramOpts{Name: "mx_md_publish_lag_seconds", Help: "Engine event time to the frame that contains it.",
			Buckets: prometheus.ExponentialBuckets(1e-3, 2, 14)}),
		frameSz: reg.NewGauge(prometheus.GaugeOpts{Name: "mx_md_frame_bytes", Help: "Size of the latest frame."}),
	}
	f.Publish()
	return f
}

// Apply applies the events of one engine command (one log record).
func (f *Feed) Apply(evs []engine.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.book.ApplyEvents(evs); err != nil {
		return err
	}
	for i := range evs {
		e := &evs[i]
		if e.Kind == engine.EvTrade {
			side := "buy"
			if e.Side == engine.Sell {
				side = "sell"
			}
			if len(f.trades) == f.keep {
				copy(f.trades, f.trades[1:])
				f.trades = f.trades[:f.keep-1]
			}
			f.trades = append(f.trades, Trade{Seq: e.Seq, Price: e.Price, Qty: e.Qty, Side: side, TsNs: e.Ts})
		}
		if e.Ts > 0 {
			f.lastTs = e.Ts
		}
	}
	f.events.Add(float64(len(evs)))
	f.dirty = true
	return nil
}

// Publish builds and stores a new frame if the state changed since the last
// one. It reports whether it did.
func (f *Feed) Publish() bool {
	t0 := time.Now()
	f.mu.Lock()
	if !f.dirty {
		f.mu.Unlock()
		return false
	}
	s := Snapshot{Type: "snapshot", Symbol: f.symbol, Seq: f.book.Seq(), TsNs: f.lastTs,
		Asks: make([][2]int64, 0, f.depth), Bids: make([][2]int64, 0, f.depth), Trades: append([]Trade(nil), f.trades...)}
	f.book.Depth(f.depth, func(side engine.Side, price, qty int64) {
		if side == engine.Sell {
			s.Asks = append(s.Asks, [2]int64{price, qty})
		} else {
			s.Bids = append(s.Bids, [2]int64{price, qty})
		}
	})
	f.dirty = false
	ts := f.lastTs
	f.mu.Unlock()

	data, err := json.Marshal(&s)
	if err != nil { // plain integers and strings: cannot fail
		panic(err)
	}
	f.version++
	f.cur.Store(&Frame{Version: f.version, Data: data})
	f.frameSz.Set(float64(len(data)))
	f.build.Observe(time.Since(t0).Seconds())
	if ts > 0 {
		f.lag.Observe(float64(time.Now().UnixNano()-ts) / 1e9)
	}
	return true
}

// Frame returns the latest frame (never nil after NewFeed).
func (f *Feed) Frame() *Frame { return f.cur.Load() }

// Run publishes a new frame every interval while there is something new,
// until stop is closed. The interval is the conflation period: however fast
// events arrive, at most one frame per interval is built and encoded.
func (f *Feed) Run(stop <-chan struct{}, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			f.Publish()
		}
	}
}
