package eventlog

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/itsdamian/mini-exchange/internal/engine"
)

// PublisherConfig configures a Publisher.
type PublisherConfig struct {
	Symbol string
	// HighWatermark is the number of unacknowledged batches at which
	// Backlogged turns true and the gateway starts shedding load.
	HighWatermark int
	// QueueCapacity bounds the local queue. It must exceed HighWatermark by at
	// least the number of commands that can be admitted but not yet appended
	// (the pipeline's in-flight limit), so admission control, not a full queue,
	// is what pushes back.
	QueueCapacity int
	// OnDurable is called for every batch Kafka acknowledged, with its offset.
	// Calls may arrive concurrently and out of order.
	OnDurable  func(evs []engine.Event, offset int64)
	Registerer prometheus.Registerer
	Logger     *slog.Logger
}

// Publisher sends event batches to Kafka asynchronously. It never blocks the
// matcher: the engine goroutine does not touch it. If Kafka is unavailable,
// batches accumulate in a bounded local queue and, past the watermark, the
// gateway refuses new commands. Nothing is dropped silently.
type Publisher struct {
	cfg   PublisherConfig
	cl    *kgo.Client
	topic string
	log   *slog.Logger

	q       chan []engine.Event
	pending atomic.Int64 // appended and not yet acknowledged
	failed  atomic.Pointer[error]
	closed  atomic.Bool
	wg      sync.WaitGroup

	ackedSeq  atomic.Uint64
	lag       prometheus.Histogram
	errs      prometheus.Counter
	published prometheus.Counter
}

// NewPublisher starts the publishing goroutine. The client must be configured
// by the caller (see NewProducerClient).
func NewPublisher(cl *kgo.Client, cfg PublisherConfig) *Publisher {
	if cfg.HighWatermark == 0 {
		cfg.HighWatermark = 8192
	}
	if cfg.QueueCapacity == 0 {
		cfg.QueueCapacity = cfg.HighWatermark * 2
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	reg := cfg.Registerer
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	f := promauto.With(reg)
	p := &Publisher{
		cfg: cfg, cl: cl, topic: Topic(cfg.Symbol), log: cfg.Logger,
		q: make(chan []engine.Event, cfg.QueueCapacity),
		lag: f.NewHistogram(prometheus.HistogramOpts{
			Name: "mx_eventlog_publish_lag_seconds", Help: "Time from the engine stamping an event to Kafka acknowledging its batch.",
			Buckets: prometheus.ExponentialBuckets(1e-4, 2, 18), // 100 us .. ~13 s
		}),
		errs:      f.NewCounter(prometheus.CounterOpts{Name: "mx_eventlog_publish_errors_total", Help: "Batches Kafka permanently refused. Any value above zero stops the gateway admitting orders."}),
		published: f.NewCounter(prometheus.CounterOpts{Name: "mx_eventlog_batches_acked_total", Help: "Batches acknowledged by Kafka."}),
	}
	f.NewGaugeFunc(prometheus.GaugeOpts{Name: "mx_eventlog_pending_batches", Help: "Batches appended but not yet acknowledged by Kafka."},
		func() float64 { return float64(p.pending.Load()) })
	f.NewGaugeFunc(prometheus.GaugeOpts{Name: "mx_eventlog_acked_sequence", Help: "Highest event sequence acknowledged by Kafka."},
		func() float64 { return float64(p.ackedSeq.Load()) })
	p.wg.Add(1)
	go p.run()
	return p
}

// Append queues one command's events. The slice is owned by the publisher
// from now on. It blocks only if the local queue is completely full, which
// admission control is sized to prevent; blocking there stalls the dispatcher
// rather than losing events.
func (p *Publisher) Append(evs []engine.Event) {
	if len(evs) == 0 {
		return
	}
	p.pending.Add(1)
	p.q <- evs
}

// Backlogged reports whether new commands should be refused: too many batches
// are waiting for Kafka, or the log has failed permanently.
func (p *Publisher) Backlogged() bool {
	return p.failed.Load() != nil || p.pending.Load() >= int64(p.cfg.HighWatermark)
}

// Pending returns the number of batches appended and not yet acknowledged.
func (p *Publisher) Pending() int64 { return p.pending.Load() }

// AckedSeq returns the highest event sequence Kafka has acknowledged.
func (p *Publisher) AckedSeq() uint64 { return p.ackedSeq.Load() }

// Err returns the permanent failure that stopped publishing, if any.
func (p *Publisher) Err() error {
	if e := p.failed.Load(); e != nil {
		return *e
	}
	return nil
}

func (p *Publisher) run() {
	defer p.wg.Done()
	key := []byte(p.cfg.Symbol)
	var buf []byte
	for evs := range p.q {
		buf = buf[:0]
		var err error
		buf, err = proto.MarshalOptions{}.MarshalAppend(buf, ToProtoBatch(p.cfg.Symbol, evs))
		if err != nil {
			p.fail(err)
			continue
		}
		value := append([]byte(nil), buf...) // the client keeps the slice until acknowledged
		rec := &kgo.Record{Topic: p.topic, Key: key, Value: value}
		batch := evs
		// Produce blocks when the client's own buffer is full: that is the
		// backpressure from Kafka up to our local queue.
		p.cl.Produce(context.Background(), rec, func(r *kgo.Record, err error) {
			if err != nil {
				p.fail(err)
				return
			}
			p.pending.Add(-1)
			p.published.Inc()
			last := batch[len(batch)-1]
			for { // monotonic max
				cur := p.ackedSeq.Load()
				if last.Seq <= cur || p.ackedSeq.CompareAndSwap(cur, last.Seq) {
					break
				}
			}
			p.lag.Observe(float64(time.Now().UnixNano()-last.Ts) / 1e9)
			if p.cfg.OnDurable != nil {
				p.cfg.OnDurable(batch, r.Offset)
			}
		})
	}
}

func (p *Publisher) fail(err error) {
	p.errs.Inc()
	if p.failed.CompareAndSwap(nil, &err) {
		p.log.Error("event log publishing failed permanently; refusing new orders", "err", err)
	}
}

// Close stops accepting batches, flushes everything to Kafka and closes the
// client. Call it after the pipeline has drained.
func (p *Publisher) Close(ctx context.Context) error {
	if p.closed.Swap(true) {
		return nil
	}
	close(p.q)
	p.wg.Wait()
	err := p.cl.Flush(ctx)
	p.cl.Close()
	return err
}
