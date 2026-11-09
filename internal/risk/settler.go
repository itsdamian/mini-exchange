package risk

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
)

// OpKind is the kind of settlement operation.
type OpKind uint8

// Settlement operations, each derived from one engine event.
const (
	OpBind    OpKind = iota + 1 // Accepted: reservation moves from client id to order id
	OpFill                      // Trade: one side consumes part of its reservation
	OpCancel                    // Canceled: release what is left of an accepted order
	OpRelease                   // Rejected: release a reservation the engine never accepted
)

// Op is one settlement operation. Seq is the engine event sequence it came
// from; per account, ops are applied in increasing Seq and a repeated Seq is
// skipped, which makes retries safe.
type Op struct {
	Kind    OpKind
	Seq     uint64
	Account uint64
	OrderID uint64 // OpBind, OpFill, OpCancel
	Client  string // OpBind, OpRelease
	Price   int64  // OpFill
	Qty     int64  // OpFill
	at      int64  // enqueue time (unix ns), for the lag metric
}

type settler struct {
	c       *Checker
	ch      chan Op
	high    int
	batch   int
	log     *slog.Logger
	closed  atomic.Bool
	enq     atomic.Uint64 // ops accepted into the queue
	fin     atomic.Uint64 // ops finished (applied or abandoned)
	failing atomic.Bool
	done    chan struct{}
	stop    chan struct{}

	dropped   prometheus.Counter
	applied   prometheus.Counter
	errs      prometheus.Counter
	anomalies *prometheus.CounterVec
	lag       prometheus.Histogram
}

func newSettler(c *Checker, cfg Config, reg prometheus.Registerer) *settler {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 1 << 20
	}
	if cfg.HighWatermark <= 0 {
		cfg.HighWatermark = cfg.QueueSize / 2
	}
	if cfg.MaxBatch <= 0 {
		cfg.MaxBatch = 256
	}
	s := &settler{
		c: c, ch: make(chan Op, cfg.QueueSize), high: cfg.HighWatermark, batch: cfg.MaxBatch,
		log:  slog.Default().With("component", "risk-settler"),
		done: make(chan struct{}), stop: make(chan struct{}),
		dropped: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "mx_risk_settle_dropped_total", Help: "Settlement ops lost because the queue was full (Redis balances then need reconciliation)."}),
		applied: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "mx_risk_settle_applied_total", Help: "Settlement ops applied to Redis."}),
		errs: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "mx_risk_settle_errors_total", Help: "Failed Redis round trips by the settler (each is retried)."}),
		anomalies: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "mx_risk_settle_anomalies_total", Help: "Ops that found no reservation or exceeded it, by kind. Non-zero means Redis and the engine disagree."}, []string{"kind"}),
		lag: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name: "mx_risk_settle_lag_seconds", Help: "Time from the engine event to the op being applied in Redis.",
			Buckets: prometheus.ExponentialBuckets(1e-4, 2, 18)}),
	}
	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "mx_risk_settle_queue", Help: "Settlement ops waiting to be applied."}, func() float64 { return float64(len(s.ch)) })
	go s.run()
	return s
}

// enqueue never blocks: the dispatcher must not stall the engine pipeline on
// Redis. A full queue drops the op and counts it; the gateway refuses new
// orders well before that (high watermark).
func (s *settler) enqueue(op Op) {
	op.at = time.Now().UnixNano()
	select {
	case s.ch <- op:
		s.enq.Add(1)
	default:
		s.dropped.Inc()
		s.log.Error("settlement queue full, op dropped: reconcile Redis balances", "kind", op.Kind, "seq", op.Seq, "account", op.Account)
	}
}

func (s *settler) backlogged() bool { return s.failing.Load() || len(s.ch) >= s.high }

func (s *settler) close(ctx context.Context) error {
	if s.closed.CompareAndSwap(false, true) {
		close(s.stop)
	}
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return errors.New("risk: settlement queue not drained: " + ctx.Err().Error())
	}
}

func (s *settler) run() {
	defer close(s.done)
	buf := make([]Op, 0, s.batch)
	for {
		buf = buf[:0]
		select {
		case op := <-s.ch:
			buf = append(buf, op)
		case <-s.stop:
			// Drain what is queued, then exit.
			for len(s.ch) > 0 {
				buf = buf[:0]
				for len(buf) < s.batch && len(s.ch) > 0 {
					buf = append(buf, <-s.ch)
				}
				s.apply(buf, true)
			}
			return
		}
		for len(buf) < s.batch {
			select {
			case op := <-s.ch:
				buf = append(buf, op)
				continue
			default:
			}
			break
		}
		s.apply(buf, false)
	}
}

// apply sends one batch, retrying with backoff until it succeeds. While
// stopping it gives up after a few attempts so shutdown cannot hang.
func (s *settler) apply(batch []Op, stopping bool) {
	backoff := 20 * time.Millisecond
	for attempt := 0; ; attempt++ {
		err := s.exec(batch)
		if err == nil {
			s.failing.Store(false)
			s.fin.Add(uint64(len(batch)))
			return
		}
		s.errs.Inc()
		s.failing.Store(true)
		if stopping && attempt >= 3 {
			s.log.Error("settlement abandoned at shutdown", "ops", len(batch), "err", err)
			s.fin.Add(uint64(len(batch)))
			return
		}
		s.log.Warn("settlement batch failed, retrying", "err", err, "ops", len(batch), "backoff", backoff)
		time.Sleep(backoff)
		backoff = min(backoff*2, 500*time.Millisecond)
	}
}

// exec sends one batch: one Lua call per account (ops of an account stay in
// order), all in one pipelined round trip.
func (s *settler) exec(batch []Op) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var order []uint64 // accounts in first-seen order
	groups := make(map[uint64][]int)
	for i := range batch {
		a := batch[i].Account
		if _, ok := groups[a]; !ok {
			order = append(order, a)
		}
		groups[a] = append(groups[a], i)
	}

	pipe := s.c.rdb.Pipeline()
	cmds := make([]*redis.Cmd, len(order))
	for g, a := range order {
		idx := groups[a]
		args := make([]any, 0, 1+5*len(idx))
		args = append(args, tag(a)+":")
		for _, i := range idx {
			op := &batch[i]
			switch op.Kind {
			case OpBind: // price carries the order id
				args = append(args, int(OpBind), op.Seq, op.Client, op.OrderID, 0)
			case OpFill:
				args = append(args, int(OpFill), op.Seq, op.OrderID, op.Price, op.Qty)
			case OpCancel:
				args = append(args, int(OpCancel), op.Seq, op.OrderID, 0, 0)
			default:
				args = append(args, int(OpRelease), op.Seq, op.Client, 0, 0)
			}
		}
		cmds[g] = settleScript.EvalSha(ctx, pipe, []string{balKey(a), s.c.seqKey(a)}, args...)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		for _, c := range cmds {
			if c.Err() != nil && strings.HasPrefix(c.Err().Error(), "NOSCRIPT") {
				// Redis lost its script cache (restart or SCRIPT FLUSH): reload, retry the batch.
				if lerr := s.c.loadScripts(ctx); lerr != nil {
					return lerr
				}
				break
			}
		}
		return err
	}

	now := time.Now().UnixNano()
	for g, a := range order {
		codes, err := cmds[g].Int64Slice()
		if err != nil {
			return err
		}
		for k, i := range groups[a] {
			s.applied.Inc()
			s.lag.Observe(float64(now-batch[i].at) / 1e9)
			switch codes[k] {
			case 2:
				s.anomalies.WithLabelValues("missing_reservation").Inc()
			case 3:
				s.anomalies.WithLabelValues("fill_exceeds_reservation").Inc()
				s.log.Error("fill exceeds reservation", "seq", batch[i].Seq, "account", a)
			}
		}
	}
	return nil
}

func (s *settler) flush(ctx context.Context) error {
	for s.fin.Load() < s.enq.Load() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Millisecond):
		}
	}
	return nil
}
