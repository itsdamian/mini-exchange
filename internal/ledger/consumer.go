package ledger

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/itsdamian/mini-exchange/internal/engine"
	"github.com/itsdamian/mini-exchange/internal/eventlog"

	exchangev1 "github.com/itsdamian/mini-exchange/gen/exchange/v1"
)

// DLQTopic is the dead-letter topic of a symbol's event log.
func DLQTopic(symbol string) string { return eventlog.Topic(symbol) + ".dlq" }

// ConsumerConfig configures a Consumer.
type ConsumerConfig struct {
	Brokers     []string
	Symbol      string
	Group       string // consumer group; a new group replays the log from the start
	Fees        Fees
	Replication int16 // replication factor of the dead-letter topic
	Registerer  prometheus.Registerer
	Log         *slog.Logger
}

// Consumer settles the trades of one symbol's event log into the ledger.
//
// Delivery is at-least-once (offsets are committed after the database
// transaction), and the effect is exactly-once because settlement is keyed by
// the trade's event id. A record that can never be applied goes to the
// dead-letter topic with the reason and the consumer moves on; a transient
// failure (database down) is retried without committing, so nothing is lost
// and nothing is skipped.
type Consumer struct {
	cfg    ConsumerConfig
	store  *Store
	assets Assets
	cl     *kgo.Client
	dlq    *kgo.Client
	log    *slog.Logger

	trades    prometheus.Counter
	dups      prometheus.Counter
	deadLett  *prometheus.CounterVec
	retries   prometheus.Counter
	lag       prometheus.Histogram
	lastOK    prometheus.Gauge
	processed prometheus.Counter
}

// NewConsumer connects to Kafka and makes sure the dead-letter topic exists.
func NewConsumer(ctx context.Context, store *Store, cfg ConsumerConfig) (*Consumer, error) {
	a, err := SplitSymbol(cfg.Symbol)
	if err != nil {
		return nil, err
	}
	if cfg.Group == "" {
		return nil, errors.New("ledger: consumer group is required")
	}
	if cfg.Replication <= 0 {
		cfg.Replication = 1
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(eventlog.Topic(cfg.Symbol)),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxBytes(16<<20),
	)
	if err != nil {
		return nil, err
	}
	dlq, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.RecordRetries(1<<30))
	if err != nil {
		cl.Close()
		return nil, err
	}
	if err := ensureTopic(ctx, dlq, DLQTopic(cfg.Symbol), cfg.Replication); err != nil {
		cl.Close()
		dlq.Close()
		return nil, err
	}
	reg := promauto.With(cfg.Registerer)
	c := &Consumer{
		cfg: cfg, store: store, assets: a, cl: cl, dlq: dlq, log: cfg.Log.With("component", "ledger"),
		trades:    reg.NewCounter(prometheus.CounterOpts{Name: "mx_ledger_trades_settled_total", Help: "Trades newly settled in the ledger."}),
		dups:      reg.NewCounter(prometheus.CounterOpts{Name: "mx_ledger_trades_duplicate_total", Help: "Trades skipped because their event id was settled before (replays)."}),
		deadLett:  reg.NewCounterVec(prometheus.CounterOpts{Name: "mx_ledger_dead_letter_total", Help: "Records sent to the dead-letter topic, by reason class."}, []string{"reason"}),
		retries:   reg.NewCounter(prometheus.CounterOpts{Name: "mx_ledger_retries_total", Help: "Transient failures retried (database or Kafka unavailable)."}),
		processed: reg.NewCounter(prometheus.CounterOpts{Name: "mx_ledger_records_total", Help: "Event log records consumed (settled, skipped or dead-lettered)."}),
		lag: reg.NewHistogram(prometheus.HistogramOpts{Name: "mx_ledger_settle_lag_seconds",
			Help: "Time from the engine event to its record being committed in PostgreSQL.", Buckets: prometheus.ExponentialBuckets(1e-3, 2, 18)}),
		lastOK: reg.NewGauge(prometheus.GaugeOpts{Name: "mx_ledger_last_settle_timestamp_seconds", Help: "Unix time of the last committed record."}),
	}
	return c, nil
}

// Close stops the Kafka clients.
func (c *Consumer) Close() {
	c.cl.Close()
	c.dlq.Close()
}

// Run consumes until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		fetches := c.cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if fetches.IsClientClosed() {
			return nil
		}
		for _, fe := range fetches.Errors() {
			if errors.Is(fe.Err, kerr.UnknownTopicOrPartition) {
				// the log does not exist yet (the gateway creates it): wait.
				sleep(ctx, 200*time.Millisecond)
				continue
			}
			c.log.Warn("fetch error", "topic", fe.Topic, "err", fe.Err)
		}
		if recs := fetches.Records(); len(recs) > 0 && c.handleRecords(ctx, recs) {
			c.commit(ctx, recs[len(recs)-1])
		}
	}
}

func (c *Consumer) commit(ctx context.Context, last *kgo.Record) {
	backoff := 50 * time.Millisecond
	for ctx.Err() == nil {
		if err := c.cl.CommitRecords(ctx, last); err == nil {
			return
		} else {
			c.retries.Inc()
			c.log.Warn("offset commit failed, retrying", "err", err)
		}
		sleep(ctx, backoff)
		backoff = min(backoff*2, 2*time.Second)
	}
}

// maxGroup bounds how many records share one database transaction.
const maxGroup = 256

type decoded struct {
	rec *kgo.Record
	evs []engine.Event
	ps  []Posting
}

// handleRecords settles or dead-letters the records of one poll, in log order.
// Consecutive good records are applied together in one transaction, which
// shares the cost of a commit. It returns false only when ctx ended before
// everything was handled (the offsets must then not be committed).
func (c *Consumer) handleRecords(ctx context.Context, recs []*kgo.Record) bool {
	group := make([]decoded, 0, min(len(recs), maxGroup))
	flush := func() bool {
		ok := c.settleGroup(ctx, group)
		group = group[:0]
		return ok
	}
	for _, r := range recs {
		evs, ps, err := c.decode(r)
		if err != nil {
			if !flush() || !c.deadLetter(ctx, r, err) {
				return false
			}
			c.processed.Inc()
			continue
		}
		group = append(group, decoded{r, evs, ps})
		if len(group) == maxGroup && !flush() {
			return false
		}
	}
	return flush()
}

// settleGroup applies the group in one transaction. If the database refuses
// it as poison, the records are applied one at a time so that only the bad
// record is dead-lettered and the good ones around it still settle.
func (c *Consumer) settleGroup(ctx context.Context, group []decoded) bool {
	if len(group) == 0 {
		return true
	}
	var all []Posting
	for i := range group {
		all = append(all, group[i].ps...)
	}
	err := c.applyWithRetry(ctx, all)
	if err == nil {
		for i := range group {
			c.done(group[i].evs)
		}
		return true
	}
	if ctx.Err() != nil {
		return false
	}
	for i := range group {
		err := c.applyWithRetry(ctx, group[i].ps)
		if err == nil {
			c.done(group[i].evs)
			continue
		}
		// Only poison reaches here: applyWithRetry retries everything else.
		if ctx.Err() != nil || !c.deadLetter(ctx, group[i].rec, err) {
			return false
		}
		c.processed.Inc()
	}
	return true
}

func (c *Consumer) done(evs []engine.Event) {
	c.observe(evs)
	c.processed.Inc()
}

func (c *Consumer) decode(r *kgo.Record) ([]engine.Event, []Posting, error) {
	var b exchangev1.EventBatch
	if err := proto.Unmarshal(r.Value, &b); err != nil {
		return nil, nil, poison("decode: %v", err)
	}
	evs, err := eventlog.FromProtoBatch(&b, c.cfg.Symbol)
	if err != nil {
		return nil, nil, poison("invalid batch: %v", err)
	}
	var ps []Posting
	for i := range evs {
		if evs[i].Kind != engine.EvTrade {
			continue
		}
		p, err := TradePosting(c.cfg.Symbol, c.assets, c.cfg.Fees, evs[i])
		if err != nil {
			return nil, nil, err
		}
		ps = append(ps, p)
	}
	return evs, ps, nil
}

// applyWithRetry applies the record's postings in one transaction. Transient
// errors are retried forever with backoff; a poison error is returned.
func (c *Consumer) applyWithRetry(ctx context.Context, ps []Posting) error {
	if len(ps) == 0 {
		return nil
	}
	backoff := 50 * time.Millisecond
	for {
		n, err := c.store.Apply(ctx, ps)
		if err == nil {
			c.trades.Add(float64(n))
			c.dups.Add(float64(len(ps) - n))
			return nil
		}
		if IsPoison(err) || ctx.Err() != nil {
			return err
		}
		c.retries.Inc()
		c.log.Warn("settlement failed, retrying (nothing is skipped)", "err", err, "backoff", backoff)
		sleep(ctx, backoff)
		backoff = min(backoff*2, 2*time.Second)
	}
}

func (c *Consumer) observe(evs []engine.Event) {
	now := time.Now()
	if n := len(evs); n > 0 && evs[n-1].Ts > 0 {
		c.lag.Observe(float64(now.UnixNano()-evs[n-1].Ts) / 1e9)
	}
	c.lastOK.Set(float64(now.Unix()))
}

// deadLetter publishes the original record with the reason to the DLQ topic
// and returns once it is durable. It returns false if ctx ended first.
func (c *Consumer) deadLetter(ctx context.Context, r *kgo.Record, cause error) bool {
	class := "invalid"
	var pe *PoisonError
	if !errors.As(cause, &pe) {
		class = "constraint"
	}
	c.deadLett.WithLabelValues(class).Inc()
	c.log.Error("poison record sent to the dead-letter topic", "offset", r.Offset, "reason", cause.Error())
	rec := &kgo.Record{
		Topic: DLQTopic(c.cfg.Symbol), Key: r.Key, Value: r.Value,
		Headers: []kgo.RecordHeader{
			{Key: "reason", Value: []byte(cause.Error())},
			{Key: "source-topic", Value: []byte(r.Topic)},
			{Key: "source-partition", Value: []byte(strconv.Itoa(int(r.Partition)))},
			{Key: "source-offset", Value: []byte(strconv.FormatInt(r.Offset, 10))},
		},
	}
	if err := c.dlq.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return false
	}
	return true
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// ensureTopic creates a one-partition topic with default retention if absent.
func ensureTopic(ctx context.Context, cl *kgo.Client, name string, replication int16) error {
	resp, err := kadm.NewClient(cl).CreateTopic(ctx, 1, replication, nil, name)
	if err != nil {
		if errors.Is(err, kerr.TopicAlreadyExists) || (resp.Err != nil && errors.Is(resp.Err, kerr.TopicAlreadyExists)) {
			return nil
		}
		return fmt.Errorf("create topic %s: %w", name, err)
	}
	return nil
}
