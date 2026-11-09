package ledger_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/itsdamian/mini-exchange/internal/engine"
	"github.com/itsdamian/mini-exchange/internal/enginetest"
	"github.com/itsdamian/mini-exchange/internal/eventlog"
	"github.com/itsdamian/mini-exchange/internal/eventlog/kafkatest"
	"github.com/itsdamian/mini-exchange/internal/ledger"
	"github.com/itsdamian/mini-exchange/internal/ledger/pgtest"
)

var bookCfg = engine.Config{MaxOrders: 4096, MaxLevels: 256}

// history runs a random command stream through a real book and returns the
// events of every command (one batch per command, as the log stores them).
func history(n int, seed uint64) [][]engine.Event {
	cmds := enginetest.Generate(n, seed, enginetest.DefaultProfile())
	book := engine.NewBook(bookCfg)
	var out [][]engine.Event
	for i := range cmds {
		c := cmds[i]
		c.Ts = time.Now().UnixNano()
		if evs := book.Apply(&c, nil); len(evs) > 0 {
			out = append(out, evs)
		}
	}
	return out
}

func countTrades(batches [][]engine.Event) int {
	n := 0
	for _, evs := range batches {
		for _, e := range evs {
			if e.Kind == engine.EvTrade {
				n++
			}
		}
	}
	return n
}

type topicRig struct {
	t       *testing.T
	symbol  string
	brokers []string
	prod    *kgo.Client
}

// newTopic creates the symbol's event topic and a plain producer to write raw
// records into it.
func newTopic(t *testing.T, symbol string) *topicRig {
	t.Helper()
	b := kafkatest.Shared(t)
	prod, err := eventlog.NewProducerClient(b.Brokers(), time.Millisecond, 1<<16)
	require.NoError(t, err)
	t.Cleanup(prod.Close)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	require.NoError(t, eventlog.EnsureTopic(ctx, prod, symbol, 1))
	return &topicRig{t: t, symbol: symbol, brokers: b.Brokers(), prod: prod}
}

func (r *topicRig) publishRaw(value []byte) {
	r.t.Helper()
	res := r.prod.ProduceSync(context.Background(), &kgo.Record{Topic: eventlog.Topic(r.symbol), Value: value})
	require.NoError(r.t, res.FirstErr())
}

func (r *topicRig) publish(batches [][]engine.Event) {
	r.t.Helper()
	for _, evs := range batches {
		raw, err := proto.Marshal(eventlog.ToProtoBatch(r.symbol, evs))
		require.NoError(r.t, err)
		r.prod.Produce(context.Background(), &kgo.Record{Topic: eventlog.Topic(r.symbol), Value: raw}, func(_ *kgo.Record, err error) {
			if err != nil {
				r.t.Errorf("produce: %v", err)
			}
		})
	}
	require.NoError(r.t, r.prod.Flush(context.Background()))
}

type runningConsumer struct {
	c      *ledger.Consumer
	reg    *prometheus.Registry
	cancel context.CancelFunc
	done   chan struct{}
}

func startConsumer(t *testing.T, s *ledger.Store, r *topicRig, group string, fees ledger.Fees) *runningConsumer {
	t.Helper()
	reg := prometheus.NewRegistry()
	c, err := ledger.NewConsumer(context.Background(), s, ledger.ConsumerConfig{
		Brokers: r.brokers, Symbol: r.symbol, Group: group, Fees: fees, Registerer: reg,
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	rc := &runningConsumer{c: c, reg: reg, cancel: cancel, done: make(chan struct{})}
	go func() { defer close(rc.done); _ = c.Run(ctx) }()
	t.Cleanup(rc.stop)
	return rc
}

func (rc *runningConsumer) stop() {
	rc.cancel()
	<-rc.done
	rc.c.Close()
}

func (rc *runningConsumer) counter(t *testing.T, name string) float64 {
	t.Helper()
	mfs, err := rc.reg.Gather()
	require.NoError(t, err)
	var sum float64
	for _, mf := range mfs {
		if mf.GetName() == name {
			for _, m := range mf.GetMetric() {
				sum += m.GetCounter().GetValue()
			}
		}
	}
	return sum
}

func fundAll(t *testing.T, s *ledger.Store, symbol string, accts int) {
	t.Helper()
	a, err := ledger.SplitSymbol(symbol)
	require.NoError(t, err)
	for id := uint64(1); id <= uint64(accts); id++ {
		p, err := ledger.DepositPosting(fmt.Sprintf("seed-%s-%d", symbol, id), symbol, a, id, 1_000_000_000, 1_000_000_000_000_000)
		require.NoError(t, err)
		_, err = s.Apply(context.Background(), []ledger.Posting{p})
		require.NoError(t, err)
	}
}

func reconcileClean(t *testing.T, s *ledger.Store, r *topicRig, fees ledger.Fees) *ledger.Report {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	rep, err := s.Reconcile(ctx, ledger.ReconcileConfig{Brokers: r.brokers, Symbol: r.symbol, Fees: fees, Wait: time.Minute})
	require.NoError(t, err)
	require.True(t, rep.OK(), "discrepancies: %+v", rep.Discrepancies)
	return rep
}

func TestConsumerSettlesTheLogAndReconcilesClean(t *testing.T) {
	const symbol = "AAA-USD"
	fees := ledger.Fees{TakerBps: 2, MakerBps: 1}
	s := openStore(t)
	r := newTopic(t, symbol)
	fundAll(t, s, symbol, 16)

	batches := history(6000, 11)
	trades := countTrades(batches)
	require.Greater(t, trades, 200, "the workload must trade")
	r.publish(batches)
	startConsumer(t, s, r, "g-"+symbol, fees)

	rep := reconcileClean(t, s, r, fees)
	require.Equal(t, trades, rep.LogTrades)
	require.Equal(t, trades, rep.LedgerTrades, "every trade has exactly one settlement")
}

// FR-6 acceptance: replaying the topic from the beginning yields the same
// balances, into the same ledger (all duplicates) and into a fresh one.
func TestReplayFromTheBeginningYieldsIdenticalBalances(t *testing.T) {
	const symbol = "BBB-USD"
	fees := ledger.Fees{TakerBps: 3, MakerBps: 1}
	s := openStore(t)
	r := newTopic(t, symbol)
	fundAll(t, s, symbol, 16)
	batches := history(5000, 21)
	trades := countTrades(batches)
	r.publish(batches)

	c1 := startConsumer(t, s, r, "g1-"+symbol, fees)
	reconcileClean(t, s, r, fees)
	first, err := s.Balances(context.Background())
	require.NoError(t, err)
	c1.stop()

	// Same ledger, a new group: every record is read again.
	c2 := startConsumer(t, s, r, "g2-"+symbol, fees)
	require.Eventually(t, func() bool {
		return c2.counter(t, "mx_ledger_trades_duplicate_total") >= float64(trades)
	}, time.Minute, 50*time.Millisecond, "the replay must see every trade again")
	require.Zero(t, c2.counter(t, "mx_ledger_trades_settled_total"), "and settle none of them twice")
	second, err := s.Balances(context.Background())
	require.NoError(t, err)
	require.Equal(t, first, second)

	// A brand new ledger given the same deposits reaches the same balances.
	s2 := openStore(t)
	fundAll(t, s2, symbol, 16)
	startConsumer(t, s2, r, "g3-"+symbol, fees)
	reconcileClean(t, s2, r, fees)
	third, err := s2.Balances(context.Background())
	require.NoError(t, err)
	require.Equal(t, first, third)
}

func dlqRecords(t *testing.T, r *topicRig, want int) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(r.brokers...),
		kgo.ConsumeTopics(ledger.DLQTopic(r.symbol)), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	require.NoError(t, err)
	defer cl.Close()
	var out []*kgo.Record
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for len(out) < want {
		fs := cl.PollFetches(ctx)
		require.NoError(t, ctx.Err(), "dead-letter topic holds %d of %d records", len(out), want)
		fs.EachRecord(func(rec *kgo.Record) { out = append(out, rec) })
	}
	return out
}

func header(rec *kgo.Record, key string) string {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func TestPoisonRecordsGoToTheDeadLetterTopicAndDoNotBlock(t *testing.T) {
	const symbol = "CCC-USD"
	s := openStore(t)
	r := newTopic(t, symbol)
	fundAll(t, s, symbol, 16)

	batches := history(3000, 31)
	trades := countTrades(batches)
	third := len(batches) / 3

	r.publish(batches[:third])
	r.publishRaw([]byte{0xff, 0xff, 0xff, 0xff}) // not protobuf

	r.publish(batches[third : 2*third])
	wrong := eventlog.ToProtoBatch("OTHER-USD", batches[0]) // a batch of another symbol
	raw, _ := proto.Marshal(wrong)
	r.publishRaw(raw)
	future := eventlog.ToProtoBatch(symbol, batches[0]) // a schema this build does not know
	future.SchemaVersion = 99
	raw, _ = proto.Marshal(future)
	r.publishRaw(raw)

	// A well-formed trade that the ledger must refuse: account 900 owns nothing.
	unfunded := []engine.Event{{Kind: engine.EvTrade, Seq: 1 << 40, Side: engine.Buy, AccountID: 900, MakerAccountID: 1, Price: 100, Qty: 1, Ts: 1}}
	raw, _ = proto.Marshal(eventlog.ToProtoBatch(symbol, unfunded))
	r.publishRaw(raw)
	badTrade := []engine.Event{{Kind: engine.EvTrade, Seq: 1<<40 + 1, Side: engine.Buy, AccountID: 1, MakerAccountID: 2, Price: 100, Qty: 0, Ts: 1}} // zero quantity
	raw, _ = proto.Marshal(eventlog.ToProtoBatch(symbol, badTrade))
	r.publishRaw(raw)

	r.publish(batches[2*third:])
	c := startConsumer(t, s, r, "g-"+symbol, ledger.Fees{})

	// The valid records on both sides of every poison record are all settled.
	require.Eventually(t, func() bool {
		n, _ := s.TradeCount(context.Background(), symbol)
		return n == trades
	}, time.Minute, 50*time.Millisecond, "good records behind poison records must still be settled")

	recs := dlqRecords(t, r, 5)
	var reasons []string
	for _, rec := range recs {
		reasons = append(reasons, header(rec, "reason"))
		require.Equal(t, eventlog.Topic(symbol), header(rec, "source-topic"))
		require.NotEmpty(t, header(rec, "source-offset"))
	}
	joined := strings.Join(reasons, "\n")
	require.Contains(t, joined, "decode")
	require.Contains(t, joined, "OTHER-USD")
	require.Contains(t, joined, "schema version 99")
	require.Contains(t, joined, "customer_not_negative")
	require.Contains(t, joined, "quantity 0")
	require.EqualValues(t, 5, c.counter(t, "mx_ledger_dead_letter_total"))
}

func TestReconcileFindsCorruption(t *testing.T) {
	const symbol = "DDD-USD"
	s := openStore(t)
	r := newTopic(t, symbol)
	fundAll(t, s, symbol, 16)
	r.publish(history(3000, 41))
	startConsumer(t, s, r, "g-"+symbol, ledger.Fees{})
	reconcileClean(t, s, r, ledger.Fees{})

	ctx := context.Background()
	exec := func(q string, args ...any) {
		t.Helper()
		_, err := s.Pool().Exec(ctx, q, args...)
		require.NoError(t, err)
	}
	kinds := func() map[string]int {
		rep, err := s.Reconcile(ctx, ledger.ReconcileConfig{Brokers: r.brokers, Symbol: symbol})
		require.NoError(t, err)
		m := map[string]int{}
		for _, d := range rep.Discrepancies {
			m[d.Kind]++
		}
		return m
	}

	// A balance edited behind the ledger's back.
	exec(`UPDATE accounts SET amount = amount + 5 WHERE acct_kind = 'customer' AND owner = 3 AND asset = 'DDD'`)
	k := kinds()
	require.Positive(t, k[ledger.DBalanceVsLog])
	require.Positive(t, k[ledger.DBalanceVsEntries])
	require.Positive(t, k[ledger.DAssetNotZero])
	exec(`UPDATE accounts SET amount = amount - 5 WHERE acct_kind = 'customer' AND owner = 3 AND asset = 'DDD'`)
	require.Empty(t, kinds())

	// A settlement that disappeared (entries first: they reference it).
	var seq int64
	require.NoError(t, s.Pool().QueryRow(ctx, `SELECT seq FROM settlements WHERE kind = 'trade' ORDER BY seq LIMIT 1`).Scan(&seq))
	id := ledger.TradeEventID(symbol, uint64(seq))
	exec(`DELETE FROM entries WHERE event_id = $1`, id)
	exec(`DELETE FROM settlements WHERE event_id = $1`, id)
	k = kinds()
	require.Equal(t, 1, k[ledger.DMissingSettlement])
	require.Positive(t, k[ledger.DBalanceVsEntries])

	// A settlement for a trade the log never had, without entries.
	exec(`INSERT INTO settlements (event_id, kind, symbol, seq) VALUES ('ghost', 'trade', $1, 999999999)`, symbol)
	k = kinds()
	require.Equal(t, 1, k[ledger.DOrphanSettlement])
	require.Equal(t, 1, k[ledger.DEmptySettlement])
}

// The database going away mid-stream must lose nothing and settle nothing twice.
func TestPostgresOutageLosesAndDuplicatesNothing(t *testing.T) {
	const symbol = "EEE-USD"
	pg := pgtest.Shared(t)
	s := openStore(t)
	r := newTopic(t, symbol)
	fundAll(t, s, symbol, 16)
	batches := history(8000, 51)
	trades := countTrades(batches)

	r.publish(batches[:3000])
	c := startConsumer(t, s, r, "g-"+symbol, ledger.Fees{})
	require.Eventually(t, func() bool {
		n, _ := s.TradeCount(context.Background(), symbol)
		return n > 50
	}, time.Minute, 20*time.Millisecond, "settling before the outage")

	ctx := context.Background()
	require.NoError(t, pg.Stop(ctx))
	r.publish(batches[3000:])
	require.Eventually(t, func() bool { return c.counter(t, "mx_ledger_retries_total") > 0 },
		30*time.Second, 50*time.Millisecond, "the consumer must notice and retry, not skip")
	require.NoError(t, pg.Restart(ctx))

	rep := reconcileClean(t, s, r, ledger.Fees{})
	require.Equal(t, trades, rep.LedgerTrades)
}
