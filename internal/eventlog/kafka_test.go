package eventlog_test

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/itsdamian/mini-exchange/internal/engine"
	"github.com/itsdamian/mini-exchange/internal/eventlog"
	"github.com/itsdamian/mini-exchange/internal/eventlog/kafkatest"
	"github.com/itsdamian/mini-exchange/internal/pipeline"
)

func TestMain(m *testing.M) {
	code := m.Run()
	kafkatest.Terminate()
	os.Exit(code)
}

type rig struct {
	brokers  []string
	symbol   string
	store    *eventlog.SnapshotStore
	follower *eventlog.Follower
	pub      *eventlog.Publisher
	producer interface{ Close() }
}

// newRig creates the topic and a publisher feeding a follower, like the gateway does.
func newRig(t *testing.T, symbol string, snapEvery uint64, hw, capacity int) *rig {
	t.Helper()
	b := kafkatest.Shared(t)
	producer, err := eventlog.NewProducerClient(b.Brokers(), time.Millisecond, 1<<14)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	require.NoError(t, eventlog.EnsureTopic(ctx, producer, symbol, 1))

	r := &rig{brokers: b.Brokers(), symbol: symbol, producer: producer}
	r.store = &eventlog.SnapshotStore{Dir: t.TempDir(), Symbol: symbol, Keep: 10}
	r.follower = eventlog.NewFollower(symbol, engine.NewBook(bookCfg), -1, r.store, snapEvery, nil)
	r.pub = eventlog.NewPublisher(producer, eventlog.PublisherConfig{
		Symbol: symbol, HighWatermark: hw, QueueCapacity: capacity, OnDurable: r.follower.OnDurable,
	})
	return r
}

func (r *rig) closeAll(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	require.NoError(t, r.pub.Close(ctx))
	r.follower.Close()
}

func recoverLog(t *testing.T, r *rig, store *eventlog.SnapshotStore) (*engine.Book, *eventlog.RecoverStats) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	b, st, err := eventlog.Recover(ctx, r.brokers, r.symbol, store, bookCfg, nil)
	require.NoError(t, err)
	return b, st
}

func TestRecoveryFromSnapshotEqualsCleanReplay(t *testing.T) {
	r := newRig(t, "RECOVER", 1500, 4096, 8192)
	batches, live := history(t, 12_000, 7)
	events := 0
	for _, evs := range batches {
		events += len(evs)
		for r.pub.Backlogged() {
			time.Sleep(time.Millisecond)
		}
		r.pub.Append(evs)
	}
	r.closeAll(t) // flushes Kafka, writes the final snapshot

	clean, cst := recoverLog(t, r, nil)
	require.Equal(t, live.Seq(), cst.LastSeq)
	require.Equal(t, live.Hash(), clean.Hash(), "clean replay of the log must equal the engine's book")
	require.Equal(t, events, cst.Events)
	require.False(t, cst.FromSnapshot)

	fromSnap, sst := recoverLog(t, r, r.store)
	require.True(t, sst.FromSnapshot)
	require.Zero(t, sst.Events, "the final snapshot is at the log end: nothing to replay")
	require.Equal(t, clean.Hash(), fromSnap.Hash())

	// drop the newest snapshot: recovery must fall back to an older one and
	// replay the tail, landing on the same state
	files, _ := filepath.Glob(filepath.Join(r.store.Dir, "snapshot-RECOVER-*.pb"))
	require.Greater(t, len(files), 2)
	require.NoError(t, os.Remove(files[len(files)-1]))
	tail, tst := recoverLog(t, r, r.store)
	require.True(t, tst.FromSnapshot)
	require.Positive(t, tst.Events)
	require.Less(t, tst.Events, events, "replay only the events after the snapshot")
	require.Equal(t, clean.Hash(), tail.Hash())
	require.Equal(t, clean.Seq(), tail.Seq())
}

func TestRecoverRefusesSnapshotAheadOfLog(t *testing.T) {
	r := newRig(t, "AHEAD", 0, 4096, 8192)
	batches, _ := history(t, 200, 8)
	for _, evs := range batches[:100] {
		r.pub.Append(evs)
	}
	r.closeAll(t)

	// A snapshot claiming events the log does not hold (e.g. restored from a
	// different environment): recovery must refuse, not start from it.
	b := engine.NewBook(bookCfg)
	for _, evs := range batches {
		require.NoError(t, b.ApplyEvents(evs))
	}
	require.NoError(t, r.store.Write(eventlog.BuildSnapshot("AHEAD", b, 5000)))
	_, _, err := eventlog.Recover(context.Background(), r.brokers, "AHEAD", r.store, bookCfg, nil)
	require.ErrorContains(t, err, "ahead of the log")
}

func TestRecoverOfMissingTopicIsEmptyBook(t *testing.T) {
	b := kafkatest.Shared(t)
	book, st, err := eventlog.Recover(context.Background(), b.Brokers(), "NEVERCREATED", nil, bookCfg, nil)
	require.NoError(t, err)
	require.Zero(t, book.Seq())
	require.Zero(t, st.Events)
}

// FR-4 acceptance: Kafka goes away during load; no event is lost or
// duplicated after it returns (sequence continuity plus exact event count).
func TestKafkaOutageLosesAndDuplicatesNothing(t *testing.T) {
	for _, mode := range []string{"stop", "kill"} {
		t.Run(mode, func(t *testing.T) {
			r := newRig(t, "OUTAGE-"+mode, 0, 300, 5000)
			broker := kafkatest.Shared(t)
			batches, live := history(t, 6000, 9)
			total := 0
			for _, evs := range batches {
				total += len(evs)
			}

			sent := 0
			append1 := func() {
				r.pub.Append(batches[sent])
				sent++
			}
			for sent < 1000 { // healthy phase
				append1()
			}
			require.Eventually(t, func() bool { return r.pub.Pending() == 0 }, 30*time.Second, 10*time.Millisecond, "healthy phase must drain")

			ctx := context.Background()
			if mode == "stop" {
				require.NoError(t, broker.Stop(ctx))
			} else {
				require.NoError(t, broker.Kill(ctx))
			}

			// Kafka is down: keep submitting, as the gateway would, until the
			// backlog says to shed load.
			for !r.pub.Backlogged() && sent < len(batches) {
				append1()
				time.Sleep(50 * time.Microsecond)
			}
			require.True(t, r.pub.Backlogged(), "with Kafka down, the backlog must reach the high watermark and trigger load shedding")
			require.Greater(t, r.pub.Pending(), int64(0))
			require.NoError(t, r.pub.Err(), "an outage is not a permanent failure")

			require.NoError(t, broker.Restart(ctx))
			require.Eventually(t, func() bool { return r.pub.Pending() == 0 }, 2*time.Minute, 50*time.Millisecond, "backlog must drain after Kafka returns")
			require.False(t, r.pub.Backlogged())

			for sent < len(batches) { // and traffic continues normally afterwards
				append1()
				for r.pub.Backlogged() {
					time.Sleep(time.Millisecond)
				}
			}
			r.closeAll(t)

			clean, st := recoverLog(t, r, nil) // enforces continuity: no gap, no duplicate
			require.Equal(t, total, st.Events, "every event published exactly once")
			require.Equal(t, live.Seq(), st.LastSeq)
			require.Equal(t, live.Hash(), clean.Hash())
			require.Equal(t, live.Seq(), r.follower.AckedSeqForTest())
		})
	}
}

// Engine crash and restart through the real pipeline: the producer is cut off
// without flushing (the unacknowledged tail is lost, which is the documented
// window of an asynchronous log), the state is recovered from snapshot + log,
// the engine resumes, and the final log still replays cleanly with no gaps.
func TestEngineCrashRecoveryResumesWithoutGapsOrDuplicates(t *testing.T) {
	symbol := "CRASH"
	b := kafkatest.Shared(t)
	store := &eventlog.SnapshotStore{Dir: t.TempDir(), Symbol: symbol, Keep: 5}
	ctx := context.Background()

	start := func(restore *pipeline.Restored, from int64) (*pipeline.Pipeline, *eventlog.Publisher, *eventlog.Follower, func()) {
		producer, err := eventlog.NewProducerClient(b.Brokers(), time.Millisecond, 1<<14)
		require.NoError(t, err)
		require.NoError(t, eventlog.EnsureTopic(ctx, producer, symbol, 1))
		var book *engine.Book
		if restore != nil {
			book = restore.Book
		} else {
			book = engine.NewBook(bookCfg)
		}
		fol := eventlog.NewFollower(symbol, book.Clone(bookCfg), from, store, 2000, nil)
		pub := eventlog.NewPublisher(producer, eventlog.PublisherConfig{Symbol: symbol, HighWatermark: 4096, QueueCapacity: 8192 + 4096, OnDurable: fol.OnDurable})
		p := pipeline.New(pipeline.Config{Book: bookCfg, Restore: restore, Log: pub, MaxInFlight: 4096})
		return p, pub, fol, producer.Close
	}
	drive := func(p *pipeline.Pipeline, n int, seed uint64, idBase uint64) {
		var wg sync.WaitGroup
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < n/4; i++ {
					side := engine.Buy
					if (i+w)%2 == 0 {
						side = engine.Sell
					}
					price := int64(1000) + int64((i*7+w)%20) - 10
					_, err := p.Submit(ctx, engine.Command{Kind: engine.CmdPlace, Side: side, Type: engine.Limit, TIF: engine.GTC,
						AccountID: uint64(1 + (i+w)%9), Price: price, Qty: int64(1 + i%5)}, "")
					if err != nil && err != pipeline.ErrLogBacklogged && err != pipeline.ErrOverloaded {
						t.Errorf("submit: %v", err)
						return
					}
				}
			}()
		}
		wg.Wait()
	}

	// ---- first life: run, then crash
	p1, pub1, fol1, crash := start(nil, -1)
	drive(p1, 8000, 1, 0)
	require.Eventually(t, func() bool { return pub1.Pending() == 0 }, 30*time.Second, 5*time.Millisecond)
	drive(p1, 2000, 2, 0) // a final burst that we then abandon mid-flight
	crash()               // abrupt: no flush, no graceful stop, no final snapshot
	_ = pub1
	_ = fol1

	// Records that were already on their way when the producer was cut off can
	// still land in the log a moment later. Both recoveries below must see the
	// same log, so wait until its end stops moving (otherwise the second one
	// can legitimately read more than the first).
	waitLogStable(t, b.Brokers(), symbol)

	// ---- recovery: snapshot + tail must equal a clean replay
	ropts := func() (*engine.Book, *eventlog.RecoverStats) {
		bk, st, err := eventlog.Recover(ctx, b.Brokers(), symbol, store, bookCfg, nil)
		require.NoError(t, err)
		return bk, st
	}
	recovered, st := ropts()
	clean, cst, err := eventlog.Recover(ctx, b.Brokers(), symbol, nil, bookCfg, nil)
	require.NoError(t, err)
	require.Equal(t, cst.BookHash, st.BookHash, "snapshot+replay must equal clean replay")
	require.Equal(t, cst.LastSeq, st.LastSeq)
	require.True(t, st.FromSnapshot, "periodic snapshots were taken during the first life")
	require.Equal(t, clean.Hash(), recovered.Hash())
	lastBeforeRestart := st.LastSeq
	require.Positive(t, lastBeforeRestart)

	// ---- second life: resume from recovered state
	p2, pub2, fol2, _ := start(&pipeline.Restored{Book: recovered, NextOrderID: recovered.MaxOrderID() + 1}, st.LastOffset)
	drive(p2, 6000, 3, 0)
	require.Eventually(t, func() bool { return pub2.Pending() == 0 }, 30*time.Second, 5*time.Millisecond)
	stopCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	require.NoError(t, p2.Stop(stopCtx))
	require.NoError(t, pub2.Close(stopCtx))
	fol2.Close()

	final, fst, err := eventlog.Recover(ctx, b.Brokers(), symbol, nil, bookCfg, nil) // enforces continuity across the restart
	require.NoError(t, err)
	require.Greater(t, fst.LastSeq, lastBeforeRestart, "the engine resumed and kept producing")
	require.Equal(t, p2.Stats().LastSeq, fst.LastSeq, "log end equals what the engine produced")
	require.Equal(t, fol2.BookHash(), final.Hash(), "durable follower equals clean replay of the final log")
}

// waitLogStable returns once the end offset of the symbol's log has not
// changed for a second.
func waitLogStable(t *testing.T, brokers []string, symbol string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer cl.Close()
	adm := kadm.NewClient(cl)
	end := func() int64 {
		ends, err := adm.ListEndOffsets(context.Background(), eventlog.Topic(symbol))
		require.NoError(t, err)
		o, _ := ends.Lookup(eventlog.Topic(symbol), 0)
		return o.Offset
	}
	last, since := end(), time.Now()
	for time.Since(since) < time.Second {
		time.Sleep(100 * time.Millisecond)
		if cur := end(); cur != last {
			last, since = cur, time.Now()
		}
	}
}
