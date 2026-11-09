package pipeline_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/itsdamian/mini-exchange/internal/engine"
	"github.com/itsdamian/mini-exchange/internal/pipeline"
)

func newPipeline(t *testing.T, cfg pipeline.Config) *pipeline.Pipeline {
	t.Helper()
	p := pipeline.New(cfg)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, p.Stop(ctx))
	})
	return p
}

func limit(acct uint64, side engine.Side, price, qty int64) engine.Command {
	return engine.Command{Kind: engine.CmdPlace, Side: side, Type: engine.Limit, TIF: engine.GTC, AccountID: acct, Price: price, Qty: qty}
}

func recv(t *testing.T, s *pipeline.Subscription) pipeline.Report {
	t.Helper()
	select {
	case r, ok := <-s.C:
		require.True(t, ok, "subscription closed: %v", s.Err())
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a report")
		return pipeline.Report{}
	}
}

func TestSubmitAssignsOrderIDsAndTimestamps(t *testing.T) {
	p := newPipeline(t, pipeline.Config{})
	ctx := context.Background()
	e1, err := p.Submit(ctx, limit(1, engine.Buy, 100, 5), "a")
	require.NoError(t, err)
	e2, err := p.Submit(ctx, limit(1, engine.Buy, 99, 5), "b")
	require.NoError(t, err)
	require.Equal(t, engine.EvAccepted, e1.Kind)
	require.Equal(t, uint64(1), e1.OrderID)
	require.Equal(t, uint64(2), e2.OrderID)
	require.Greater(t, e2.Ts, int64(0))
	require.GreaterOrEqual(t, e2.Ts, e1.Ts)
	require.Greater(t, e2.Seq, e1.Seq)
}

func TestRejectedCommandReturnsRejectEvent(t *testing.T) {
	p := newPipeline(t, pipeline.Config{})
	ev, err := p.Submit(context.Background(), limit(1, engine.Buy, 0, 5), "bad")
	require.NoError(t, err)
	require.Equal(t, engine.EvRejected, ev.Kind)
	require.Equal(t, engine.RejectInvalidPrice, ev.Reject)
}

func TestTradeProducesReportsForBothSides(t *testing.T) {
	p := newPipeline(t, pipeline.Config{})
	ctx := context.Background()
	maker, err := p.Subscribe(1)
	require.NoError(t, err)
	taker, err := p.Subscribe(2)
	require.NoError(t, err)

	_, err = p.Submit(ctx, limit(1, engine.Sell, 100, 10), "maker-1")
	require.NoError(t, err)
	_, err = p.Submit(ctx, limit(2, engine.Buy, 100, 4), "taker-1")
	require.NoError(t, err)

	r := recv(t, maker)
	require.Equal(t, pipeline.ReportNew, r.Type)
	require.Equal(t, "maker-1", r.ClientOrderID)
	require.Equal(t, int64(10), r.Leaves)
	r = recv(t, maker)
	require.Equal(t, pipeline.ReportTrade, r.Type)
	require.True(t, r.IsMaker)
	require.Equal(t, engine.Sell, r.Side)
	require.Equal(t, int64(4), r.Qty)
	require.Equal(t, int64(6), r.Leaves)
	require.Equal(t, "maker-1", r.ClientOrderID)

	r = recv(t, taker)
	require.Equal(t, pipeline.ReportNew, r.Type)
	r = recv(t, taker)
	require.Equal(t, pipeline.ReportTrade, r.Type)
	require.False(t, r.IsMaker)
	require.Equal(t, engine.Buy, r.Side)
	require.Equal(t, int64(100), r.Price)
	require.Equal(t, int64(0), r.Leaves, "taker fully filled")
	require.Equal(t, "taker-1", r.ClientOrderID)

	st := p.Stats()
	require.Equal(t, uint64(2), st.Accepted)
	require.Equal(t, uint64(1), st.Trades)
}

func TestCancelReport(t *testing.T) {
	p := newPipeline(t, pipeline.Config{})
	ctx := context.Background()
	sub, _ := p.Subscribe(1)
	acc, err := p.Submit(ctx, limit(1, engine.Buy, 100, 7), "c1")
	require.NoError(t, err)
	ev, err := p.Submit(ctx, engine.Command{Kind: engine.CmdCancel, OrderID: acc.OrderID, AccountID: 1}, "c1")
	require.NoError(t, err)
	require.Equal(t, engine.EvCanceled, ev.Kind)
	require.Equal(t, int64(7), ev.Qty)

	require.Equal(t, pipeline.ReportNew, recv(t, sub).Type)
	r := recv(t, sub)
	require.Equal(t, pipeline.ReportCanceled, r.Type)
	require.Equal(t, "c1", r.ClientOrderID)
	require.Equal(t, int64(7), r.Qty)
}

func TestConcurrentSubmittersAllGetDistinctOrders(t *testing.T) {
	p := newPipeline(t, pipeline.Config{})
	const n = 2000
	ids := make(chan uint64, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ev, err := p.Submit(context.Background(), limit(uint64(i%7+1), engine.Buy, int64(100+i%5), 1), "x")
			if err == nil {
				ids <- ev.OrderID
			}
		}()
	}
	wg.Wait()
	close(ids)
	seen := map[uint64]bool{}
	for id := range ids {
		require.False(t, seen[id], "duplicate order id %d", id)
		seen[id] = true
	}
	require.Len(t, seen, n)
	require.Equal(t, uint64(n), p.Stats().Accepted)
}

func TestOverloadReturnsErrorInsteadOfBlocking(t *testing.T) {
	p := newPipeline(t, pipeline.Config{InputSize: 2, MaxInFlight: 2})
	var wg sync.WaitGroup
	var mu sync.Mutex
	overloaded := 0
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := p.Submit(context.Background(), limit(1, engine.Buy, 100, 1), "x")
			if err == pipeline.ErrOverloaded {
				mu.Lock()
				overloaded++
				mu.Unlock()
			} else {
				require.NoError(t, err)
			}
		}()
	}
	wg.Wait()
	require.Positive(t, overloaded, "with 2 in-flight slots, 200 concurrent submits must shed load")
}

func TestCanceledContextReleasesTicket(t *testing.T) {
	p := newPipeline(t, pipeline.Config{MaxInFlight: 4})
	for i := 0; i < 50; i++ { // far more than MaxInFlight: tickets must come back
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, _ = p.Submit(ctx, limit(1, engine.Buy, 100, 1), "x")
	}
	require.Eventually(t, func() bool {
		_, err := p.Submit(context.Background(), limit(1, engine.Buy, 100, 1), "x")
		return err == nil
	}, 5*time.Second, 5*time.Millisecond, "tickets leaked")
}

func TestSlowSubscriberIsDisconnectedNotDropped(t *testing.T) {
	p := newPipeline(t, pipeline.Config{ReportBuffer: 2})
	sub, _ := p.Subscribe(1)
	for i := 0; i < 10; i++ {
		_, err := p.Submit(context.Background(), limit(1, engine.Buy, 100, 1), "x")
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool {
		for {
			select {
			case _, ok := <-sub.C:
				if !ok {
					return true
				}
			default:
				return false
			}
		}
	}, 5*time.Second, 5*time.Millisecond)
	require.ErrorIs(t, sub.Err(), pipeline.ErrSlowConsumer)
}

func TestStopDrainsAndRefusesNewWork(t *testing.T) {
	p := pipeline.New(pipeline.Config{})
	sub, _ := p.Subscribe(1)
	_, err := p.Submit(context.Background(), limit(1, engine.Buy, 100, 1), "x")
	require.NoError(t, err)
	require.NoError(t, p.Stop(context.Background()))
	_, err = p.Submit(context.Background(), limit(1, engine.Buy, 100, 1), "y")
	require.ErrorIs(t, err, pipeline.ErrStopped)
	for range sub.C { // closed by Stop
	}
	require.ErrorIs(t, sub.Err(), pipeline.ErrStopped)
}

type fakeLog struct {
	mu      sync.Mutex
	batches [][]engine.Event
	backlog bool
}

func (f *fakeLog) Append(evs []engine.Event) {
	f.mu.Lock()
	f.batches = append(f.batches, evs)
	f.mu.Unlock()
}

func (f *fakeLog) Backlogged() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.backlog
}

func (f *fakeLog) snapshot() [][]engine.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]engine.Event(nil), f.batches...)
}

func TestEventLogGetsOneContiguousBatchPerCommand(t *testing.T) {
	fl := &fakeLog{}
	p := newPipeline(t, pipeline.Config{Log: fl})
	ctx := context.Background()

	_, err := p.Submit(ctx, limit(1, engine.Sell, 100, 10), "a")
	require.NoError(t, err)
	_, err = p.Submit(ctx, limit(2, engine.Buy, 100, 4), "b") // accepted + trade
	require.NoError(t, err)
	_, err = p.Submit(ctx, limit(1, engine.Buy, 100, 0), "c") // rejected
	require.NoError(t, err)
	require.Eventually(t, func() bool { return len(fl.snapshot()) == 3 }, 5*time.Second, time.Millisecond)

	bs := fl.snapshot()
	require.Len(t, bs[0], 1)
	require.Equal(t, engine.EvAccepted, bs[0][0].Kind)
	require.Len(t, bs[1], 2, "a command's events stay together in one batch")
	require.Equal(t, engine.EvAccepted, bs[1][0].Kind)
	require.Equal(t, engine.EvTrade, bs[1][1].Kind)
	require.Len(t, bs[2], 1)
	require.Equal(t, engine.EvRejected, bs[2][0].Kind)

	var seq uint64
	for _, b := range bs {
		for _, e := range b {
			seq++
			require.Equal(t, seq, e.Seq, "batches are in order and contiguous")
		}
	}
}

func TestBackloggedLogRefusesNewCommandsWithoutEnqueueing(t *testing.T) {
	fl := &fakeLog{backlog: true}
	p := newPipeline(t, pipeline.Config{Log: fl})
	_, err := p.Submit(context.Background(), limit(1, engine.Buy, 100, 1), "x")
	require.ErrorIs(t, err, pipeline.ErrLogBacklogged)
	require.Zero(t, p.Stats().Accepted)

	fl.mu.Lock()
	fl.backlog = false
	fl.mu.Unlock()
	_, err = p.Submit(context.Background(), limit(1, engine.Buy, 100, 1), "x")
	require.NoError(t, err, "accepted again as soon as the backlog clears")
}

func TestRestoredPipelineContinuesIDsSequenceAndLeaves(t *testing.T) {
	// a book that already has history: one resting sell of 10 (order id 41)
	book := engine.NewBook(engine.Config{MaxOrders: 16, MaxLevels: 4})
	var out []engine.Event
	for i := uint64(1); i <= 41; i++ {
		c := engine.Command{Kind: engine.CmdPlace, Side: engine.Buy, Type: engine.Limit, TIF: engine.IOC, OrderID: i, AccountID: 9, Price: 5, Qty: 1, Ts: 1}
		out = book.Apply(&c, out[:0])
	}
	c := engine.Command{Kind: engine.CmdPlace, Side: engine.Sell, Type: engine.Limit, TIF: engine.GTC, OrderID: 42, AccountID: 1, Price: 100, Qty: 10, Ts: 1}
	book.Apply(&c, nil)
	seqBefore := book.Seq()

	p := newPipeline(t, pipeline.Config{Restore: &pipeline.Restored{Book: book, NextOrderID: 43}})
	sub, _ := p.Subscribe(1)
	ev, err := p.Submit(context.Background(), limit(2, engine.Buy, 100, 4), "after")
	require.NoError(t, err)
	require.Equal(t, uint64(43), ev.OrderID, "order ids continue after the recovered maximum")
	require.Equal(t, seqBefore+1, ev.Seq, "sequence numbers continue without a gap")

	r := recv(t, sub) // the pre-crash resting order gets a trade report
	require.Equal(t, pipeline.ReportTrade, r.Type)
	require.Equal(t, int64(6), r.Leaves, "leaves computed from the recovered quantity")
}
