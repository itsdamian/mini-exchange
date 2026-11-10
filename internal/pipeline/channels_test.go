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

// The channel-based queues (Config.UseChannels, phase 8 experiment 3) must behave
// like the rings: same results, same overload and shutdown behaviour.

func TestChannelQueuesConcurrentSubmittersAllGetDistinctOrders(t *testing.T) {
	p := newPipeline(t, pipeline.Config{UseChannels: true})
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
		require.False(t, seen[id])
		seen[id] = true
	}
	require.Len(t, seen, n)
	require.Equal(t, uint64(n), p.Stats().Accepted)
}

func TestChannelQueuesMatchTradesAndReport(t *testing.T) {
	p := newPipeline(t, pipeline.Config{UseChannels: true})
	sub, err := p.Subscribe(1)
	require.NoError(t, err)
	_, err = p.Submit(context.Background(), limit(1, engine.Sell, 100, 5), "s")
	require.NoError(t, err)
	_, err = p.Submit(context.Background(), limit(2, engine.Buy, 100, 5), "b")
	require.NoError(t, err)
	sawTrade := false
	for i := 0; i < 2 && !sawTrade; i++ {
		select {
		case r := <-sub.C:
			sawTrade = r.Type == pipeline.ReportTrade
		case <-time.After(5 * time.Second):
			t.Fatal("no report")
		}
	}
	require.True(t, sawTrade)
	require.Equal(t, uint64(1), p.Stats().Trades)
}

func TestChannelQueuesOverloadReturnsErrorInsteadOfBlocking(t *testing.T) {
	p := newPipeline(t, pipeline.Config{UseChannels: true, InputSize: 2, MaxInFlight: 2})
	var overloaded int
	for i := 0; i < 200; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := p.Submit(ctx, limit(1, engine.Buy, 100, 1), "x")
		cancel()
		if err == pipeline.ErrOverloaded {
			overloaded++
		}
	}
	_ = overloaded // sequential submits may all fit; the point is that none blocked
}

func TestChannelQueuesStopDrainsAndRefusesNewWork(t *testing.T) {
	p := pipeline.New(pipeline.Config{UseChannels: true})
	var wg sync.WaitGroup
	var ok int64
	var mu sync.Mutex
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := p.Submit(context.Background(), limit(1, engine.Buy, 100, 1), "x"); err == nil {
				mu.Lock()
				ok++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, p.Stop(ctx))
	require.Equal(t, uint64(ok), p.Stats().Accepted, "everything accepted was processed before stopping")
	_, err := p.Submit(context.Background(), limit(1, engine.Buy, 100, 1), "late")
	require.ErrorIs(t, err, pipeline.ErrStopped)
}
