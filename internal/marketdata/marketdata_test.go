package marketdata_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"

	"github.com/itsdamian/mini-exchange/internal/engine"
	"github.com/itsdamian/mini-exchange/internal/enginetest"
	"github.com/itsdamian/mini-exchange/internal/marketdata"
)

var bookCfg = engine.Config{MaxOrders: 4096, MaxLevels: 256}

// commands applies a random stream to a reference book and returns the event
// batches, one per command, plus the reference book.
func history(n int, seed uint64) ([][]engine.Event, *engine.Book) {
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
	return out, book
}

func decode(t testing.TB, data []byte) marketdata.Snapshot {
	t.Helper()
	var s marketdata.Snapshot
	require.NoError(t, json.Unmarshal(data, &s))
	return s
}

func TestFeedDepthMatchesTheReferenceBook(t *testing.T) {
	batches, ref := history(5000, 3)
	f := marketdata.NewFeed("BTC-USD", engine.NewBook(bookCfg), marketdata.FeedConfig{Depth: 5})
	for _, evs := range batches {
		require.NoError(t, f.Apply(evs))
	}
	require.True(t, f.Publish())
	require.False(t, f.Publish(), "nothing new, no new frame")
	s := decode(t, f.Frame().Data)

	require.Equal(t, ref.Seq(), s.Seq)
	var asks, bids [][2]int64
	ref.Depth(5, func(side engine.Side, p, q int64) {
		if side == engine.Sell {
			asks = append(asks, [2]int64{p, q})
		} else {
			bids = append(bids, [2]int64{p, q})
		}
	})
	require.Equal(t, asks, s.Asks)
	require.Equal(t, bids, s.Bids)
	require.LessOrEqual(t, len(s.Trades), 50)
	require.NotEmpty(t, s.Trades, "the workload trades")
	for i := 1; i < len(s.Trades); i++ {
		require.Greater(t, s.Trades[i].Seq, s.Trades[i-1].Seq, "recent trades are in order, newest last")
	}
}

func TestFeedRefusesAnInconsistentStream(t *testing.T) {
	batches, _ := history(100, 4)
	f := marketdata.NewFeed("S", engine.NewBook(bookCfg), marketdata.FeedConfig{})
	require.NoError(t, f.Apply(batches[0]))
	require.Error(t, f.Apply(batches[2]), "a gap must not be papered over")
}

func dial(t testing.TB, url string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, url, nil)
	require.NoError(t, err)
	return c
}

func wsURL(s *httptest.Server) string { return "ws" + strings.TrimPrefix(s.URL, "http") + "/ws" }

func TestClientGetsStateAtOnceAndConflatedUpdates(t *testing.T) {
	batches, _ := history(20000, 5)
	f := marketdata.NewFeed("BTC-USD", engine.NewBook(bookCfg), marketdata.FeedConfig{})
	srv := httptest.NewServer(marketdata.NewServer(f, marketdata.ServerConfig{Interval: 100 * time.Millisecond}).Handler())
	defer srv.Close()
	stop := make(chan struct{})
	defer close(stop)
	go f.Run(stop, 20*time.Millisecond)

	c := dial(t, wsURL(srv))
	defer func() { _ = c.CloseNow() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, first, err := c.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, "snapshot", decode(t, first).Type, "state arrives immediately")

	// Feed events as fast as possible for about a second: tens of thousands
	// of events, but the client may see at most one frame per 100 ms.
	var got atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, _, err := c.Read(ctx); err != nil {
				return
			}
			got.Add(1)
		}
	}()
	start := time.Now()
	applied := 0
	for _, evs := range batches {
		require.NoError(t, f.Apply(evs))
		applied += len(evs)
		time.Sleep(20 * time.Microsecond)
	}
	time.Sleep(300 * time.Millisecond)
	_ = c.CloseNow()
	<-done
	elapsed := time.Since(start)
	maxFrames := int64(elapsed/(100*time.Millisecond)) + 2
	require.Greater(t, got.Load(), int64(1), "the client did get updates")
	require.LessOrEqual(t, got.Load(), maxFrames, "at most one frame per 100 ms (%d events applied in %v)", applied, elapsed)
}

// FR-7 acceptance, hermetic part: 1,000 clients, memory stays flat while a
// slow client that never reads is disconnected.
func TestThousandClientsStableMemoryAndSlowClientDropped(t *testing.T) {
	batches, _ := history(30000, 6)
	f := marketdata.NewFeed("BTC-USD", engine.NewBook(bookCfg), marketdata.FeedConfig{})
	m := marketdata.NewServer(f, marketdata.ServerConfig{Interval: 100 * time.Millisecond, WriteTimeout: 300 * time.Millisecond})
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	stop := make(chan struct{})
	defer close(stop)
	go f.Run(stop, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	var msgs atomic.Int64
	const n = 1000
	for range n {
		c := dial(t, wsURL(srv))
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { _ = c.CloseNow() }()
			c.SetReadLimit(1 << 20)
			for {
				if _, _, err := c.Read(ctx); err != nil {
					return
				}
				msgs.Add(1)
			}
		}()
	}
	// One client that connects and never reads.
	lazy := dial(t, wsURL(srv))
	defer func() { _ = lazy.CloseNow() }()
	require.Eventually(t, func() bool { return m.Clients() == n+1 }, 10*time.Second, 20*time.Millisecond)

	heap := func() uint64 {
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		return ms.HeapAlloc
	}
	feed := func(batches [][]engine.Event) {
		for _, evs := range batches {
			require.NoError(t, f.Apply(evs))
			time.Sleep(10 * time.Microsecond)
		}
	}
	feed(batches[:len(batches)/3])
	before := heap()
	feed(batches[len(batches)/3:])
	time.Sleep(500 * time.Millisecond)
	after := heap()

	require.Greater(t, msgs.Load(), int64(n), "every client received updates")
	// Growth beyond a few MB while the same 1,000 clients keep receiving would
	// mean per-client queues. The book state itself is bounded.
	require.Less(t, int64(after)-int64(before), int64(8<<20), "heap before %d after %d", before, after)
	t.Logf("heap before=%dKB after=%dKB messages=%d", before>>10, after>>10, msgs.Load())
}

func TestSlowClientIsDisconnectedNotBuffered(t *testing.T) {
	batches, _ := history(60000, 7)
	f := marketdata.NewFeed("BTC-USD", engine.NewBook(bookCfg), marketdata.FeedConfig{Depth: 200, KeepTrades: 2000}) // big frames: the socket buffers fill quickly
	m := marketdata.NewServer(f, marketdata.ServerConfig{Interval: 5 * time.Millisecond, WriteTimeout: 200 * time.Millisecond})
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	stop := make(chan struct{})
	defer close(stop)
	go f.Run(stop, 2*time.Millisecond)

	lazy := dial(t, wsURL(srv)) // never reads
	defer func() { _ = lazy.CloseNow() }()
	require.Eventually(t, func() bool { return m.Clients() == 1 }, 5*time.Second, 10*time.Millisecond)

	// Keep changing the state with real events (big frames): they pile up in
	// the socket buffers of the client that does not read, until a write
	// blocks past its timeout and the server drops it.
	next := 0
	require.Eventually(t, func() bool {
		for range 20 {
			if next < len(batches) {
				_ = f.Apply(batches[next])
				next++
			}
		}
		return m.Clients() == 0
	}, 90*time.Second, time.Millisecond, "a client that stops reading must be disconnected")
	require.Less(t, next, len(batches), "ran out of events before the client was dropped")
}

func TestClientLimit(t *testing.T) {
	f := marketdata.NewFeed("S", engine.NewBook(bookCfg), marketdata.FeedConfig{})
	m := marketdata.NewServer(f, marketdata.ServerConfig{MaxClients: 2})
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	a, b := dial(t, wsURL(srv)), dial(t, wsURL(srv))
	defer func() { _ = a.CloseNow() }()
	defer func() { _ = b.CloseNow() }()
	require.Eventually(t, func() bool { return m.Clients() == 2 }, 5*time.Second, 10*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, resp, err := websocket.Dial(ctx, wsURL(srv), nil)
	require.Error(t, err)
	require.Equal(t, 503, resp.StatusCode)
}
