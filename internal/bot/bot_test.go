package bot_test

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/itsdamian/mini-exchange/internal/bot"
	"github.com/itsdamian/mini-exchange/internal/engine"
	"github.com/itsdamian/mini-exchange/internal/gateway"
	"github.com/itsdamian/mini-exchange/internal/nonet"
	"github.com/itsdamian/mini-exchange/internal/pipeline"

	exchangev1 "github.com/itsdamian/mini-exchange/gen/exchange/v1"
)

func TestMain(m *testing.M) {
	nonet.Install() // no test here may reach an external host
	os.Exit(m.Run())
}

const sample = "../../data/sample-prices.csv"

func TestLoadFileFormats(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(p, []byte(body), 0o600))
		return p
	}
	collect := func(path string) []float64 {
		s, err := bot.LoadFile(path)
		require.NoError(t, err)
		var out []float64
		for {
			p, err := s.Next(context.Background())
			if err == io.EOF {
				return out
			}
			require.NoError(t, err)
			out = append(out, p)
		}
	}
	want := []float64{100.5, 101.25}
	require.Equal(t, want, collect(write("a.csv", "time,price\nt1,100.5\nt2,101.25\n")))
	require.Equal(t, want, collect(write("b.csv", "100.5\n101.25\n")), "no header: last column")
	require.Equal(t, want, collect(write("c.csv", "time,volume,price,side\nt1,1,100.5,b\nt2,2,101.25,s\n")), "named price column")
	require.Equal(t, want, collect(write("d.json", "[100.5, 101.25]")))
	require.Equal(t, want, collect(write("e.json", `[{"time":"t","price":100.5},{"price":101.25}]`)))

	for name, body := range map[string]string{"bad.csv": "price\nabc\n", "neg.csv": "price\n-5\n", "empty.csv": "price\n", "bad.json": "{}"} {
		_, err := bot.LoadFile(write(name, body))
		require.Error(t, err, name)
	}
	_, err := bot.LoadFile(filepath.Join(dir, "missing.csv"))
	require.Error(t, err)
	s, err := bot.LoadFile(sample)
	require.NoError(t, err)
	require.Equal(t, 3000, s.Len())
}

func TestPlannerIsDeterministicAndShapedLikeAMarketMaker(t *testing.T) {
	run := func(seed uint64) [][]bot.Action {
		p := bot.NewPlanner(bot.Config{Seed: seed})
		var out [][]bot.Action
		for _, px := range []float64{67000, 67001.5, 66990.25, 67000, 67010} {
			out = append(out, p.Step(px))
		}
		return out
	}
	require.Equal(t, run(1), run(1))
	require.NotEqual(t, run(1), run(2))

	steps := run(1)
	first := steps[0]
	var bids, asks []bot.Action
	for _, a := range first {
		require.False(t, a.Cancel, "nothing to cancel on the first step")
		if a.Account == 1 {
			if a.Buy {
				bids = append(bids, a)
			} else {
				asks = append(asks, a)
			}
		}
	}
	require.Len(t, bids, 3)
	require.Len(t, asks, 3)
	require.Less(t, bids[0].Price, asks[0].Price, "the quotes do not cross")
	require.EqualValues(t, 6_700_000, (bids[0].Price+asks[0].Price)/2, "quoted around the reference price in ticks")

	cancels := 0
	for _, a := range steps[1] {
		if a.Cancel {
			cancels++
			require.EqualValues(t, 1, a.Account)
		}
	}
	require.Equal(t, 6, cancels, "the previous quotes are pulled before new ones are placed")
}

type capture struct {
	mu  sync.Mutex
	evs []engine.Event
}

func (c *capture) Append(evs []engine.Event) {
	c.mu.Lock()
	for _, e := range evs {
		e.Ts = 0 // timestamps come from the sequencer's clock, the only non-deterministic field
		c.evs = append(c.evs, e)
	}
	c.mu.Unlock()
}
func (c *capture) Backlogged() bool { return false }

// runAgainstGateway runs the bot through the real gRPC gateway and engine and
// returns the event stream the engine produced.
func runAgainstGateway(t *testing.T, seed uint64, prices []float64) ([]engine.Event, bot.Stats) {
	t.Helper()
	cap := &capture{}
	p := pipeline.New(pipeline.Config{Log: cap})
	gw := gateway.New("BTC-USD", p, nil)
	lis := bufconn.Listen(1 << 20)
	g := grpc.NewServer(grpc.UnaryInterceptor(gw.UnaryInterceptor()))
	gw.Register(g)
	go func() { _ = g.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	r := &bot.Runner{Client: exchangev1.NewOrderGatewayServiceClient(conn), Symbol: "BTC-USD", Planner: bot.NewPlanner(bot.Config{Seed: seed})}
	st, err := r.Run(context.Background(), bot.NewFileSource(prices))
	require.NoError(t, err)
	_ = conn.Close()
	g.GracefulStop()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, p.Stop(ctx))
	return cap.evs, st
}

// FR-8 acceptance: with the recorded file and a fixed seed, two runs produce
// identical event streams.
func TestSameFileAndSeedGiveIdenticalEventStreams(t *testing.T) {
	src, err := bot.LoadFile(sample)
	require.NoError(t, err)
	var prices []float64
	for i := 0; i < 400; i++ {
		p, _ := src.Next(context.Background())
		prices = append(prices, p)
	}

	a, sa := runAgainstGateway(t, 42, prices)
	b, sb := runAgainstGateway(t, 42, prices)
	require.Equal(t, sa, sb)
	require.Equal(t, a, b, "same file and seed: identical event streams")
	require.Zero(t, sa.Errors)

	trades := 0
	for _, e := range a {
		if e.Kind == engine.EvTrade {
			trades++
		}
	}
	require.Greater(t, trades, 100, "the flow trades")
	require.Greater(t, sa.Cancelled, 1000, "the maker re-quotes every step")

	c, _ := runAgainstGateway(t, 43, prices)
	require.NotEqual(t, a, c, "a different seed gives a different stream")
}

func fakeCoinbase(t *testing.T, prices []string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = c.CloseNow() }()
		ctx := r.Context()
		_, sub, err := c.Read(ctx)
		if err != nil || !strings.Contains(string(sub), `"ticker"`) || !strings.Contains(string(sub), "BTC-USD") {
			_ = c.Write(ctx, websocket.MessageText, []byte(`{"type":"error","message":"bad subscribe"}`))
			return
		}
		_ = c.Write(ctx, websocket.MessageText, []byte(`{"type":"subscriptions","channels":[]}`))
		for _, p := range prices {
			b, _ := json.Marshal(map[string]string{"type": "ticker", "product_id": "BTC-USD", "price": p})
			_ = c.Write(ctx, websocket.MessageText, b)
		}
		<-ctx.Done()
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestCoinbaseSourceAgainstAFakeServer(t *testing.T) {
	srv := fakeCoinbase(t, []string{"67000.10", "67001.20", "oops", "67002.30"})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := bot.NewCoinbaseSource(ctx, "ws"+strings.TrimPrefix(srv.URL, "http"), "BTC-USD", nil)
	require.NoError(t, err)
	defer s.Close()
	for _, want := range []float64{67000.10, 67001.20, 67002.30} { // the unparsable one is skipped
		got, err := s.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}

func TestCoinbaseSourceReportsAFeedError(t *testing.T) {
	srv := fakeCoinbase(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := bot.NewCoinbaseSource(ctx, "ws"+strings.TrimPrefix(srv.URL, "http"), "ETH-USD", nil) // the fake rejects other products
	require.NoError(t, err)
	defer s.Close()
	_, err = s.Next(ctx)
	require.ErrorContains(t, err, "bad subscribe")
}

// The guard: a test that tries to reach the real feed fails at once.
func TestTestsCannotReachExternalHosts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	_, err := bot.NewCoinbaseSource(ctx, bot.CoinbaseURL, "BTC-USD", nil)
	require.ErrorIs(t, err, nonet.ErrExternal)
	require.Less(t, time.Since(start), 2*time.Second, "refused before any network traffic")
}
