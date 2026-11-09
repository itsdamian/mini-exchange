package gateway_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/itsdamian/mini-exchange/internal/gateway"
	"github.com/itsdamian/mini-exchange/internal/pipeline"
	"github.com/itsdamian/mini-exchange/internal/risk"
	"github.com/itsdamian/mini-exchange/internal/risk/redistest"

	exchangev1 "github.com/itsdamian/mini-exchange/gen/exchange/v1"
)

func TestMain(m *testing.M) {
	code := m.Run()
	redistest.Terminate()
	os.Exit(code)
}

type riskEnv struct {
	client  exchangev1.OrderGatewayServiceClient
	checker *risk.Checker
	reg     *prometheus.Registry
	redis   *redistest.Server
}

// startRisk runs a real pipeline and gateway wired to a real Redis. Accounts
// are namespaced by base so tests sharing the Redis do not collide.
func startRisk(t *testing.T, lim risk.Limits) *riskEnv {
	t.Helper()
	srv := redistest.Shared(t)
	reg := prometheus.NewRegistry()
	checker, err := risk.New(context.Background(), srv.Client(t), risk.Config{Symbol: symbol, Limits: lim, Registerer: reg, Timeout: time.Second})
	require.NoError(t, err)
	p := pipeline.New(pipeline.Config{Risk: checker})
	gw := gateway.New(symbol, p, nil).WithRisk(checker)
	lis := bufconn.Listen(1 << 20)
	g := grpc.NewServer(grpc.UnaryInterceptor(gw.UnaryInterceptor()))
	gw.Register(g)
	go func() { _ = g.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
		g.GracefulStop()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, p.Stop(ctx))
		require.NoError(t, checker.Close(ctx))
	})
	return &riskEnv{client: exchangev1.NewOrderGatewayServiceClient(conn), checker: checker, reg: reg, redis: srv}
}

func (e *riskEnv) flush(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, e.checker.Flush(ctx))
}

func (e *riskEnv) counter(t *testing.T, name string) float64 {
	t.Helper()
	mfs, err := e.reg.Gather()
	require.NoError(t, err)
	var sum float64
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			sum += value(m)
		}
	}
	return sum
}

func value(m *dto.Metric) float64 {
	if m.Counter != nil {
		return m.Counter.GetValue()
	}
	return 0
}

// A different account range per test: the Redis is shared and never flushed.
var acctNext atomic.Uint64

func accounts(t *testing.T, n int) []uint64 {
	t.Helper()
	base := 100_000 * acctNext.Add(1)
	out := make([]uint64, n)
	for i := range out {
		out[i] = base + uint64(i) + 1
	}
	return out
}

func TestRiskRejectsAreTypedAndNotCached(t *testing.T) {
	e := startRisk(t, risk.Limits{MaxOrderQty: 100, MaxPrice: 10_000, MaxOpenOrders: 2})
	ctx := context.Background()
	a := accounts(t, 1)[0]
	require.NoError(t, e.checker.Deposit(ctx, a, 0, 1_000))

	// Too large and underfunded are typed rejects, not transport errors.
	r, err := e.client.PlaceOrder(ctx, place("big", a, exchangev1.Side_SIDE_BUY, 10, 101))
	require.NoError(t, err)
	require.Equal(t, exchangev1.OrderStatus_ORDER_STATUS_REJECTED, r.Status)
	require.Equal(t, exchangev1.RejectReason_REJECT_REASON_ORDER_TOO_LARGE, r.RejectReason)

	r, err = e.client.PlaceOrder(ctx, place("poor", a, exchangev1.Side_SIDE_BUY, 100, 11)) // 1100 > 1000
	require.NoError(t, err)
	require.Equal(t, exchangev1.RejectReason_REJECT_REASON_INSUFFICIENT_FUNDS, r.RejectReason)

	// The rejection was not cached: after a deposit the same key is accepted,
	// and exactly one order exists.
	require.NoError(t, e.checker.Deposit(ctx, a, 0, 200))
	r, err = e.client.PlaceOrder(ctx, place("poor", a, exchangev1.Side_SIDE_BUY, 100, 11))
	require.NoError(t, err)
	require.Equal(t, exchangev1.OrderStatus_ORDER_STATUS_ACCEPTED, r.Status)

	// Open-order limit (2): one is open, a second fits, a third is refused.
	r, _ = e.client.PlaceOrder(ctx, place("o2", a, exchangev1.Side_SIDE_BUY, 1, 1))
	require.Equal(t, exchangev1.OrderStatus_ORDER_STATUS_ACCEPTED, r.Status)
	r, _ = e.client.PlaceOrder(ctx, place("o3", a, exchangev1.Side_SIDE_BUY, 1, 1))
	require.Equal(t, exchangev1.RejectReason_REJECT_REASON_TOO_MANY_OPEN_ORDERS, r.RejectReason)

	// Cancelling releases the slot and the funds once settlement catches up.
	cr, err := e.client.CancelOrder(ctx, &exchangev1.CancelOrderRequest{ClientOrderId: "poor", AccountId: a, Symbol: symbol})
	require.NoError(t, err)
	require.Equal(t, exchangev1.OrderStatus_ORDER_STATUS_CANCELED, cr.Status)
	e.flush(t)
	b, err := e.checker.Balance(ctx, a)
	require.NoError(t, err)
	require.Equal(t, risk.Balance{QuoteAvail: 1_199, QuoteHeld: 1, Open: 1}, b)
}

func TestRiskRateLimited(t *testing.T) {
	e := startRisk(t, risk.Limits{MaxOrderQty: 100, MaxPrice: 10_000, RatePerSec: 1, Burst: 2})
	ctx := context.Background()
	a := accounts(t, 1)[0]
	require.NoError(t, e.checker.Deposit(ctx, a, 0, 1_000_000))
	var limited int
	for i := range 5 { // sent back to back: far faster than 1 order/s
		r, err := e.client.PlaceOrder(ctx, place(fmt.Sprint("r", i), a, exchangev1.Side_SIDE_BUY, 10, 1))
		require.NoError(t, err)
		if r.RejectReason == exchangev1.RejectReason_REJECT_REASON_RATE_LIMITED {
			limited++
		}
	}
	require.GreaterOrEqual(t, limited, 2)
	require.LessOrEqual(t, limited, 3) // at most one token may have refilled during the loop
}

// End to end through the real pipeline: after a random mix of orders, trades
// and cancels, no balance is ever negative, funds are conserved exactly, and
// once everything is cancelled nothing is left held.
func TestRiskConservationEndToEnd(t *testing.T) {
	e := startRisk(t, risk.Limits{MaxOrderQty: 100, MaxPrice: 1_000, MaxOpenOrders: 1000})
	ctx := context.Background()
	accts := accounts(t, 8)
	const base, quote = int64(10_000), int64(10_000_000)
	for _, a := range accts {
		require.NoError(t, e.checker.Deposit(ctx, a, base, quote))
	}

	rng := rand.New(rand.NewPCG(1, 2))
	type ord struct {
		acct uint64
		cid  string
	}
	var open []ord
	for i := range 3000 {
		a := accts[rng.IntN(len(accts))]
		cid := fmt.Sprint("e", i)
		switch rng.IntN(10) {
		case 0, 1: // cancel a random earlier order (it may have traded already)
			if len(open) > 0 {
				j := rng.IntN(len(open))
				_, err := e.client.CancelOrder(ctx, &exchangev1.CancelOrderRequest{ClientOrderId: open[j].cid, AccountId: open[j].acct, Symbol: symbol})
				require.NoError(t, err)
				open = append(open[:j], open[j+1:]...)
			}
			continue
		}
		req := place(cid, a, exchangev1.Side_SIDE_BUY, 480+rng.Int64N(41), 1+rng.Int64N(50))
		if rng.IntN(2) == 0 {
			req.Side = exchangev1.Side_SIDE_SELL
		}
		switch rng.IntN(6) {
		case 0:
			req.TimeInForce = exchangev1.TimeInForce_TIME_IN_FORCE_IOC
		case 1:
			req.Type = exchangev1.OrderType_ORDER_TYPE_MARKET
			req.Price = 0
		}
		r, err := e.client.PlaceOrder(ctx, req)
		require.NoError(t, err)
		if r.Status == exchangev1.OrderStatus_ORDER_STATUS_ACCEPTED {
			open = append(open, ord{a, cid})
		}
		if i%500 == 499 {
			e.flush(t)
			au, err := e.checker.Audit(ctx)
			require.NoError(t, err)
			require.Zero(t, au.Negative)
		}
	}
	for _, o := range open { // cancel everything still resting (unknown ones are fine)
		_, err := e.client.CancelOrder(ctx, &exchangev1.CancelOrderRequest{ClientOrderId: o.cid, AccountId: o.acct, Symbol: symbol})
		require.NoError(t, err)
	}
	e.flush(t)

	var tb, tq, held, nopen int64
	for _, a := range accts {
		b, err := e.checker.Balance(ctx, a)
		require.NoError(t, err)
		require.GreaterOrEqual(t, b.BaseAvail, int64(0))
		require.GreaterOrEqual(t, b.QuoteAvail, int64(0))
		tb += b.BaseAvail + b.BaseHeld
		tq += b.QuoteAvail + b.QuoteHeld
		held += b.BaseHeld + b.QuoteHeld
		nopen += b.Open
	}
	n := int64(len(accts))
	require.Equal(t, n*base, tb, "base is conserved")
	require.Equal(t, n*quote, tq, "quote is conserved")
	require.Zero(t, held, "nothing held once every order is done")
	require.Zero(t, nopen, "no open reservations left")
	require.Zero(t, e.counter(t, "mx_risk_settle_anomalies_total"), "settlement found no inconsistency")
	require.Zero(t, e.counter(t, "mx_risk_settle_dropped_total"))
}

// With Redis down the gateway fails closed (UNAVAILABLE, no order admitted),
// and works again once Redis is back, including settlement after Redis lost
// its script cache.
func TestRiskFailsClosedWhenRedisIsDown(t *testing.T) {
	e := startRisk(t, risk.Limits{MaxOrderQty: 100, MaxPrice: 10_000})
	ctx := context.Background()
	accts := accounts(t, 2)
	a, b := accts[0], accts[1]
	require.NoError(t, e.checker.Deposit(ctx, a, 100, 100_000))

	r, err := e.client.PlaceOrder(ctx, place("up", a, exchangev1.Side_SIDE_BUY, 1, 1))
	require.NoError(t, err)
	require.Equal(t, exchangev1.OrderStatus_ORDER_STATUS_ACCEPTED, r.Status)

	require.NoError(t, e.redis.Stop(ctx))
	start := time.Now()
	_, err = e.client.PlaceOrder(ctx, place("down", a, exchangev1.Side_SIDE_BUY, 10, 1))
	require.Equal(t, codes.Unavailable, status.Code(err), "fail closed: %v", err)
	require.Less(t, time.Since(start), 3*time.Second, "the refusal is quick (bounded by the Redis timeout)")

	require.NoError(t, e.redis.Restart(ctx))
	// The graceful stop saved its data, so a's balances survived; its script
	// cache did not, which exercises the settler's NOSCRIPT reload.
	require.NoError(t, e.checker.Deposit(ctx, b, 100, 100_000))

	// The refused key is usable again: nothing was cached for it.
	// (Admission resumes once the settler's retry loop has seen Redis again.)
	require.Eventually(t, func() bool {
		r, err = e.client.PlaceOrder(ctx, place("down", a, exchangev1.Side_SIDE_SELL, 10, 5))
		return err == nil
	}, 10*time.Second, 50*time.Millisecond)
	require.Equal(t, exchangev1.OrderStatus_ORDER_STATUS_ACCEPTED, r.Status)
	require.Eventually(t, func() bool { // a refusal while settlement catches up is allowed, and retryable
		r, err = e.client.PlaceOrder(ctx, place("after", b, exchangev1.Side_SIDE_BUY, 10, 5)) // trades against it
		return err == nil
	}, 10*time.Second, 50*time.Millisecond)
	require.Equal(t, exchangev1.OrderStatus_ORDER_STATUS_ACCEPTED, r.Status)
	e.flush(t)

	ba, _ := e.checker.Balance(ctx, a)
	bb, _ := e.checker.Balance(ctx, b)
	require.Equal(t, int64(95), ba.BaseAvail)
	require.Equal(t, int64(100_049), ba.QuoteAvail) // +50 from the sale, 1 still held by the resting "up" order
	require.Equal(t, int64(105), bb.BaseAvail)
	require.Equal(t, int64(99_950), bb.QuoteAvail)
}
