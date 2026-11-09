package risk_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/itsdamian/mini-exchange/internal/engine"
	"github.com/itsdamian/mini-exchange/internal/risk"
	"github.com/itsdamian/mini-exchange/internal/risk/redistest"
)

func TestMain(m *testing.M) {
	code := m.Run()
	redistest.Terminate()
	os.Exit(code)
}

var nextAccount atomic.Uint64 // tests share one Redis, so each gets fresh account ids

func account() uint64 { return 1000 + nextAccount.Add(1) }

var defaultLimits = risk.Limits{MaxOrderQty: 1000, MaxPrice: 10_000, MaxOpenOrders: 5}

func newChecker(t *testing.T, lim risk.Limits, clock func() time.Time) (*risk.Checker, *redis.Client) {
	t.Helper()
	rdb := redistest.Shared(t).Client(t)
	c, err := risk.New(context.Background(), rdb, risk.Config{Symbol: "BTC-USD", Limits: lim, Clock: clock, Timeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, c.Close(ctx))
	})
	return c, rdb
}

func flush(t *testing.T, c *risk.Checker) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, c.Flush(ctx))
}

func buy(a uint64, cid string, price, qty int64) risk.Request {
	return risk.Request{Account: a, Client: cid, Side: engine.Buy, Type: engine.Limit, Price: price, Qty: qty}
}

func sell(a uint64, cid string, price, qty int64) risk.Request {
	return risk.Request{Account: a, Client: cid, Side: engine.Sell, Type: engine.Limit, Price: price, Qty: qty}
}

func bal(t *testing.T, c *risk.Checker, a uint64) risk.Balance {
	t.Helper()
	b, err := c.Balance(context.Background(), a)
	require.NoError(t, err)
	return b
}

func TestLimitsValidation(t *testing.T) {
	rdb := redistest.Shared(t).Client(t)
	for name, lim := range map[string]risk.Limits{
		"zero":          {},
		"overflow":      {MaxOrderQty: 1 << 40, MaxPrice: 1 << 40},
		"rate no burst": {MaxOrderQty: 1, MaxPrice: 1, RatePerSec: 10},
	} {
		_, err := risk.New(context.Background(), rdb, risk.Config{Limits: lim})
		require.Error(t, err, name)
	}
}

func TestReserveHoldsFundsAndRejects(t *testing.T) {
	c, _ := newChecker(t, defaultLimits, nil)
	ctx := context.Background()
	a := account()
	require.NoError(t, c.Deposit(ctx, a, 100, 5_000))

	// Buy 10 @ 300 holds 3000 quote.
	r, err := c.Reserve(ctx, buy(a, "b1", 300, 10))
	require.NoError(t, err)
	require.Equal(t, risk.OK, r)
	require.Equal(t, risk.Balance{BaseAvail: 100, QuoteAvail: 2_000, QuoteHeld: 3_000, Open: 1}, bal(t, c, a))

	// Retrying the same client id does not hold twice.
	r, err = c.Reserve(ctx, buy(a, "b1", 300, 10))
	require.NoError(t, err)
	require.Equal(t, risk.OK, r)
	require.Equal(t, int64(3_000), bal(t, c, a).QuoteHeld)

	// 2000 left: 7 @ 300 = 2100 does not fit, 6 @ 300 = 1800 does.
	r, _ = c.Reserve(ctx, buy(a, "b2", 300, 7))
	require.Equal(t, risk.InsufficientFunds, r)
	r, _ = c.Reserve(ctx, buy(a, "b3", 300, 6))
	require.Equal(t, risk.OK, r)

	// Sell 100 base holds all of it; one more unit does not fit.
	r, _ = c.Reserve(ctx, sell(a, "s1", 300, 100))
	require.Equal(t, risk.OK, r)
	r, _ = c.Reserve(ctx, sell(a, "s2", 300, 1))
	require.Equal(t, risk.InsufficientFunds, r)

	b := bal(t, c, a)
	require.Equal(t, risk.Balance{BaseAvail: 0, BaseHeld: 100, QuoteAvail: 200, QuoteHeld: 4_800, Open: 3}, b)
}

func TestReserveStaticLimits(t *testing.T) {
	c, _ := newChecker(t, defaultLimits, nil)
	ctx := context.Background()
	a := account()
	require.NoError(t, c.Deposit(ctx, a, 1_000_000, 1_000_000))
	cases := []struct {
		name string
		req  risk.Request
		want risk.Reason
	}{
		{"zero qty", buy(a, "z1", 100, 0), risk.InvalidQuantity},
		{"negative qty", buy(a, "z2", 100, -1), risk.InvalidQuantity},
		{"too large", buy(a, "z3", 100, 1001), risk.OrderTooLarge},
		{"zero price", buy(a, "z4", 0, 1), risk.InvalidPrice},
		{"price above max", buy(a, "z5", 10_001, 1), risk.InvalidPrice},
		{"largest allowed", buy(a, "z6", 10_000, 100), risk.OK},
	}
	for _, tc := range cases {
		r, err := c.Reserve(ctx, tc.req)
		require.NoError(t, err, tc.name)
		require.Equal(t, tc.want, r, tc.name)
	}
	// Only the accepted one holds anything.
	require.Equal(t, int64(1), bal(t, c, a).Open)
}

func TestMaxOpenOrders(t *testing.T) {
	c, _ := newChecker(t, defaultLimits, nil) // 5 open
	ctx := context.Background()
	a := account()
	require.NoError(t, c.Deposit(ctx, a, 0, 1_000_000))
	for i := range 5 {
		r, err := c.Reserve(ctx, buy(a, fmt.Sprint("o", i), 10, 1))
		require.NoError(t, err)
		require.Equal(t, risk.OK, r)
	}
	r, _ := c.Reserve(ctx, buy(a, "o5", 10, 1))
	require.Equal(t, risk.TooManyOpenOrders, r)
	// Releasing one frees a slot.
	require.NoError(t, c.Release(ctx, a, "o0"))
	r, _ = c.Reserve(ctx, buy(a, "o5", 10, 1))
	require.Equal(t, risk.OK, r)
}

func TestTokenBucket(t *testing.T) {
	var now atomic.Int64
	now.Store(time.Unix(1_000_000, 0).UnixNano())
	clock := func() time.Time { return time.Unix(0, now.Load()) }
	lim := risk.Limits{MaxOrderQty: 1000, MaxPrice: 10_000, RatePerSec: 10, Burst: 3}
	c, _ := newChecker(t, lim, clock)
	ctx := context.Background()
	a, other := account(), account()
	require.NoError(t, c.Deposit(ctx, a, 0, 1_000_000))
	require.NoError(t, c.Deposit(ctx, other, 0, 1_000_000))

	try := func(acct uint64, id string) risk.Reason {
		r, err := c.Reserve(ctx, buy(acct, id, 10, 1))
		require.NoError(t, err)
		return r
	}
	// A burst of 3 passes, the 4th is limited.
	require.Equal(t, risk.OK, try(a, "t1"))
	require.Equal(t, risk.OK, try(a, "t2"))
	require.Equal(t, risk.OK, try(a, "t3"))
	require.Equal(t, risk.RateLimited, try(a, "t4"))
	// Limits are per account.
	require.Equal(t, risk.OK, try(other, "t1"))
	// A limited order held nothing.
	require.Equal(t, int64(3), bal(t, c, a).Open)

	// 10 tokens/s: after 50 ms there is half a token, still limited; after
	// 100 ms there is one.
	now.Add(50 * int64(time.Millisecond))
	require.Equal(t, risk.RateLimited, try(a, "t5"))
	now.Add(50 * int64(time.Millisecond))
	require.Equal(t, risk.OK, try(a, "t6"))
	require.Equal(t, risk.RateLimited, try(a, "t7"))

	// The bucket never exceeds its burst, however long the pause.
	now.Add(time.Hour.Nanoseconds())
	for i := range 3 {
		require.Equal(t, risk.OK, try(a, fmt.Sprint("p", i)), i)
	}
	require.Equal(t, risk.RateLimited, try(a, "p3"))
}

func TestMarketBuyReservesAtMaxPrice(t *testing.T) {
	c, _ := newChecker(t, defaultLimits, nil) // MaxPrice 10_000
	ctx := context.Background()
	a := account()
	require.NoError(t, c.Deposit(ctx, a, 0, 100_000))
	r, err := c.Reserve(ctx, risk.Request{Account: a, Client: "m1", Side: engine.Buy, Type: engine.Market, Qty: 10})
	require.NoError(t, err)
	require.Equal(t, risk.OK, r)
	require.Equal(t, int64(100_000), bal(t, c, a).QuoteHeld) // 10 * MaxPrice
	r, _ = c.Reserve(ctx, risk.Request{Account: a, Client: "m2", Side: engine.Buy, Type: engine.Market, Qty: 1})
	require.Equal(t, risk.InsufficientFunds, r)
}

func op(kind risk.OpKind, seq, acct uint64) risk.Op {
	return risk.Op{Kind: kind, Seq: seq, Account: acct}
}

func TestSettlementLifecycle(t *testing.T) {
	c, _ := newChecker(t, defaultLimits, nil)
	ctx := context.Background()
	buyer, seller := account(), account()
	require.NoError(t, c.Deposit(ctx, buyer, 0, 10_000))
	require.NoError(t, c.Deposit(ctx, seller, 50, 0))

	// Seller rests 20 @ 100 (order 7); buyer places 20 @ 120 (order 8) and
	// trades 12 @ 100 (price improvement), then cancels the rest.
	r, _ := c.Reserve(ctx, sell(seller, "s", 100, 20))
	require.Equal(t, risk.OK, r)
	r, _ = c.Reserve(ctx, buy(buyer, "b", 120, 20))
	require.Equal(t, risk.OK, r)

	bind := func(seq uint64, acct uint64, cid string, oid uint64) {
		o := op(risk.OpBind, seq, acct)
		o.Client, o.OrderID = cid, oid
		c.Enqueue(o)
	}
	fill := func(seq uint64, acct, oid uint64, price, qty int64) {
		o := op(risk.OpFill, seq, acct)
		o.OrderID, o.Price, o.Qty = oid, price, qty
		c.Enqueue(o)
	}
	bind(1, seller, "s", 7)
	bind(2, buyer, "b", 8)
	fill(3, buyer, 8, 100, 12)
	fill(3, seller, 7, 100, 12)
	fill(3, seller, 7, 100, 12) // duplicate delivery of the same event: must not apply twice
	flush(t, c)

	// Buyer: held 2400 at reservation, 12 filled consumes 1200 and releases
	// 12*20=240 of improvement, 8*120=960 stays held. Gained 12 base.
	require.Equal(t, risk.Balance{BaseAvail: 12, QuoteAvail: 10_000 - 2400 + 240, QuoteHeld: 960, Open: 1}, bal(t, c, buyer))
	// Seller: 8 base still held, 12 sold for 1200 quote.
	require.Equal(t, risk.Balance{BaseAvail: 30, BaseHeld: 8, QuoteAvail: 1200, Open: 1}, bal(t, c, seller))

	// Buyer cancels the remainder; seller's order fills the rest.
	cancel := op(risk.OpCancel, 4, buyer)
	cancel.OrderID = 8
	c.Enqueue(cancel)
	fill(5, seller, 7, 100, 8) // (a later taker consumed the rest)
	flush(t, c)
	require.Equal(t, risk.Balance{BaseAvail: 12, QuoteAvail: 10_000 - 1200}, bal(t, c, buyer))
	require.Equal(t, risk.Balance{BaseAvail: 30, QuoteAvail: 2000}, bal(t, c, seller))

	// Totals are conserved: base 50, quote 10_000.
	require.Equal(t, int64(42), bal(t, c, buyer).BaseAvail+bal(t, c, seller).BaseAvail)
}

func TestSettlementRejectAndGatewayRelease(t *testing.T) {
	c, _ := newChecker(t, defaultLimits, nil)
	ctx := context.Background()
	a := account()
	require.NoError(t, c.Deposit(ctx, a, 0, 1_000))
	_, _ = c.Reserve(ctx, buy(a, "x", 100, 5))
	_, _ = c.Reserve(ctx, buy(a, "y", 100, 5))
	require.Equal(t, int64(1_000), bal(t, c, a).QuoteHeld)

	o := op(risk.OpRelease, 9, a)
	o.Client = "x"
	c.Enqueue(o)
	flush(t, c)
	require.Equal(t, risk.Balance{QuoteAvail: 500, QuoteHeld: 500, Open: 1}, bal(t, c, a))

	// Releasing twice (engine reject replayed, or the gateway releasing too) is harmless.
	c.Enqueue(o)
	require.NoError(t, c.Release(ctx, a, "x"))
	require.NoError(t, c.Release(ctx, a, "y"))
	require.NoError(t, c.Release(ctx, a, "y"))
	flush(t, c)
	require.Equal(t, risk.Balance{QuoteAvail: 1_000}, bal(t, c, a))
}

// FR-3 acceptance: concurrent reservations never produce a negative available
// balance, and never hold more than the account owns.
func TestConcurrentReservationsNeverOverdraw(t *testing.T) {
	lim := risk.Limits{MaxOrderQty: 1000, MaxPrice: 10_000} // no open or rate limit: pure funds race
	c, _ := newChecker(t, lim, nil)
	ctx := context.Background()
	a := account()
	const funds = 1_000 // quote units, each order costs 10
	require.NoError(t, c.Deposit(ctx, a, 0, funds))

	var (
		wg      sync.WaitGroup
		ok      atomic.Int64
		stop    = make(chan struct{})
		sampled sync.WaitGroup
	)
	// A sampler watches the balance throughout: avail must never go negative
	// and avail+held must always equal the deposit.
	sampled.Add(1)
	go func() {
		defer sampled.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			b, err := c.Balance(ctx, a)
			if err == nil && (b.QuoteAvail < 0 || b.QuoteHeld < 0 || b.QuoteAvail+b.QuoteHeld != funds) {
				t.Errorf("invariant broken: %+v", b)
				return
			}
		}
	}()
	for i := range 400 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := c.Reserve(ctx, buy(a, fmt.Sprint("c", i), 10, 1))
			if err != nil {
				t.Error(err)
				return
			}
			if r == risk.OK {
				ok.Add(1)
			} else if r != risk.InsufficientFunds {
				t.Errorf("unexpected reason %v", r)
			}
		}()
	}
	wg.Wait()
	close(stop)
	sampled.Wait()

	// Exactly funds/cost orders could be admitted, no more and no fewer.
	require.Equal(t, int64(100), ok.Load())
	require.Equal(t, risk.Balance{QuoteHeld: funds, Open: 100}, bal(t, c, a))
}

func TestAuditAndLargeValues(t *testing.T) {
	// Values near the documented bound stay exact through the double-based Lua.
	lim := risk.Limits{MaxOrderQty: 1 << 20, MaxPrice: 1 << 30}
	c, _ := newChecker(t, lim, nil)
	ctx := context.Background()
	a := account()
	big := risk.MaxBalance - 1
	require.NoError(t, c.Deposit(ctx, a, 0, big))
	require.Error(t, c.Deposit(ctx, a, 0, 2), "above MaxBalance is refused")

	r, err := c.Reserve(ctx, buy(a, "big", 1<<30, 1<<20)) // 2^50
	require.NoError(t, err)
	require.Equal(t, risk.OK, r)
	b := bal(t, c, a)
	require.Equal(t, big-(1<<50), b.QuoteAvail)
	require.Equal(t, int64(1<<50), b.QuoteHeld)

	// One fill of 3 @ 777 at reserve price 2^30: exact arithmetic, all integers.
	o := op(risk.OpBind, 1, a)
	o.Client, o.OrderID = "big", 1
	c.Enqueue(o)
	f := op(risk.OpFill, 2, a)
	f.OrderID, f.Price, f.Qty = 1, 777, 3
	c.Enqueue(f)
	flush(t, c)
	b = bal(t, c, a)
	require.Equal(t, int64(3), b.BaseAvail)
	require.Equal(t, int64(1<<50)-3*(1<<30), b.QuoteHeld)
	require.Equal(t, big-(1<<50)+3*(1<<30)-3*777, b.QuoteAvail)

	au, err := c.Audit(ctx)
	require.NoError(t, err)
	require.Zero(t, au.Negative)
	require.GreaterOrEqual(t, au.Accounts, 1)
}
