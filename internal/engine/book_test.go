package engine_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/itsdamian/mini-exchange/internal/engine"
)

type harness struct {
	t    *testing.T
	b    *engine.Book
	next uint64
	ts   int64
}

func newHarness(t *testing.T) *harness {
	return &harness{t: t, b: engine.NewBook(engine.Config{MaxOrders: 64, MaxLevels: 16}), next: 1}
}

func (h *harness) apply(c engine.Command) []engine.Event {
	h.ts++
	c.Ts = h.ts
	return h.b.Apply(&c, nil)
}

// place submits a LIMIT GTC order and returns the events.
func (h *harness) limit(acct uint64, side engine.Side, price, qty int64) (uint64, []engine.Event) {
	return h.place(acct, side, engine.Limit, engine.GTC, price, qty)
}

func (h *harness) place(acct uint64, side engine.Side, typ engine.OrderType, tif engine.TimeInForce, price, qty int64) (uint64, []engine.Event) {
	id := h.next
	h.next++
	return id, h.apply(engine.Command{
		Kind: engine.CmdPlace, Side: side, Type: typ, TIF: tif,
		OrderID: id, AccountID: acct, Price: price, Qty: qty,
	})
}

func (h *harness) cancel(acct, id uint64) []engine.Event {
	return h.apply(engine.Command{Kind: engine.CmdCancel, OrderID: id, AccountID: acct})
}

func kinds(evs []engine.Event) []engine.EventKind {
	k := make([]engine.EventKind, len(evs))
	for i := range evs {
		k[i] = evs[i].Kind
	}
	return k
}

func TestLimitRestsWhenNoCross(t *testing.T) {
	h := newHarness(t)
	id, evs := h.limit(1, engine.Buy, 100, 10)
	require.Equal(t, []engine.EventKind{engine.EvAccepted}, kinds(evs))
	require.Equal(t, int64(10), evs[0].Qty)
	q, ok := h.b.Resting(id)
	require.True(t, ok)
	require.Equal(t, int64(10), q)
	bid, ok := h.b.BestBid()
	require.True(t, ok)
	require.Equal(t, int64(100), bid)
	_, ok = h.b.BestAsk()
	require.False(t, ok)
}

func TestFullFillAtMakerPrice(t *testing.T) {
	h := newHarness(t)
	maker, _ := h.limit(1, engine.Sell, 100, 10)
	taker, evs := h.limit(2, engine.Buy, 105, 10) // willing to pay more
	require.Equal(t, []engine.EventKind{engine.EvAccepted, engine.EvTrade}, kinds(evs))
	tr := evs[1]
	require.Equal(t, taker, tr.OrderID)
	require.Equal(t, maker, tr.MakerOrderID)
	require.Equal(t, uint64(2), tr.AccountID)
	require.Equal(t, uint64(1), tr.MakerAccountID)
	require.Equal(t, int64(100), tr.Price, "trade executes at the resting price")
	require.Equal(t, int64(10), tr.Qty)
	require.Equal(t, engine.Buy, tr.Side)
	require.Zero(t, h.b.Len())
}

func TestPartialFillTakerRests(t *testing.T) {
	h := newHarness(t)
	h.limit(1, engine.Sell, 100, 4)
	taker, evs := h.limit(2, engine.Buy, 100, 10)
	require.Equal(t, []engine.EventKind{engine.EvAccepted, engine.EvTrade}, kinds(evs))
	require.Equal(t, int64(4), evs[1].Qty)
	q, ok := h.b.Resting(taker)
	require.True(t, ok)
	require.Equal(t, int64(6), q)
	bid, _ := h.b.BestBid()
	require.Equal(t, int64(100), bid)
}

func TestPartialFillMakerStaysResting(t *testing.T) {
	h := newHarness(t)
	maker, _ := h.limit(1, engine.Sell, 100, 10)
	_, evs := h.limit(2, engine.Buy, 100, 3)
	require.Equal(t, []engine.EventKind{engine.EvAccepted, engine.EvTrade}, kinds(evs))
	q, ok := h.b.Resting(maker)
	require.True(t, ok)
	require.Equal(t, int64(7), q)
}

func TestPriceTimePriority(t *testing.T) {
	h := newHarness(t)
	first, _ := h.limit(1, engine.Sell, 101, 5)
	second, _ := h.limit(2, engine.Sell, 101, 5) // same price, later
	better, _ := h.limit(3, engine.Sell, 100, 5) // better price, latest
	_, evs := h.limit(4, engine.Buy, 101, 12)

	var makers []uint64
	var prices []int64
	for _, e := range evs {
		if e.Kind == engine.EvTrade {
			makers = append(makers, e.MakerOrderID)
			prices = append(prices, e.Price)
		}
	}
	require.Equal(t, []uint64{better, first, second}, makers)
	require.Equal(t, []int64{100, 101, 101}, prices)
	q, _ := h.b.Resting(second)
	require.Equal(t, int64(3), q, "time priority: second filled last, partially")
}

func TestMatchSweepsMultipleLevels(t *testing.T) {
	h := newHarness(t)
	h.limit(1, engine.Buy, 98, 5)
	h.limit(1, engine.Buy, 99, 5)
	h.limit(1, engine.Buy, 100, 5)
	_, evs := h.limit(2, engine.Sell, 99, 20) // sells down to 99 only
	var trades []engine.Event
	for _, e := range evs {
		if e.Kind == engine.EvTrade {
			trades = append(trades, e)
		}
	}
	require.Len(t, trades, 2)
	require.Equal(t, int64(100), trades[0].Price)
	require.Equal(t, int64(99), trades[1].Price)
	ask, _ := h.b.BestAsk()
	require.Equal(t, int64(99), ask, "remainder 10 rests at the limit price")
	bid, _ := h.b.BestBid()
	require.Equal(t, int64(98), bid)
}

func TestIOCCancelsRemainder(t *testing.T) {
	h := newHarness(t)
	h.limit(1, engine.Sell, 100, 4)
	id, evs := h.place(2, engine.Buy, engine.Limit, engine.IOC, 100, 10)
	require.Equal(t, []engine.EventKind{engine.EvAccepted, engine.EvTrade, engine.EvCanceled}, kinds(evs))
	require.Equal(t, int64(6), evs[2].Qty)
	_, ok := h.b.Resting(id)
	require.False(t, ok)
}

func TestIOCNoLiquidity(t *testing.T) {
	h := newHarness(t)
	_, evs := h.place(1, engine.Buy, engine.Limit, engine.IOC, 100, 10)
	require.Equal(t, []engine.EventKind{engine.EvAccepted, engine.EvCanceled}, kinds(evs))
	require.Equal(t, int64(10), evs[1].Qty)
	require.Zero(t, h.b.Len())
}

func TestMarketOrder(t *testing.T) {
	h := newHarness(t)
	h.limit(1, engine.Sell, 100, 3)
	h.limit(1, engine.Sell, 105, 3)
	id, evs := h.place(2, engine.Buy, engine.Market, 0, 0, 10)
	require.Equal(t, []engine.EventKind{engine.EvAccepted, engine.EvTrade, engine.EvTrade, engine.EvCanceled}, kinds(evs))
	require.Equal(t, int64(100), evs[1].Price)
	require.Equal(t, int64(105), evs[2].Price)
	require.Equal(t, int64(4), evs[3].Qty, "unfilled market remainder is cancelled, never rests")
	_, ok := h.b.Resting(id)
	require.False(t, ok)
	require.Zero(t, h.b.Len())
}

func TestMarketIgnoresPriceField(t *testing.T) {
	h := newHarness(t)
	h.limit(1, engine.Sell, 100, 1)
	_, evs := h.place(2, engine.Buy, engine.Market, engine.GTC, -5, 1)
	require.Equal(t, []engine.EventKind{engine.EvAccepted, engine.EvTrade}, kinds(evs))
	require.Zero(t, evs[0].Price)
}

func TestSelfTradePreventionCancelsNewest(t *testing.T) {
	h := newHarness(t)
	h.limit(2, engine.Sell, 100, 5) // first in the queue, other account
	mine, _ := h.limit(1, engine.Sell, 100, 5)
	// Account 1 buys: first maker is account 2 (trade), second is its own
	// resting order => the taker's remainder is cancelled, resting untouched.
	taker, evs := h.limit(1, engine.Buy, 100, 8)
	require.Equal(t, []engine.EventKind{engine.EvAccepted, engine.EvTrade, engine.EvCanceled}, kinds(evs))
	require.Equal(t, int64(5), evs[1].Qty)
	require.Equal(t, int64(3), evs[2].Qty)
	require.Equal(t, taker, evs[2].OrderID)
	q, ok := h.b.Resting(mine)
	require.True(t, ok)
	require.Equal(t, int64(5), q, "resting order of the same account is untouched")
	_, ok = h.b.Resting(taker)
	require.False(t, ok, "STP remainder must not rest even for GTC")
}

func TestSelfTradePreventionImmediate(t *testing.T) {
	h := newHarness(t)
	mine, _ := h.limit(1, engine.Sell, 100, 5)
	_, evs := h.limit(1, engine.Buy, 100, 5)
	require.Equal(t, []engine.EventKind{engine.EvAccepted, engine.EvCanceled}, kinds(evs))
	require.Equal(t, int64(5), evs[1].Qty)
	_, ok := h.b.Resting(mine)
	require.True(t, ok)
}

func TestCancel(t *testing.T) {
	h := newHarness(t)
	a, _ := h.limit(1, engine.Buy, 100, 5)
	b2, _ := h.limit(1, engine.Buy, 100, 7)
	c, _ := h.limit(1, engine.Buy, 100, 9)

	evs := h.cancel(1, b2) // middle of the FIFO
	require.Equal(t, []engine.EventKind{engine.EvCanceled}, kinds(evs))
	require.Equal(t, int64(7), evs[0].Qty)
	require.Equal(t, 2, h.b.Len())

	// remaining queue order is preserved: a then c
	_, tr := h.limit(2, engine.Sell, 100, 14)
	var makers []uint64
	for _, e := range tr {
		if e.Kind == engine.EvTrade {
			makers = append(makers, e.MakerOrderID)
		}
	}
	require.Equal(t, []uint64{a, c}, makers)
}

func TestCancelLastOrderRemovesLevel(t *testing.T) {
	h := newHarness(t)
	id, _ := h.limit(1, engine.Sell, 100, 5)
	h.cancel(1, id)
	_, ok := h.b.BestAsk()
	require.False(t, ok)
	require.Zero(t, h.b.Len())
}

func TestRejects(t *testing.T) {
	tests := []struct {
		name string
		cmd  engine.Command
		want engine.RejectReason
	}{
		{"zero price", engine.Command{Kind: engine.CmdPlace, Side: engine.Buy, Type: engine.Limit, TIF: engine.GTC, OrderID: 9, AccountID: 1, Price: 0, Qty: 1}, engine.RejectInvalidPrice},
		{"negative price", engine.Command{Kind: engine.CmdPlace, Side: engine.Buy, Type: engine.Limit, TIF: engine.GTC, OrderID: 9, AccountID: 1, Price: -1, Qty: 1}, engine.RejectInvalidPrice},
		{"zero qty", engine.Command{Kind: engine.CmdPlace, Side: engine.Buy, Type: engine.Limit, TIF: engine.GTC, OrderID: 9, AccountID: 1, Price: 1, Qty: 0}, engine.RejectInvalidQuantity},
		{"negative qty", engine.Command{Kind: engine.CmdPlace, Side: engine.Sell, Type: engine.Market, OrderID: 9, AccountID: 1, Qty: -3}, engine.RejectInvalidQuantity},
		{"bad side", engine.Command{Kind: engine.CmdPlace, Side: 0, Type: engine.Limit, TIF: engine.GTC, OrderID: 9, AccountID: 1, Price: 1, Qty: 1}, engine.RejectInvalidOrder},
		{"bad type", engine.Command{Kind: engine.CmdPlace, Side: engine.Buy, Type: 0, OrderID: 9, AccountID: 1, Price: 1, Qty: 1}, engine.RejectInvalidOrder},
		{"bad tif", engine.Command{Kind: engine.CmdPlace, Side: engine.Buy, Type: engine.Limit, TIF: 0, OrderID: 9, AccountID: 1, Price: 1, Qty: 1}, engine.RejectInvalidOrder},
		{"bad kind", engine.Command{Kind: 0, OrderID: 9, AccountID: 1}, engine.RejectInvalidOrder},
		{"cancel unknown", engine.Command{Kind: engine.CmdCancel, OrderID: 12345, AccountID: 1}, engine.RejectUnknownOrder},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			evs := h.apply(tc.cmd)
			require.Len(t, evs, 1)
			require.Equal(t, engine.EvRejected, evs[0].Kind)
			require.Equal(t, tc.want, evs[0].Reject)
			require.Zero(t, h.b.Len())
		})
	}
}

func TestRejectDuplicateOrderID(t *testing.T) {
	h := newHarness(t)
	id, _ := h.limit(1, engine.Buy, 100, 5)
	evs := h.apply(engine.Command{Kind: engine.CmdPlace, Side: engine.Buy, Type: engine.Limit, TIF: engine.GTC, OrderID: id, AccountID: 1, Price: 99, Qty: 1})
	require.Equal(t, engine.RejectDuplicateOrderID, evs[0].Reject)
	require.Equal(t, 1, h.b.Len())
}

func TestCancelWrongAccountIsUnknown(t *testing.T) {
	h := newHarness(t)
	id, _ := h.limit(1, engine.Buy, 100, 5)
	evs := h.cancel(2, id)
	require.Equal(t, engine.RejectUnknownOrder, evs[0].Reject)
	_, ok := h.b.Resting(id)
	require.True(t, ok)
}

func TestCancelTwiceIsUnknown(t *testing.T) {
	h := newHarness(t)
	id, _ := h.limit(1, engine.Buy, 100, 5)
	h.cancel(1, id)
	evs := h.cancel(1, id)
	require.Equal(t, engine.RejectUnknownOrder, evs[0].Reject)
}

func TestSequenceNumbersAreContiguousAndTimestampsCopied(t *testing.T) {
	h := newHarness(t)
	var all []engine.Event
	h.limit(1, engine.Sell, 100, 5)
	_, e := h.limit(2, engine.Buy, 100, 9)
	all = append(all, e...)
	all = append(all, h.cancel(2, 9999)...)
	for i, e := range all {
		require.Equal(t, uint64(i)+2, e.Seq) // first order produced seq 1
		require.NotZero(t, e.Ts)
	}
	require.Equal(t, all[len(all)-1].Seq, h.b.Seq())
}

func TestSlabGrowthKeepsWorking(t *testing.T) {
	b := engine.NewBook(engine.Config{MaxOrders: 2, MaxLevels: 1})
	var out []engine.Event
	for i := uint64(1); i <= 100; i++ {
		c := engine.Command{Kind: engine.CmdPlace, Side: engine.Buy, Type: engine.Limit, TIF: engine.GTC, OrderID: i, AccountID: i, Price: int64(i), Qty: 1, Ts: 1}
		out = b.Apply(&c, out[:0])
	}
	require.Equal(t, 100, b.Len())
	c := engine.Command{Kind: engine.CmdPlace, Side: engine.Sell, Type: engine.Market, OrderID: 1000, AccountID: 1000, Qty: 100, Ts: 1}
	_ = b.Apply(&c, out[:0])
	require.Zero(t, b.Len())
}
