package ledger

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/itsdamian/mini-exchange/internal/engine"
)

var btcusd = Assets{Base: "BTC", Quote: "USD"}

func trade(side engine.Side, taker, maker uint64, price, qty int64) engine.Event {
	return engine.Event{Kind: engine.EvTrade, Seq: 7, Side: side, AccountID: taker, MakerAccountID: maker, Price: price, Qty: qty}
}

func sumBy(es []Entry) map[string]int64 {
	m := map[string]int64{}
	for _, e := range es {
		m[e.Asset] += e.Delta
	}
	return m
}

func TestSplitSymbol(t *testing.T) {
	a, err := SplitSymbol("BTC-USD")
	require.NoError(t, err)
	require.Equal(t, btcusd, a)
	for _, bad := range []string{"", "BTC", "-USD", "BTC-", "A-B-C"} {
		_, err := SplitSymbol(bad)
		require.Error(t, err, bad)
	}
}

func TestTradePostingBuyerTaker(t *testing.T) {
	p, err := TradePosting("BTC-USD", btcusd, Fees{}, trade(engine.Buy, 1, 2, 100, 3))
	require.NoError(t, err)
	require.Equal(t, "BTC-USD:7", p.EventID)
	require.ElementsMatch(t, []Entry{
		{EntryCash, Customer, 1, "USD", -300}, {EntryCash, Customer, 2, "USD", 300},
		{EntryAsset, Customer, 2, "BTC", -3}, {EntryAsset, Customer, 1, "BTC", 3},
	}, p.Entries)
}

func TestTradePostingSellerTaker(t *testing.T) {
	p, err := TradePosting("BTC-USD", btcusd, Fees{}, trade(engine.Sell, 1, 2, 100, 3))
	require.NoError(t, err)
	require.ElementsMatch(t, []Entry{
		{EntryCash, Customer, 2, "USD", -300}, {EntryCash, Customer, 1, "USD", 300},
		{EntryAsset, Customer, 1, "BTC", -3}, {EntryAsset, Customer, 2, "BTC", 3},
	}, p.Entries)
}

func TestTradePostingFeesFollowTakerAndMaker(t *testing.T) {
	// Seller is the taker: seller pays the taker fee, buyer (maker) the maker fee.
	p, err := TradePosting("BTC-USD", btcusd, Fees{TakerBps: 10, MakerBps: 2}, trade(engine.Sell, 1, 2, 1000, 10))
	require.NoError(t, err) // notional 10000: taker fee 10, maker fee 2
	require.Contains(t, p.Entries, Entry{EntryFee, Customer, 1, "USD", -10})
	require.Contains(t, p.Entries, Entry{EntryFee, Customer, 2, "USD", -2})
	require.Contains(t, p.Entries, Entry{EntryFee, FeeAcct, 0, "USD", 10})
	require.Contains(t, p.Entries, Entry{EntryFee, FeeAcct, 0, "USD", 2})
	require.NoError(t, p.Balanced())
}

func TestFeeRoundsDownAndSkipsZero(t *testing.T) {
	f, ok := feeOf(999, 10) // 0.999
	require.True(t, ok)
	require.Zero(t, f)
	f, ok = feeOf(1999, 10) // 1.999
	require.True(t, ok)
	require.EqualValues(t, 1, f)
	p, err := TradePosting("BTC-USD", btcusd, Fees{TakerBps: 1, MakerBps: 1}, trade(engine.Buy, 1, 2, 10, 1))
	require.NoError(t, err)
	require.Len(t, p.Entries, 4, "a fee that rounds to zero posts no fee entries")
}

func TestTradePostingRejectsBadInput(t *testing.T) {
	cases := map[string]engine.Event{
		"not a trade":       {Kind: engine.EvAccepted, Seq: 1},
		"zero quantity":     trade(engine.Buy, 1, 2, 100, 0),
		"zero price":        trade(engine.Buy, 1, 2, 0, 1),
		"zero account":      trade(engine.Buy, 0, 2, 100, 1),
		"huge account":      trade(engine.Buy, math.MaxUint64, 2, 100, 1),
		"overflow":          trade(engine.Buy, 1, 2, math.MaxInt64, 2),
		"no taker side":     trade(0, 1, 2, 100, 1),
		"negative quantity": trade(engine.Buy, 1, 2, 100, -5),
	}
	for name, ev := range cases {
		_, err := TradePosting("BTC-USD", btcusd, Fees{}, ev)
		require.Error(t, err, name)
		require.True(t, IsPoison(err), name)
	}
}

func TestFeeOverflowIsPoison(t *testing.T) {
	_, ok := feeOf(math.MaxInt64, math.MaxInt64)
	require.False(t, ok) // bps*notional/10000 exceeds int64
}

func TestDepositPosting(t *testing.T) {
	p, err := DepositPosting("r1", "BTC-USD", btcusd, 5, 10, 0)
	require.NoError(t, err)
	require.Equal(t, "deposit:r1", p.EventID)
	require.Len(t, p.Entries, 2)
	_, err = DepositPosting("", "BTC-USD", btcusd, 5, 10, 0)
	require.Error(t, err)
	_, err = DepositPosting("r", "BTC-USD", btcusd, 5, 0, 0)
	require.Error(t, err)
	_, err = DepositPosting("r", "BTC-USD", btcusd, 5, -1, 3)
	require.Error(t, err)
}

func TestUnbalancedPostingIsDetected(t *testing.T) {
	p := Posting{EventID: "x", Entries: []Entry{{EntryCash, Customer, 1, "USD", -5}, {EntryCash, Customer, 2, "USD", 4}}}
	require.True(t, IsPoison(p.Balanced()))
}

// Every trade posting balances to zero per asset, with fees or without.
func TestTradePostingAlwaysBalancesProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		side := rapid.SampledFrom([]engine.Side{engine.Buy, engine.Sell}).Draw(t, "side")
		ev := trade(side,
			rapid.Uint64Range(1, 1<<40).Draw(t, "taker"), rapid.Uint64Range(1, 1<<40).Draw(t, "maker"),
			rapid.Int64Range(1, 1<<30).Draw(t, "price"), rapid.Int64Range(1, 1<<20).Draw(t, "qty"))
		f := Fees{TakerBps: rapid.Int64Range(0, 500).Draw(t, "taker bps"), MakerBps: rapid.Int64Range(0, 500).Draw(t, "maker bps")}
		p, err := TradePosting("BTC-USD", btcusd, f, ev)
		require.NoError(t, err)
		for asset, s := range sumBy(p.Entries) {
			require.Zero(t, s, asset)
		}
	})
}
