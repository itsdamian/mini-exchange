package ledger_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/itsdamian/mini-exchange/internal/engine"
	"github.com/itsdamian/mini-exchange/internal/eventlog/kafkatest"
	"github.com/itsdamian/mini-exchange/internal/ledger"
	"github.com/itsdamian/mini-exchange/internal/ledger/pgtest"
	"github.com/itsdamian/mini-exchange/internal/risk/redistest"
)

func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.Terminate()
	kafkatest.Terminate()
	redistest.Terminate()
	os.Exit(code)
}

var assets = ledger.Assets{Base: "BTC", Quote: "USD"}

func openStore(t *testing.T) *ledger.Store {
	t.Helper()
	s, err := ledger.Open(context.Background(), pgtest.Shared(t).NewDSN(t))
	require.NoError(t, err)
	t.Cleanup(s.Close)
	return s
}

func deposit(t *testing.T, s *ledger.Store, acct uint64, base, quote int64) {
	t.Helper()
	p, err := ledger.DepositPosting(fmt.Sprintf("seed-%d", acct), "BTC-USD", assets, acct, base, quote)
	require.NoError(t, err)
	_, err = s.Apply(context.Background(), []ledger.Posting{p})
	require.NoError(t, err)
}

func tradeEv(seq uint64, side engine.Side, taker, maker uint64, price, qty int64) engine.Event {
	return engine.Event{Kind: engine.EvTrade, Seq: seq, Side: side, AccountID: taker, MakerAccountID: maker, Price: price, Qty: qty}
}

func posting(t *testing.T, ev engine.Event, f ledger.Fees) ledger.Posting {
	t.Helper()
	p, err := ledger.TradePosting("BTC-USD", assets, f, ev)
	require.NoError(t, err)
	return p
}

func balance(t *testing.T, s *ledger.Store, kind ledger.AcctKind, owner uint64, asset string) int64 {
	t.Helper()
	bs, err := s.Balances(context.Background())
	require.NoError(t, err)
	for _, b := range bs {
		if b.Acct == kind && b.Owner == owner && b.Asset == asset {
			return b.Amount
		}
	}
	return 0
}

func TestApplyIsIdempotent(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	deposit(t, s, 1, 0, 10_000)
	deposit(t, s, 2, 10, 0)

	p := posting(t, tradeEv(1, engine.Buy, 1, 2, 100, 3), ledger.Fees{})
	n, err := s.Apply(ctx, []ledger.Posting{p})
	require.NoError(t, err)
	require.Equal(t, 1, n)
	n, err = s.Apply(ctx, []ledger.Posting{p, p})
	require.NoError(t, err)
	require.Zero(t, n, "the same event id settles once")

	require.EqualValues(t, 10_000-300, balance(t, s, ledger.Customer, 1, "USD"))
	require.EqualValues(t, 3, balance(t, s, ledger.Customer, 1, "BTC"))
	require.EqualValues(t, 300, balance(t, s, ledger.Customer, 2, "USD"))
	require.EqualValues(t, 7, balance(t, s, ledger.Customer, 2, "BTC"))
}

func TestFeeAccountCollectsFees(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	deposit(t, s, 1, 0, 100_000)
	deposit(t, s, 2, 10, 0)
	_, err := s.Apply(ctx, []ledger.Posting{posting(t, tradeEv(1, engine.Buy, 1, 2, 1000, 10), ledger.Fees{TakerBps: 10, MakerBps: 5})})
	require.NoError(t, err)
	require.EqualValues(t, 15, balance(t, s, ledger.FeeAcct, 0, "USD")) // taker 10 + maker 5
	require.EqualValues(t, 100_000-10_000-10, balance(t, s, ledger.Customer, 1, "USD"))
	require.EqualValues(t, 10_000-5, balance(t, s, ledger.Customer, 2, "USD"))
}

func TestNegativeCustomerBalanceIsRefusedAndRollsBack(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	deposit(t, s, 1, 0, 1_000)
	deposit(t, s, 2, 10, 0)
	deposit(t, s, 3, 10, 0)

	ok := posting(t, tradeEv(1, engine.Buy, 1, 2, 100, 5), ledger.Fees{})   // costs 500: fine
	bad := posting(t, tradeEv(2, engine.Buy, 1, 3, 100, 10), ledger.Fees{}) // costs 1000, only 500 left
	n, err := s.Apply(ctx, []ledger.Posting{ok, bad})
	require.Error(t, err)
	require.True(t, ledger.IsPoison(err), "a constraint violation is poison, not a retryable error: %v", err)
	require.Zero(t, n)

	// The whole batch rolled back, including the good trade.
	require.EqualValues(t, 1_000, balance(t, s, ledger.Customer, 1, "USD"))
	require.Zero(t, balance(t, s, ledger.Customer, 2, "USD"))
	n, err = s.Apply(ctx, []ledger.Posting{ok})
	require.NoError(t, err)
	require.Equal(t, 1, n, "the rolled-back trade is still unsettled and applies cleanly")
}

// FR-6 acceptance: applying the full log twice yields identical balances.
func TestApplyingTheSameTradesTwiceYieldsIdenticalBalances(t *testing.T) {
	ctx := context.Background()
	s := openStore(t)
	const accounts = 6
	for a := uint64(1); a <= accounts; a++ {
		deposit(t, s, a, 1_000_000, 1_000_000_000)
	}
	rng := rand.New(rand.NewPCG(7, 7))
	var ps []ledger.Posting
	for seq := uint64(1); seq <= 2_000; seq++ {
		taker, maker := uint64(rng.IntN(accounts))+1, uint64(rng.IntN(accounts))+1
		if taker == maker {
			maker = taker%accounts + 1
		}
		side := engine.Buy
		if rng.IntN(2) == 0 {
			side = engine.Sell
		}
		ps = append(ps, posting(t, tradeEv(seq, side, taker, maker, int64(rng.IntN(500))+1, int64(rng.IntN(20))+1), ledger.Fees{TakerBps: 5, MakerBps: 1}))
	}
	n, err := s.Apply(ctx, ps)
	require.NoError(t, err)
	require.Equal(t, len(ps), n)
	first, err := s.Balances(ctx)
	require.NoError(t, err)

	n, err = s.Apply(ctx, ps)
	require.NoError(t, err)
	require.Zero(t, n)
	second, err := s.Balances(ctx)
	require.NoError(t, err)
	require.Equal(t, first, second)

	// Sum of all balances is zero per asset.
	sums := map[string]int64{}
	for _, b := range first {
		sums[b.Asset] += b.Amount
	}
	require.Equal(t, map[string]int64{"BTC": 0, "USD": 0}, sums)
}
