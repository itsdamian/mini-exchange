package engine_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/itsdamian/mini-exchange/internal/engine"
	"github.com/itsdamian/mini-exchange/internal/refbook"
)

// genCommands draws a command sequence over a small price/account space so
// that crossing, self-trades and cancels are frequent.
func genCommands(t *rapid.T) []engine.Command {
	n := rapid.IntRange(1, 300).Draw(t, "n")
	cmds := make([]engine.Command, 0, n)
	var placed []uint64
	next := uint64(1)
	for i := range n {
		ts := int64(i + 1)
		acct := rapid.Uint64Range(1, 4).Draw(t, "acct")
		if len(placed) > 0 && rapid.IntRange(0, 99).Draw(t, "kind") < 25 {
			id := rapid.SampledFrom(placed).Draw(t, "cancelID")
			cmds = append(cmds, engine.Command{Kind: engine.CmdCancel, OrderID: id, AccountID: acct, Ts: ts})
			continue
		}
		c := engine.Command{
			Kind: engine.CmdPlace, OrderID: next, AccountID: acct, Ts: ts,
			Side: rapid.SampledFrom([]engine.Side{engine.Buy, engine.Sell}).Draw(t, "side"),
			Type: rapid.SampledFrom([]engine.OrderType{engine.Limit, engine.Limit, engine.Limit, engine.Market}).Draw(t, "type"),
			TIF:  rapid.SampledFrom([]engine.TimeInForce{engine.GTC, engine.GTC, engine.IOC}).Draw(t, "tif"),
			// slightly out-of-range values keep the reject paths exercised
			Price: rapid.Int64Range(-1, 12).Draw(t, "price"),
			Qty:   rapid.Int64Range(-1, 20).Draw(t, "qty"),
		}
		placed = append(placed, next)
		next++
		cmds = append(cmds, c)
	}
	return cmds
}

func TestPropertyInvariants(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		cmds := genCommands(t)
		b := engine.NewBook(engine.Config{MaxOrders: 8, MaxLevels: 4})

		accepted := map[uint64]int64{} // order id -> accepted qty
		filled := map[uint64]int64{}   // order id -> traded qty (as taker or maker)
		canceled := map[uint64]int64{} // order id -> cancelled qty
		arrival := map[uint64]int{}    // order id -> acceptance order
		accountOf := map[uint64]uint64{}
		var out []engine.Event
		var lastSeq uint64

		for i := range cmds {
			c := cmds[i]
			out = b.Apply(&c, out[:0])

			var lastTradePrice int64
			var lastTradeMaker uint64
			sawTrade := false
			for _, e := range out {
				require.Equal(t, lastSeq+1, e.Seq, "sequence numbers are contiguous")
				lastSeq = e.Seq
				switch e.Kind {
				case engine.EvAccepted:
					accepted[e.OrderID] = e.Qty
					arrival[e.OrderID] = i
					accountOf[e.OrderID] = e.AccountID
				case engine.EvCanceled:
					canceled[e.OrderID] += e.Qty
				case engine.EvTrade:
					require.Positive(t, e.Qty)
					require.NotEqual(t, e.AccountID, e.MakerAccountID, "no self trades")
					filled[e.OrderID] += e.Qty
					filled[e.MakerOrderID] += e.Qty
					if c.Type == engine.Limit {
						if c.Side == engine.Buy {
							require.LessOrEqual(t, e.Price, c.Price, "buyer never pays above its limit")
						} else {
							require.GreaterOrEqual(t, e.Price, c.Price, "seller never receives below its limit")
						}
					}
					if sawTrade { // price-time priority within one taker
						if c.Side == engine.Buy {
							require.GreaterOrEqual(t, e.Price, lastTradePrice, "asks consumed lowest first")
						} else {
							require.LessOrEqual(t, e.Price, lastTradePrice, "bids consumed highest first")
						}
						if e.Price == lastTradePrice {
							require.Greater(t, arrival[e.MakerOrderID], arrival[lastTradeMaker], "FIFO within a level")
						}
					}
					sawTrade, lastTradePrice, lastTradeMaker = true, e.Price, e.MakerOrderID
				}
			}

			// the book is never crossed
			bid, hasBid := b.BestBid()
			ask, hasAsk := b.BestAsk()
			if hasBid && hasAsk {
				require.Less(t, bid, ask, "book crossed after op %d", i)
			}
		}

		// quantity is conserved per order: accepted = filled + canceled + resting
		resting := 0
		for id, q := range accepted {
			rest, ok := b.Resting(id)
			if ok {
				resting++
			}
			require.Equal(t, q, filled[id]+canceled[id]+rest, "order %d conservation", id)
		}
		require.Equal(t, resting, b.Len())
	})
}

func TestPropertyDeterministic(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		cmds := genCommands(t)
		run := func() (uint64, uint64) {
			b := engine.NewBook(engine.Config{MaxOrders: 4, MaxLevels: 2})
			h := engine.HashSeed
			var out []engine.Event
			for i := range cmds {
				c := cmds[i]
				out = b.Apply(&c, out[:0])
				h = engine.HashEvents(h, out)
			}
			return h, b.Hash()
		}
		h1, s1 := run()
		h2, s2 := run()
		require.Equal(t, h1, h2, "same input => same output hash")
		require.Equal(t, s1, s2, "same input => same book state hash")
	})
}

func TestPropertyAgreesWithReference(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		cmds := genCommands(t)
		fast := engine.NewBook(engine.Config{MaxOrders: 4, MaxLevels: 2})
		ref := refbook.New()
		var fo, ro []engine.Event
		for i := range cmds {
			c := cmds[i]
			fo = fast.Apply(&c, fo[:0])
			ro = ref.Apply(&c, ro[:0])
			require.Equal(t, ro, fo, "events differ at op %d (%+v)", i, c)
		}
		require.Equal(t, ref.Len(), fast.Len())
		require.Equal(t, ref.Hash(), fast.Hash())
	})
}
