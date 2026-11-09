// Package enginetest generates deterministic random command streams for
// tests and benchmarks of package engine. Same seed and profile always give
// the same stream.
package enginetest

import (
	"math/rand/v2"

	"github.com/itsdamian/mini-exchange/internal/engine"
)

// Profile shapes the generated flow. Percentages are out of 100 and are
// applied in the order: invalid, cancel, then order kind.
type Profile struct {
	Accounts   int   // number of distinct accounts (small => self-trades happen)
	MidPrice   int64 // ticks
	Depth      int64 // passive orders are placed within [1, Depth] ticks of mid
	MaxQty     int64
	TargetBook int // when more orders rest than this, cancels become likely

	InvalidPct    int // malformed price/quantity/ids
	CancelPct     int // cancel of a (usually) live order
	MarketPct     int
	IOCPct        int
	AggressivePct int // share of GTC limits priced to cross the spread
}

// DefaultProfile is a mixed flow with a book of roughly 1000 resting orders.
func DefaultProfile() Profile {
	return Profile{
		Accounts: 16, MidPrice: 100_000, Depth: 50, MaxQty: 100, TargetBook: 1000,
		InvalidPct: 1, CancelPct: 28, MarketPct: 6, IOCPct: 10, AggressivePct: 12,
	}
}

type liveOrder struct {
	id      uint64
	account uint64
}

// Generate returns n commands. It runs a scratch engine.Book internally only
// to know which orders are still resting, so cancels target real orders; the
// returned commands are independent of any particular book instance.
func Generate(n int, seed uint64, p Profile) []engine.Command {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	scratch := engine.NewBook(engine.Config{MaxOrders: p.TargetBook * 2, MaxLevels: int(p.Depth) * 4})
	var events []engine.Event
	var live []liveOrder
	cmds := make([]engine.Command, 0, n)
	nextID := uint64(1)
	ts := int64(0)

	pickLive := func() (liveOrder, bool) {
		for len(live) > 0 {
			i := rng.IntN(len(live))
			lo := live[i]
			if _, ok := scratch.Resting(lo.id); ok {
				return lo, true
			}
			live[i] = live[len(live)-1] // lazily drop filled orders
			live = live[:len(live)-1]
		}
		return liveOrder{}, false
	}
	qty := func() int64 { return 1 + rng.Int64N(p.MaxQty) }
	passive := func(side engine.Side) int64 {
		off := 1 + rng.Int64N(p.Depth)
		if side == engine.Buy {
			return p.MidPrice - off
		}
		return p.MidPrice + off
	}
	crossing := func(side engine.Side) int64 {
		off := rng.Int64N(p.Depth)
		if side == engine.Buy {
			return p.MidPrice + off
		}
		return p.MidPrice - off
	}

	for range n {
		ts += 1 + rng.Int64N(1000)
		acct := uint64(1 + rng.IntN(p.Accounts))
		side := engine.Buy
		if rng.IntN(2) == 0 {
			side = engine.Sell
		}
		c := engine.Command{Kind: engine.CmdPlace, Side: side, Type: engine.Limit, TIF: engine.GTC, AccountID: acct, Ts: ts}

		roll := rng.IntN(100)
		cancelPct := p.CancelPct
		if len(live) > p.TargetBook {
			cancelPct = 60
		}
		switch {
		case roll < p.InvalidPct:
			nextID++
			c.OrderID = nextID
			switch rng.IntN(4) {
			case 0:
				c.Price, c.Qty = 0, qty()
			case 1:
				c.Price, c.Qty = passive(side), 0
			case 2:
				c.Price, c.Qty = passive(side), -qty()
			default: // unknown order id
				c = engine.Command{Kind: engine.CmdCancel, OrderID: 1 << 40, AccountID: acct, Ts: ts}
			}
		case roll < p.InvalidPct+cancelPct:
			if lo, ok := pickLive(); ok {
				c = engine.Command{Kind: engine.CmdCancel, OrderID: lo.id, AccountID: lo.account, Ts: ts}
				if rng.IntN(50) == 0 {
					c.AccountID = lo.account + 1000 // wrong owner => unknown order
				}
				break
			}
			fallthrough
		default:
			nextID++
			c.OrderID = nextID
			c.Qty = qty()
			kind := rng.IntN(100)
			switch {
			case kind < p.MarketPct:
				c.Type = engine.Market
			case kind < p.MarketPct+p.IOCPct:
				c.TIF = engine.IOC
				c.Price = crossing(side)
			case kind < p.MarketPct+p.IOCPct+p.AggressivePct:
				c.Price = crossing(side)
			default:
				c.Price = passive(side)
			}
		}

		events = scratch.Apply(&c, events[:0])
		if c.Kind == engine.CmdPlace {
			if _, ok := scratch.Resting(c.OrderID); ok {
				live = append(live, liveOrder{c.OrderID, c.AccountID})
			}
		}
		cmds = append(cmds, c)
	}
	return cmds
}
