// Package refbook is a deliberately naive reference implementation of the
// matching rules in package engine. It keeps every resting order in one flat
// slice and finds the best match by scanning, so it is slow but obviously
// correct. It exists only for differential testing and benchmarks; the
// production engine must produce byte-identical events and book state.
package refbook

import "github.com/itsdamian/mini-exchange/internal/engine"

type resting struct {
	id      uint64
	account uint64
	side    engine.Side
	price   int64
	qty     int64
	arrival uint64 // monotonically increasing: defines time priority
}

// Book is the reference order book.
type Book struct {
	orders  []resting
	seq     uint64
	arrival uint64
}

// New returns an empty reference book.
func New() *Book { return &Book{} }

// Len returns the number of resting orders.
func (b *Book) Len() int { return len(b.orders) }

func (b *Book) emit(out []engine.Event, e engine.Event) []engine.Event {
	b.seq++
	e.Seq = b.seq
	return append(out, e)
}

func (b *Book) find(id uint64) int {
	for i := range b.orders {
		if b.orders[i].id == id {
			return i
		}
	}
	return -1
}

// bestMaker returns the index of the order a taker of the given side would
// trade with first, or -1: best price, then earliest arrival.
func (b *Book) bestMaker(takerSide engine.Side) int {
	best := -1
	for i := range b.orders {
		o := &b.orders[i]
		if o.side == takerSide {
			continue
		}
		if best == -1 {
			best = i
			continue
		}
		cur := &b.orders[best]
		better := false
		switch {
		case o.price == cur.price:
			better = o.arrival < cur.arrival
		case o.side == engine.Sell:
			better = o.price < cur.price // lowest ask first
		default:
			better = o.price > cur.price // highest bid first
		}
		if better {
			best = i
		}
	}
	return best
}

// Apply mirrors engine.Book.Apply.
func (b *Book) Apply(c *engine.Command, out []engine.Event) []engine.Event {
	switch c.Kind {
	case engine.CmdPlace:
		return b.place(c, out)
	case engine.CmdCancel:
		return b.cancel(c, out)
	}
	return b.emit(out, engine.Event{
		Kind: engine.EvRejected, Reject: engine.RejectInvalidOrder, Ts: c.Ts,
		OrderID: c.OrderID, AccountID: c.AccountID,
	})
}

func (b *Book) rejected(out []engine.Event, c *engine.Command, r engine.RejectReason) []engine.Event {
	return b.emit(out, engine.Event{
		Kind: engine.EvRejected, Reject: r, Side: c.Side, Ts: c.Ts,
		OrderID: c.OrderID, AccountID: c.AccountID, Price: c.Price, Qty: c.Qty,
	})
}

func (b *Book) place(c *engine.Command, out []engine.Event) []engine.Event {
	switch {
	case c.Side != engine.Buy && c.Side != engine.Sell:
		return b.rejected(out, c, engine.RejectInvalidOrder)
	case c.Type != engine.Limit && c.Type != engine.Market:
		return b.rejected(out, c, engine.RejectInvalidOrder)
	case c.Type == engine.Limit && c.TIF != engine.GTC && c.TIF != engine.IOC:
		return b.rejected(out, c, engine.RejectInvalidOrder)
	case c.Type == engine.Limit && c.Price <= 0:
		return b.rejected(out, c, engine.RejectInvalidPrice)
	case c.Qty <= 0:
		return b.rejected(out, c, engine.RejectInvalidQuantity)
	case b.find(c.OrderID) >= 0:
		return b.rejected(out, c, engine.RejectDuplicateOrderID)
	}

	price := c.Price
	if c.Type == engine.Market {
		price = 0
	}
	out = b.emit(out, engine.Event{
		Kind: engine.EvAccepted, Side: c.Side, Ts: c.Ts,
		OrderID: c.OrderID, AccountID: c.AccountID, Price: price, Qty: c.Qty,
	})

	rem := c.Qty
	selfTrade := false
	for rem > 0 {
		i := b.bestMaker(c.Side)
		if i < 0 {
			break
		}
		m := &b.orders[i]
		if c.Type == engine.Limit &&
			((c.Side == engine.Buy && m.price > c.Price) || (c.Side == engine.Sell && m.price < c.Price)) {
			break
		}
		if m.account == c.AccountID {
			selfTrade = true
			break
		}
		q := min(rem, m.qty)
		out = b.emit(out, engine.Event{
			Kind: engine.EvTrade, Side: c.Side, Ts: c.Ts,
			OrderID: c.OrderID, AccountID: c.AccountID,
			MakerOrderID: m.id, MakerAccountID: m.account,
			Price: m.price, Qty: q,
		})
		rem -= q
		m.qty -= q
		if m.qty == 0 {
			b.orders = append(b.orders[:i], b.orders[i+1:]...)
		}
	}

	if rem == 0 {
		return out
	}
	if c.Type == engine.Limit && c.TIF == engine.GTC && !selfTrade {
		b.arrival++
		b.orders = append(b.orders, resting{
			id: c.OrderID, account: c.AccountID, side: c.Side,
			price: c.Price, qty: rem, arrival: b.arrival,
		})
		return out
	}
	return b.emit(out, engine.Event{
		Kind: engine.EvCanceled, Side: c.Side, Ts: c.Ts,
		OrderID: c.OrderID, AccountID: c.AccountID, Price: price, Qty: rem,
	})
}

func (b *Book) cancel(c *engine.Command, out []engine.Event) []engine.Event {
	i := b.find(c.OrderID)
	if i < 0 || b.orders[i].account != c.AccountID {
		return b.rejected(out, c, engine.RejectUnknownOrder)
	}
	o := b.orders[i]
	b.orders = append(b.orders[:i], b.orders[i+1:]...)
	return b.emit(out, engine.Event{
		Kind: engine.EvCanceled, Side: o.side, Ts: c.Ts,
		OrderID: c.OrderID, AccountID: c.AccountID, Price: o.price, Qty: o.qty,
	})
}

// Hash returns the same state hash as engine.Book.Hash for an equal book:
// asks from best (lowest) to worst, then bids from best (highest) to worst,
// each price level in arrival order.
func (b *Book) Hash() uint64 {
	h := engine.HashSeed
	h = hashU64(h, b.seq)
	for _, side := range []engine.Side{engine.Sell, engine.Buy} {
		var done []resting
		for {
			// pick the best not-yet-hashed order of this side
			best := -1
			for i := range b.orders {
				o := &b.orders[i]
				if o.side != side || contains(done, o.id) {
					continue
				}
				if best == -1 || b.before(o, &b.orders[best]) {
					best = i
				}
			}
			if best == -1 {
				break
			}
			o := b.orders[best]
			done = append(done, o)
			h = engine.HashResting(h, o.side, o.price, o.id, o.account, o.qty)
		}
	}
	return h
}

func (b *Book) before(o, cur *resting) bool {
	if o.price == cur.price {
		return o.arrival < cur.arrival
	}
	if o.side == engine.Sell {
		return o.price < cur.price
	}
	return o.price > cur.price
}

func contains(rs []resting, id uint64) bool {
	for i := range rs {
		if rs[i].id == id {
			return true
		}
	}
	return false
}

// hashU64 folds one uint64 the same way engine's unexported mix does.
func hashU64(h, v uint64) uint64 {
	for range 8 {
		h ^= v & 0xff
		h *= 1099511628211
		v >>= 8
	}
	return h
}
