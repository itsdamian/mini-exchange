package engine

import "fmt"

// ReplayError reports an event stream that is inconsistent with the book it
// is applied to (a gap, a trade against an unknown order, ...). It means the
// log or snapshot is corrupt, or was produced by a different history.
type ReplayError struct {
	Seq    uint64
	Reason string
}

func (e *ReplayError) Error() string {
	return fmt.Sprintf("engine: replay inconsistent at seq %d: %s", e.Seq, e.Reason)
}

// ApplyEvents rebuilds book state from the events of exactly ONE command (a
// whole batch as produced by Apply). It does not match anything: it applies
// the recorded outcome, so state = snapshot + replay needs no re-execution of
// the original commands, and replay cannot diverge from history.
//
// The rules mirror Apply's event contract (ADR 0003):
//   - Accepted starts an incoming order; Trade reduces it and a resting maker;
//   - Canceled for the incoming order ends it without resting; Canceled for
//     another order removes that resting order;
//   - when the batch ends and the incoming order still has quantity and was
//     not cancelled, it rested (only GTC limits can leave a remainder without
//     a Canceled event).
//
// Sequence numbers must be contiguous with the book's current Seq.
func (b *Book) ApplyEvents(evs []Event) error {
	var (
		pending  Event // the Accepted event of the incoming order
		inbound  bool
		leaves   int64
		finished bool // incoming order cancelled: must not rest
	)
	for i := range evs {
		e := &evs[i]
		if e.Seq != b.seq+1 {
			return &ReplayError{e.Seq, fmt.Sprintf("expected seq %d", b.seq+1)}
		}
		b.seq = e.Seq
		switch e.Kind {
		case EvAccepted:
			if inbound {
				return &ReplayError{e.Seq, "two accepted events in one batch"}
			}
			pending, inbound, leaves, finished = *e, true, e.Qty, false
			b.maxOrderID = max(b.maxOrderID, e.OrderID)

		case EvRejected:
			if e.Reject != RejectUnknownOrder { // cancels carry the target id, places their own
				b.maxOrderID = max(b.maxOrderID, e.OrderID)
			}

		case EvTrade:
			if !inbound || e.OrderID != pending.OrderID {
				return &ReplayError{e.Seq, "trade without its accepted taker"}
			}
			m, ok := b.index[e.MakerOrderID]
			if !ok || b.slab[m].qty < e.Qty || e.Qty <= 0 || e.Qty > leaves {
				return &ReplayError{e.Seq, fmt.Sprintf("invalid trade against maker %d", e.MakerOrderID)}
			}
			if b.slab[m].qty == e.Qty {
				b.unlink(m)
			} else {
				b.reduce(m, e.Qty)
			}
			leaves -= e.Qty

		case EvCanceled:
			if inbound && e.OrderID == pending.OrderID {
				if e.Qty != leaves {
					return &ReplayError{e.Seq, "cancelled quantity differs from the incoming order's remainder"}
				}
				leaves, finished = 0, true
				continue
			}
			m, ok := b.index[e.OrderID]
			if !ok || b.slab[m].qty != e.Qty {
				return &ReplayError{e.Seq, fmt.Sprintf("cancel of unknown or changed order %d", e.OrderID)}
			}
			b.unlink(m)

		default:
			return &ReplayError{e.Seq, fmt.Sprintf("unknown event kind %d", e.Kind)}
		}
	}
	if inbound && leaves > 0 && !finished {
		if pending.Price <= 0 || (pending.Side != Buy && pending.Side != Sell) {
			return &ReplayError{pending.Seq, "incoming order with a remainder cannot rest"}
		}
		if _, dup := b.index[pending.OrderID]; dup {
			return &ReplayError{pending.Seq, "resting order id already in the book"}
		}
		b.restOrder(pending.Side, pending.Price, pending.OrderID, pending.AccountID, leaves)
	}
	return nil
}

// reduce lowers the quantity of resting order i by q without removing it.
func (b *Book) reduce(i int32, q int64) {
	o := &b.slab[i]
	levels, desc := &b.bids, false
	if o.side == Sell {
		levels, desc = &b.asks, true
	}
	pos, found := findLevel(*levels, o.price, desc)
	if !found {
		panic("engine: resting order without a price level")
	}
	(*levels)[pos].qty -= q
	o.qty -= q
}

// Orders calls fn for every resting order in priority order: asks from best
// to worst, then bids from best to worst, FIFO within a price level. This is
// the order Hash uses and the order a snapshot must restore.
func (b *Book) Orders(fn func(side Side, price int64, id, account uint64, qty int64)) {
	for i := len(b.asks) - 1; i >= 0; i-- {
		b.eachInLevel(Sell, &b.asks[i], fn)
	}
	for i := len(b.bids) - 1; i >= 0; i-- {
		b.eachInLevel(Buy, &b.bids[i], fn)
	}
}

func (b *Book) eachInLevel(side Side, lv *level, fn func(Side, int64, uint64, uint64, int64)) {
	for i := lv.head; i != nilIdx; i = b.slab[i].next {
		o := &b.slab[i]
		fn(side, lv.price, o.id, o.account, o.qty)
	}
}

// RestoreOrder appends a resting order at the tail of its price level. Call
// it in snapshot (priority) order on an empty Book, then SetState.
func (b *Book) RestoreOrder(side Side, price int64, id, account uint64, qty int64) error {
	switch {
	case side != Buy && side != Sell:
		return fmt.Errorf("engine: restore order %d: invalid side", id)
	case price <= 0 || qty <= 0:
		return fmt.Errorf("engine: restore order %d: invalid price or quantity", id)
	}
	if _, dup := b.index[id]; dup {
		return fmt.Errorf("engine: restore order %d: duplicate id", id)
	}
	b.restOrder(side, price, id, account, qty)
	return nil
}

// SetState sets the sequence counter and the highest order id seen. Used
// after RestoreOrder when loading a snapshot.
func (b *Book) SetState(seq, maxOrderID uint64) {
	b.seq, b.maxOrderID = seq, maxOrderID
}

// MaxOrderID returns the highest order id seen by ApplyEvents or SetState.
// The next order id to assign is MaxOrderID()+1.
func (b *Book) MaxOrderID() uint64 { return b.maxOrderID }

// Clone returns an independent copy of the book (same orders, same priority,
// same counters) sized by cfg. It is O(resting orders) and not for the hot path.
func (b *Book) Clone(cfg Config) *Book {
	cfg.MaxOrders = max(cfg.MaxOrders, b.Len())
	n := NewBook(cfg)
	b.Orders(func(side Side, price int64, id, account uint64, qty int64) {
		n.restOrder(side, price, id, account, qty)
	})
	n.SetState(b.seq, b.maxOrderID)
	return n
}

// Depth calls fn for the n best price levels of each side with the aggregate
// resting quantity: asks from best to worst, then bids from best to worst. It
// only reads the book; the caller must own the Book (it is not safe to call
// while another goroutine applies commands or events).
func (b *Book) Depth(n int, fn func(side Side, price, qty int64)) {
	for i, c := len(b.asks)-1, 0; i >= 0 && c < n; i, c = i-1, c+1 {
		fn(Sell, b.asks[i].price, b.asks[i].qty)
	}
	for i, c := len(b.bids)-1, 0; i >= 0 && c < n; i, c = i-1, c+1 {
		fn(Buy, b.bids[i].price, b.bids[i].qty)
	}
}
