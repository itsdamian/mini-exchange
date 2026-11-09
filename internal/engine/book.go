package engine

// Config sizes the preallocated structures of a Book. Exceeding them is
// allowed (the structures grow) but growth allocates, so size them for the
// expected peak to keep the hot path allocation-free.
type Config struct {
	MaxOrders int // peak number of simultaneously resting orders
	MaxLevels int // peak number of price levels per side
}

const nilIdx int32 = -1

// order is a resting order stored in the slab. Orders of one price level form
// an intrusive doubly linked list through prev/next slab indices (FIFO = time
// priority). Free slots form a singly linked list through next.
type order struct {
	id      uint64
	account uint64
	price   int64
	qty     int64 // remaining quantity
	prev    int32
	next    int32
	side    Side
}

// level is one price level: a FIFO of orders plus the aggregate quantity.
type level struct {
	price      int64
	qty        int64
	head, tail int32
}

// Book is the order book of one symbol plus its matcher.
type Book struct {
	slab  []order
	free  int32
	index map[uint64]int32 // order id -> slab index (integer key, preallocated)
	// bids are sorted by ascending price, asks by descending price, so the
	// best level of each side is always the last element and removing it is a
	// slice truncation.
	bids []level
	asks []level
	seq  uint64
}

// NewBook returns an empty Book.
func NewBook(cfg Config) *Book {
	if cfg.MaxOrders < 1 {
		cfg.MaxOrders = 1024
	}
	if cfg.MaxLevels < 1 {
		cfg.MaxLevels = 1024
	}
	b := &Book{
		slab:  make([]order, cfg.MaxOrders),
		index: make(map[uint64]int32, cfg.MaxOrders),
		bids:  make([]level, 0, cfg.MaxLevels),
		asks:  make([]level, 0, cfg.MaxLevels),
	}
	for i := range b.slab {
		b.slab[i].next = int32(i + 1)
	}
	b.slab[len(b.slab)-1].next = nilIdx
	b.free = 0
	return b
}

// Seq returns the sequence number of the last emitted event (0 if none).
func (b *Book) Seq() uint64 { return b.seq }

// Len returns the number of resting orders.
func (b *Book) Len() int { return len(b.index) }

// BestBid returns the best bid price.
func (b *Book) BestBid() (price int64, ok bool) {
	if n := len(b.bids); n > 0 {
		return b.bids[n-1].price, true
	}
	return 0, false
}

// BestAsk returns the best ask price.
func (b *Book) BestAsk() (price int64, ok bool) {
	if n := len(b.asks); n > 0 {
		return b.asks[n-1].price, true
	}
	return 0, false
}

// Resting returns the remaining quantity of a resting order.
func (b *Book) Resting(id uint64) (qty int64, ok bool) {
	i, ok := b.index[id]
	if !ok {
		return 0, false
	}
	return b.slab[i].qty, true
}

// Hash returns a hash of the full book state (every resting order in priority
// order, plus the sequence counter). Equal hashes mean equal books; used for
// determinism and (later) recovery checks. It does not allocate.
func (b *Book) Hash() uint64 {
	h := HashSeed
	h = mix(h, b.seq)
	for i := len(b.asks) - 1; i >= 0; i-- {
		h = b.hashLevel(h, Sell, &b.asks[i])
	}
	for i := len(b.bids) - 1; i >= 0; i-- {
		h = b.hashLevel(h, Buy, &b.bids[i])
	}
	return h
}

func (b *Book) hashLevel(h uint64, side Side, lv *level) uint64 {
	for i := lv.head; i != nilIdx; i = b.slab[i].next {
		o := &b.slab[i]
		h = HashResting(h, side, lv.price, o.id, o.account, o.qty)
	}
	return h
}

func (b *Book) emit(out []Event, e Event) []Event {
	b.seq++
	e.Seq = b.seq
	return append(out, e)
}

// Apply runs one command and appends the resulting events to out, returning
// the extended slice. Pass out[:0] of a reused slice to avoid allocation.
func (b *Book) Apply(c *Command, out []Event) []Event {
	switch c.Kind {
	case CmdPlace:
		return b.place(c, out)
	case CmdCancel:
		return b.cancel(c, out)
	default:
		return b.emit(out, Event{
			Kind: EvRejected, Reject: RejectInvalidOrder, Ts: c.Ts,
			OrderID: c.OrderID, AccountID: c.AccountID,
		})
	}
}

func (b *Book) reject(out []Event, c *Command, r RejectReason) []Event {
	return b.emit(out, Event{
		Kind: EvRejected, Reject: r, Side: c.Side, Ts: c.Ts,
		OrderID: c.OrderID, AccountID: c.AccountID, Price: c.Price, Qty: c.Qty,
	})
}

// validate order: structure (side/type/tif), then price, then quantity.
func (b *Book) validate(c *Command) RejectReason {
	if c.Side != Buy && c.Side != Sell {
		return RejectInvalidOrder
	}
	switch c.Type {
	case Limit:
		if c.TIF != GTC && c.TIF != IOC {
			return RejectInvalidOrder
		}
		if c.Price <= 0 {
			return RejectInvalidPrice
		}
	case Market:
	default:
		return RejectInvalidOrder
	}
	if c.Qty <= 0 {
		return RejectInvalidQuantity
	}
	if _, dup := b.index[c.OrderID]; dup {
		return RejectDuplicateOrderID
	}
	return RejectNone
}

func (b *Book) place(c *Command, out []Event) []Event {
	if r := b.validate(c); r != RejectNone {
		return b.reject(out, c, r)
	}
	price := c.Price
	if c.Type == Market {
		price = 0
	}
	out = b.emit(out, Event{
		Kind: EvAccepted, Side: c.Side, Ts: c.Ts,
		OrderID: c.OrderID, AccountID: c.AccountID, Price: price, Qty: c.Qty,
	})

	rem := c.Qty
	stp := false
	opp := &b.asks
	if c.Side == Sell {
		opp = &b.bids
	}
match:
	for rem > 0 && len(*opp) > 0 {
		lv := &(*opp)[len(*opp)-1]
		if c.Type == Limit {
			if (c.Side == Buy && lv.price > c.Price) || (c.Side == Sell && lv.price < c.Price) {
				break
			}
		}
		for rem > 0 && lv.head != nilIdx {
			m := &b.slab[lv.head]
			if m.account == c.AccountID {
				// Self-trade prevention: cancel the newest order (the taker).
				stp = true
				break match
			}
			q := min(rem, m.qty)
			out = b.emit(out, Event{
				Kind: EvTrade, Side: c.Side, Ts: c.Ts,
				OrderID: c.OrderID, AccountID: c.AccountID,
				MakerOrderID: m.id, MakerAccountID: m.account,
				Price: lv.price, Qty: q,
			})
			rem -= q
			m.qty -= q
			lv.qty -= q
			if m.qty == 0 {
				idx := lv.head
				lv.head = m.next
				if lv.head == nilIdx {
					lv.tail = nilIdx
				} else {
					b.slab[lv.head].prev = nilIdx
				}
				delete(b.index, m.id)
				b.release(idx)
			}
		}
		if lv.head == nilIdx {
			*opp = (*opp)[:len(*opp)-1]
		}
	}

	if rem == 0 {
		return out
	}
	if c.Type == Limit && c.TIF == GTC && !stp {
		b.rest(c, rem)
		return out
	}
	return b.emit(out, Event{
		Kind: EvCanceled, Side: c.Side, Ts: c.Ts,
		OrderID: c.OrderID, AccountID: c.AccountID, Price: price, Qty: rem,
	})
}

func (b *Book) cancel(c *Command, out []Event) []Event {
	i, ok := b.index[c.OrderID]
	if !ok || b.slab[i].account != c.AccountID {
		return b.reject(out, c, RejectUnknownOrder)
	}
	o := &b.slab[i]
	levels := &b.bids
	desc := false
	if o.side == Sell {
		levels, desc = &b.asks, true
	}
	pos, found := findLevel(*levels, o.price, desc)
	if !found { // unreachable: index and levels are kept consistent
		panic("engine: resting order without a price level")
	}
	lv := &(*levels)[pos]
	if o.prev == nilIdx {
		lv.head = o.next
	} else {
		b.slab[o.prev].next = o.next
	}
	if o.next == nilIdx {
		lv.tail = o.prev
	} else {
		b.slab[o.next].prev = o.prev
	}
	lv.qty -= o.qty
	qty, side, price := o.qty, o.side, o.price
	if lv.head == nilIdx {
		copy((*levels)[pos:], (*levels)[pos+1:])
		*levels = (*levels)[:len(*levels)-1]
	}
	delete(b.index, c.OrderID)
	b.release(i)
	return b.emit(out, Event{
		Kind: EvCanceled, Side: side, Ts: c.Ts,
		OrderID: c.OrderID, AccountID: c.AccountID, Price: price, Qty: qty,
	})
}

// rest inserts the remainder of a GTC limit order at the tail of its level.
func (b *Book) rest(c *Command, qty int64) {
	idx := b.alloc()
	b.slab[idx] = order{
		id: c.OrderID, account: c.AccountID, price: c.Price, qty: qty,
		prev: nilIdx, next: nilIdx, side: c.Side,
	}
	b.index[c.OrderID] = idx

	levels := &b.bids
	desc := false
	if c.Side == Sell {
		levels, desc = &b.asks, true
	}
	pos, found := findLevel(*levels, c.Price, desc)
	if !found {
		*levels = append(*levels, level{})
		copy((*levels)[pos+1:], (*levels)[pos:])
		(*levels)[pos] = level{price: c.Price, head: nilIdx, tail: nilIdx}
	}
	lv := &(*levels)[pos]
	if lv.tail == nilIdx {
		lv.head = idx
	} else {
		b.slab[lv.tail].next = idx
		b.slab[idx].prev = lv.tail
	}
	lv.tail = idx
	lv.qty += qty
}

// findLevel returns the position of price in levels, or the position where it
// would be inserted. Bids are ascending, asks (desc=true) are descending.
func findLevel(levels []level, price int64, desc bool) (pos int, found bool) {
	lo, hi := 0, len(levels)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		p := levels[mid].price
		if (!desc && p < price) || (desc && p > price) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo, lo < len(levels) && levels[lo].price == price
}

// alloc takes a slot from the free list, growing the slab if it is exhausted
// (the only allocation path; avoided by sizing Config.MaxOrders).
func (b *Book) alloc() int32 {
	if b.free == nilIdx {
		old := len(b.slab)
		b.slab = append(b.slab, make([]order, old)...)
		for i := old; i < len(b.slab); i++ {
			b.slab[i].next = int32(i + 1)
		}
		b.slab[len(b.slab)-1].next = nilIdx
		b.free = int32(old)
	}
	idx := b.free
	b.free = b.slab[idx].next
	return idx
}

func (b *Book) release(idx int32) {
	b.slab[idx].next = b.free
	b.free = idx
}
