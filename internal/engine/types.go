// Package engine implements a deterministic, single-threaded, in-memory
// matching engine for one symbol.
//
// Rules the package follows (see docs/adr/0001, 0003):
//   - prices are int64 ticks and quantities are int64 units; no floating point;
//   - no wall-clock reads and no map iteration inside matching; timestamps are
//     supplied by the caller (the sequencer) on every Command;
//   - Apply performs no I/O, takes no locks, and does not allocate in steady
//     state (the slab, level slices and id index are preallocated).
//
// A Book is NOT safe for concurrent use. One goroutine owns it.
package engine

// Side is the side of an order.
type Side uint8

// Order sides. The zero value is invalid on purpose.
const (
	Buy  Side = 1
	Sell Side = 2
)

// OrderType is the type of an order.
type OrderType uint8

// Order types. The zero value is invalid on purpose.
const (
	Limit  OrderType = 1
	Market OrderType = 2
)

// TimeInForce controls what happens to the unfilled part of a LIMIT order.
// It is ignored for MARKET orders, which never rest.
type TimeInForce uint8

// Time in force values. The zero value is invalid on purpose.
const (
	GTC TimeInForce = 1 // good till cancelled: remainder rests on the book
	IOC TimeInForce = 2 // immediate or cancel: remainder is cancelled
)

// CmdKind is the kind of a Command.
type CmdKind uint8

// Command kinds. The zero value is invalid on purpose.
const (
	CmdPlace  CmdKind = 1
	CmdCancel CmdKind = 2
)

// Command is the engine input. It is a plain value with no pointers so it can
// live in a preallocated ring buffer.
//
// For CmdPlace, OrderID is the id of the new order (assigned by the
// sequencer). For CmdCancel, OrderID is the id of the order to cancel and
// AccountID must be its owner; only OrderID, AccountID and Ts are read.
type Command struct {
	Kind      CmdKind
	Side      Side
	Type      OrderType
	TIF       TimeInForce
	OrderID   uint64
	AccountID uint64
	Price     int64 // ticks; must be > 0 for LIMIT, ignored for MARKET
	Qty       int64 // units; must be > 0
	Ts        int64 // assigned by the sequencer, copied onto output events
}

// EventKind is the kind of an Event.
type EventKind uint8

// Event kinds. The zero value is invalid on purpose.
const (
	EvAccepted EventKind = 1
	EvRejected EventKind = 2
	EvCanceled EventKind = 3
	EvTrade    EventKind = 4
)

// RejectReason is the typed reason carried by an EvRejected event.
type RejectReason uint8

// Reject reasons. RejectInsufficientFunds and RejectRateLimited are produced
// by the risk layer (phase 4); they are defined here so the whole system
// shares one enum. The engine itself never emits them.
const (
	RejectNone              RejectReason = 0
	RejectInvalidPrice      RejectReason = 1
	RejectInvalidQuantity   RejectReason = 2
	RejectUnknownOrder      RejectReason = 3
	RejectInsufficientFunds RejectReason = 4
	RejectRateLimited       RejectReason = 5
	RejectInvalidOrder      RejectReason = 6 // malformed side/type/tif/kind
	RejectDuplicateOrderID  RejectReason = 7 // id of a live order reused
)

// Event is the engine output. Like Command it contains no pointers.
//
// Field meaning by kind:
//
//	EvAccepted: OrderID, AccountID, Side, Price (0 for MARKET), Qty = accepted qty.
//	EvRejected: OrderID, AccountID, Reject; Qty/Price echo the command.
//	EvCanceled: OrderID, AccountID, Side, Qty = quantity removed from the book
//	            (or never rested). Emitted for explicit cancels, for the
//	            unfilled remainder of IOC/MARKET orders, and for self-trade
//	            prevention.
//	EvTrade:    OrderID/AccountID = taker, MakerOrderID/MakerAccountID = maker,
//	            Side = taker side, Price = maker (resting) price, Qty = traded qty.
type Event struct {
	Kind           EventKind
	Side           Side
	Reject         RejectReason
	Seq            uint64 // per-book, strictly increasing, starts at 1
	Ts             int64
	OrderID        uint64
	AccountID      uint64
	MakerOrderID   uint64
	MakerAccountID uint64
	Price          int64
	Qty            int64
}

// FNV-1a 64-bit constants for the determinism hash.
const (
	fnvOffset64 uint64 = 14695981039346656037
	fnvPrime64  uint64 = 1099511628211
)

// HashSeed is the initial value for HashEvent / Book.Hash chains.
const HashSeed = fnvOffset64

func mix(h, v uint64) uint64 {
	for range 8 {
		h ^= v & 0xff
		h *= fnvPrime64
		v >>= 8
	}
	return h
}

// HashEvent folds e into the running hash h. Two runs over the same input
// must produce the same final hash (determinism check). It does not allocate.
func HashEvent(h uint64, e *Event) uint64 {
	h = mix(h, uint64(e.Kind)|uint64(e.Side)<<8|uint64(e.Reject)<<16)
	h = mix(h, e.Seq)
	h = mix(h, uint64(e.Ts))
	h = mix(h, e.OrderID)
	h = mix(h, e.AccountID)
	h = mix(h, e.MakerOrderID)
	h = mix(h, e.MakerAccountID)
	h = mix(h, uint64(e.Price))
	h = mix(h, uint64(e.Qty))
	return h
}

// HashEvents folds every event of evs into h.
func HashEvents(h uint64, evs []Event) uint64 {
	for i := range evs {
		h = HashEvent(h, &evs[i])
	}
	return h
}

// HashResting folds one resting order into h; shared by Book and the
// reference implementation so their state hashes are comparable.
func HashResting(h uint64, side Side, price int64, id, account uint64, qty int64) uint64 {
	h = mix(h, uint64(side))
	h = mix(h, uint64(price))
	h = mix(h, id)
	h = mix(h, account)
	h = mix(h, uint64(qty))
	return h
}
