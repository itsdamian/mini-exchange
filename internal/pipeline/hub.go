package pipeline

import (
	"errors"
	"sync"

	"github.com/itsdamian/mini-exchange/internal/engine"
)

// ErrSlowConsumer ends a subscription whose buffer overflowed. Reports are
// never dropped silently: the subscriber is disconnected and must resubscribe.
var ErrSlowConsumer = errors.New("pipeline: subscriber too slow")

// ReportType is the kind of an execution Report.
type ReportType uint8

// Report types.
const (
	ReportNew ReportType = iota + 1
	ReportTrade
	ReportCanceled
	ReportRejected
)

// Report is one execution report for one account, derived from an engine
// event. Trades produce two reports, one per side.
type Report struct {
	Type          ReportType
	AccountID     uint64
	OrderID       uint64
	ClientOrderID string // empty if the order is unknown to this pipeline
	Side          engine.Side
	Price         int64
	Qty           int64
	Leaves        int64
	IsMaker       bool
	Reject        engine.RejectReason
	Seq           uint64
	Ts            int64
}

// Subscription delivers the reports of one account. Receive from C until it
// is closed, then call Err to learn why.
type Subscription struct {
	C <-chan Report

	h       *hub
	account uint64
	ch      chan Report
	err     error // set before ch is closed, under hub.mu
	closed  bool
}

// Err returns the reason the subscription ended (nil for a normal Close).
func (s *Subscription) Err() error {
	s.h.mu.RLock()
	defer s.h.mu.RUnlock()
	return s.err
}

// Close ends the subscription.
func (s *Subscription) Close() { s.h.remove(s, nil) }

type hub struct {
	mu        sync.RWMutex
	byAccount map[uint64][]*Subscription
	bufSize   int
	shut      bool
}

func newHub(bufSize int) *hub {
	return &hub{byAccount: make(map[uint64][]*Subscription), bufSize: bufSize}
}

func (h *hub) subscribe(account uint64) (*Subscription, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.shut {
		return nil, ErrStopped
	}
	ch := make(chan Report, h.bufSize)
	s := &Subscription{C: ch, ch: ch, h: h, account: account}
	h.byAccount[account] = append(h.byAccount[account], s)
	return s, nil
}

// remove detaches s and closes its channel. Sends happen only under RLock and
// close only under Lock, so a send on a closed channel is impossible.
func (h *hub) remove(s *Subscription, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if s.closed {
		return
	}
	s.closed, s.err = true, err
	subs := h.byAccount[s.account]
	for i, x := range subs {
		if x == s {
			subs[i] = subs[len(subs)-1]
			subs = subs[:len(subs)-1]
			break
		}
	}
	if len(subs) == 0 {
		delete(h.byAccount, s.account)
	} else {
		h.byAccount[s.account] = subs
	}
	close(s.ch)
}

// publish never blocks: a subscriber whose buffer is full is disconnected.
func (h *hub) publish(r *Report) {
	h.mu.RLock()
	subs := h.byAccount[r.AccountID]
	var slow []*Subscription
	for _, s := range subs {
		select {
		case s.ch <- *r:
		default:
			slow = append(slow, s)
		}
	}
	h.mu.RUnlock()
	for _, s := range slow {
		h.remove(s, ErrSlowConsumer)
	}
}

func (h *hub) shutdown() {
	h.mu.Lock()
	h.shut = true
	var all []*Subscription
	for _, subs := range h.byAccount {
		all = append(all, subs...)
	}
	h.mu.Unlock()
	for _, s := range all {
		h.remove(s, ErrStopped)
	}
}
