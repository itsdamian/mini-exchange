// Package pipeline connects the gateway to the matching engine.
//
//	handlers --Submit--> MPSC input ring --> engine goroutine (owns the Book)
//	                                             |
//	                                     SPSC output ring
//	                                             |
//	                                  dispatcher goroutine
//	                              (completes requests, fans out reports)
//
// The engine goroutine does nothing but pop a command, stamp it, run
// Book.Apply and push the resulting events to the output ring: no network or
// disk I/O, no mutex, no channel. Everything that needs locks or channels
// (request completion, execution report fan-out) happens on the dispatcher
// goroutine, after the matcher has moved on. Execution reports therefore come
// from the output buffer, never from Kafka.
package pipeline

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/itsdamian/mini-exchange/internal/engine"
	"github.com/itsdamian/mini-exchange/internal/ring"
)

// Errors returned by Submit.
var (
	// ErrOverloaded means the input ring or the in-flight limit is full. The
	// request was NOT enqueued; the gateway sheds load with an explicit error.
	ErrOverloaded = errors.New("pipeline: overloaded")
	// ErrStopped means the pipeline is shutting down.
	ErrStopped = errors.New("pipeline: stopped")
)

// Config configures a Pipeline. Zero values get defaults.
type Config struct {
	Book         engine.Config
	InputSize    int // MPSC ring capacity, power of two (default 65536)
	OutputSize   int // SPSC ring capacity, power of two (default 65536)
	MaxInFlight  int // concurrent Submit calls (default 32768)
	ReportBuffer int // per-subscriber report buffer (default 4096)
	Registerer   prometheus.Registerer
}

// Stats are cumulative counters maintained by the dispatcher.
type Stats struct {
	Accepted, Rejected, Canceled, Trades uint64
}

type inItem struct {
	cmd    engine.Command
	ticket uint32
	enq    int64 // ns since Pipeline.base when pushed
}

type outItem struct {
	ev     engine.Event
	ticket uint32 // non-zero on the first event of a command
}

const (
	slotWaiting uint32 = iota
	slotDelivered
	slotAbandoned
)

type slot struct {
	state  atomic.Uint32
	ch     chan engine.Event // capacity 1
	client string            // client order id, set by Submit
}

type liveOrder struct {
	client    string
	account   uint64
	side      engine.Side
	remaining int64
}

// parker lets a consumer sleep when its queue is empty without losing wakeups.
type parker struct {
	parked atomic.Bool
	wake   chan struct{}
}

func newParker() *parker { return &parker{wake: make(chan struct{}, 1)} }

// notify wakes the consumer if it is (about to be) parked.
func (p *parker) notify() {
	if p.parked.Load() && p.parked.CompareAndSwap(true, false) {
		select {
		case p.wake <- struct{}{}:
		default:
		}
	}
}

const (
	spinIters  = 2000
	yieldIters = 200
)

// Pipeline is the engine goroutine, its rings and the dispatcher.
type Pipeline struct {
	book *engine.Book
	in   *ring.MPSC[inItem]
	out  *ring.SPSC[outItem]

	slots []slot
	free  chan uint32

	inPark, outPark *parker
	hub             *hub

	base     time.Time
	baseUnix int64
	nextID   uint64 // engine goroutine only

	live map[uint64]liveOrder // dispatcher goroutine only

	closed     atomic.Bool
	engineDone atomic.Bool
	wg         sync.WaitGroup

	accepted, rejected, canceled, trades atomic.Uint64

	queueWait  prometheus.Histogram
	matchTime  prometheus.Histogram
	overloaded prometheus.Counter
}

// New creates and starts a Pipeline. Call Stop to shut it down.
func New(cfg Config) *Pipeline {
	if cfg.InputSize == 0 {
		cfg.InputSize = 1 << 16
	}
	if cfg.OutputSize == 0 {
		cfg.OutputSize = 1 << 16
	}
	if cfg.MaxInFlight == 0 {
		cfg.MaxInFlight = 1 << 15
	}
	if cfg.ReportBuffer == 0 {
		cfg.ReportBuffer = 4096
	}
	if cfg.Book.MaxOrders == 0 {
		cfg.Book = engine.Config{MaxOrders: 1 << 17, MaxLevels: 4096}
	}
	reg := cfg.Registerer
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	f := promauto.With(reg)
	buckets := prometheus.ExponentialBuckets(1e-7, 2, 24) // 100 ns .. ~0.8 s

	now := time.Now()
	p := &Pipeline{
		book:     engine.NewBook(cfg.Book),
		in:       ring.NewMPSC[inItem](cfg.InputSize),
		out:      ring.NewSPSC[outItem](cfg.OutputSize),
		slots:    make([]slot, cfg.MaxInFlight+1),
		free:     make(chan uint32, cfg.MaxInFlight),
		inPark:   newParker(),
		outPark:  newParker(),
		hub:      newHub(cfg.ReportBuffer),
		base:     now,
		baseUnix: now.UnixNano(),
		live:     make(map[uint64]liveOrder, cfg.Book.MaxOrders),
		queueWait: f.NewHistogram(prometheus.HistogramOpts{
			Name: "mx_engine_queue_wait_seconds", Help: "Time a command waits in the input ring before the engine pops it.", Buckets: buckets,
		}),
		matchTime: f.NewHistogram(prometheus.HistogramOpts{
			Name: "mx_engine_match_seconds", Help: "Time spent inside Book.Apply per command.", Buckets: buckets,
		}),
		overloaded: f.NewCounter(prometheus.CounterOpts{
			Name: "mx_pipeline_overloaded_total", Help: "Commands refused because the input ring or in-flight limit was full.",
		}),
	}
	for i := 1; i <= cfg.MaxInFlight; i++ {
		p.slots[i].ch = make(chan engine.Event, 1)
		p.free <- uint32(i)
	}
	p.wg.Add(2)
	go p.engineLoop()
	go p.dispatchLoop()
	return p
}

func (p *Pipeline) now() int64 { return int64(time.Since(p.base)) }

// Stats returns the cumulative counters.
func (p *Pipeline) Stats() Stats {
	return Stats{p.accepted.Load(), p.rejected.Load(), p.canceled.Load(), p.trades.Load()}
}

// Subscribe returns a stream of execution reports for account, starting now.
func (p *Pipeline) Subscribe(account uint64) (*Subscription, error) {
	return p.hub.subscribe(account)
}

// Submit sends a command to the engine and waits for its first event
// (Accepted or Rejected for a place, Canceled or Rejected for a cancel).
// For CmdPlace the engine assigns the order id; read it from the event.
// clientOrderID is only used to label execution reports.
//
// Submit never blocks on a full queue: it returns ErrOverloaded instead. If
// ctx ends while waiting, the command may still be executed.
func (p *Pipeline) Submit(ctx context.Context, cmd engine.Command, clientOrderID string) (engine.Event, error) {
	if p.closed.Load() {
		return engine.Event{}, ErrStopped
	}
	var t uint32
	select {
	case t = <-p.free:
	default:
		p.overloaded.Inc()
		return engine.Event{}, ErrOverloaded
	}
	s := &p.slots[t]
	s.client = clientOrderID
	s.state.Store(slotWaiting)
	if !p.in.Push(inItem{cmd: cmd, ticket: t, enq: p.now()}) {
		p.free <- t
		p.overloaded.Inc()
		return engine.Event{}, ErrOverloaded
	}
	p.inPark.notify()

	select {
	case ev := <-s.ch:
		p.free <- t
		return ev, nil
	case <-ctx.Done():
		if s.state.CompareAndSwap(slotWaiting, slotAbandoned) {
			return engine.Event{}, ctx.Err() // the dispatcher releases the ticket
		}
		ev := <-s.ch // already delivered
		p.free <- t
		return ev, nil
	}
}

// Stop refuses new commands, drains what is queued, and stops both goroutines
// and all subscriptions.
func (p *Pipeline) Stop(ctx context.Context) error {
	if p.closed.Swap(true) {
		return nil
	}
	p.inPark.notify()
	done := make(chan struct{})
	go func() { p.wg.Wait(); close(done) }()
	select {
	case <-done:
		p.hub.shutdown()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// nextInput pops the next command, spinning, then yielding, then parking.
// It returns false only when the pipeline is closed and the ring is drained.
func (p *Pipeline) nextInput() (inItem, bool) {
	for idle := 0; ; idle++ {
		if it, ok := p.in.Pop(); ok {
			return it, true
		}
		if p.closed.Load() {
			it, ok := p.in.Pop()
			return it, ok
		}
		switch {
		case idle < spinIters:
		case idle < spinIters+yieldIters:
			runtime.Gosched()
		default:
			p.inPark.parked.Store(true)
			if it, ok := p.in.Pop(); ok {
				p.inPark.parked.Store(false)
				return it, true
			}
			if p.closed.Load() {
				p.inPark.parked.Store(false)
				continue
			}
			<-p.inPark.wake
			idle = 0
		}
	}
}

func (p *Pipeline) engineLoop() {
	defer p.wg.Done()
	defer func() { p.engineDone.Store(true); p.outPark.notify() }()
	evs := make([]engine.Event, 0, 256)
	for {
		it, ok := p.nextInput()
		if !ok {
			return
		}
		start := p.now()
		cmd := it.cmd
		cmd.Ts = p.baseUnix + start // the sequencer stamps the time, not the matcher
		if cmd.Kind == engine.CmdPlace {
			p.nextID++
			cmd.OrderID = p.nextID
		}
		evs = p.book.Apply(&cmd, evs[:0])
		end := p.now()
		p.queueWait.Observe(float64(start-it.enq) / 1e9)
		p.matchTime.Observe(float64(end-start) / 1e9)

		for i := range evs {
			o := outItem{ev: evs[i]}
			if i == 0 {
				o.ticket = it.ticket
			}
			for !p.out.Push(o) { // the dispatcher is draining: never drop events
				p.outPark.notify()
				runtime.Gosched()
			}
		}
		p.outPark.notify()
	}
}

func (p *Pipeline) nextOutput() (outItem, bool) {
	for idle := 0; ; idle++ {
		if it, ok := p.out.Pop(); ok {
			return it, true
		}
		if p.engineDone.Load() {
			it, ok := p.out.Pop()
			return it, ok
		}
		switch {
		case idle < spinIters:
		case idle < spinIters+yieldIters:
			runtime.Gosched()
		default:
			p.outPark.parked.Store(true)
			if it, ok := p.out.Pop(); ok {
				p.outPark.parked.Store(false)
				return it, true
			}
			if p.engineDone.Load() {
				p.outPark.parked.Store(false)
				continue
			}
			<-p.outPark.wake
			idle = 0
		}
	}
}

func (p *Pipeline) dispatchLoop() {
	defer p.wg.Done()
	for {
		it, ok := p.nextOutput()
		if !ok {
			return
		}
		p.handle(&it)
	}
}

func (p *Pipeline) handle(it *outItem) {
	ev := &it.ev
	var client string
	if it.ticket != 0 {
		client = p.slots[it.ticket].client // read before the ticket is released
	}
	switch ev.Kind {
	case engine.EvAccepted:
		p.accepted.Add(1)
		p.live[ev.OrderID] = liveOrder{client: client, account: ev.AccountID, side: ev.Side, remaining: ev.Qty}
		p.hub.publish(&Report{Type: ReportNew, AccountID: ev.AccountID, OrderID: ev.OrderID, ClientOrderID: client,
			Side: ev.Side, Price: ev.Price, Qty: ev.Qty, Leaves: ev.Qty, Seq: ev.Seq, Ts: ev.Ts})
	case engine.EvRejected:
		p.rejected.Add(1)
		p.hub.publish(&Report{Type: ReportRejected, AccountID: ev.AccountID, OrderID: ev.OrderID, ClientOrderID: client,
			Side: ev.Side, Price: ev.Price, Qty: ev.Qty, Reject: ev.Reject, Seq: ev.Seq, Ts: ev.Ts})
	case engine.EvCanceled:
		p.canceled.Add(1)
		o := p.live[ev.OrderID]
		delete(p.live, ev.OrderID)
		p.hub.publish(&Report{Type: ReportCanceled, AccountID: ev.AccountID, OrderID: ev.OrderID, ClientOrderID: o.client,
			Side: ev.Side, Price: ev.Price, Qty: ev.Qty, Seq: ev.Seq, Ts: ev.Ts})
	case engine.EvTrade:
		p.trades.Add(1)
		taker, maker := p.live[ev.OrderID], p.live[ev.MakerOrderID]
		taker.remaining -= ev.Qty
		maker.remaining -= ev.Qty
		p.settle(ev.OrderID, taker)
		p.settle(ev.MakerOrderID, maker)
		p.hub.publish(&Report{Type: ReportTrade, AccountID: ev.AccountID, OrderID: ev.OrderID, ClientOrderID: taker.client,
			Side: ev.Side, Price: ev.Price, Qty: ev.Qty, Leaves: taker.remaining, Seq: ev.Seq, Ts: ev.Ts})
		p.hub.publish(&Report{Type: ReportTrade, AccountID: ev.MakerAccountID, OrderID: ev.MakerOrderID, ClientOrderID: maker.client,
			Side: opposite(ev.Side), Price: ev.Price, Qty: ev.Qty, Leaves: maker.remaining, IsMaker: true, Seq: ev.Seq, Ts: ev.Ts})
	}
	if it.ticket != 0 {
		p.deliver(it.ticket, *ev)
	}
}

func (p *Pipeline) settle(id uint64, o liveOrder) {
	if o.remaining <= 0 {
		delete(p.live, id)
	} else {
		p.live[id] = o
	}
}

func opposite(s engine.Side) engine.Side {
	if s == engine.Buy {
		return engine.Sell
	}
	return engine.Buy
}

func (p *Pipeline) deliver(t uint32, ev engine.Event) {
	s := &p.slots[t]
	if s.state.CompareAndSwap(slotWaiting, slotDelivered) {
		s.ch <- ev // capacity 1 and empty: never blocks
		return
	}
	p.free <- t // the waiter gave up: release the ticket here
}
