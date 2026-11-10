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
	"github.com/itsdamian/mini-exchange/internal/risk"
)

// Errors returned by Submit.
var (
	// ErrOverloaded means the input ring or the in-flight limit is full. The
	// request was NOT enqueued; the gateway sheds load with an explicit error.
	ErrOverloaded = errors.New("pipeline: overloaded")
	// ErrLogBacklogged means the event log is backed up (Kafka slow or down) or
	// has failed. Orders are refused rather than accepted without a way to log
	// them: nothing is dropped silently. Retry later.
	ErrLogBacklogged = errors.New("pipeline: event log backed up or unavailable")
	// ErrStopped means the pipeline is shutting down.
	ErrStopped = errors.New("pipeline: stopped")
)

// EventLog receives the events of every command, one batch per command, in
// sequence order, from the dispatcher goroutine.
// RiskSink receives one settlement operation per engine event that changes a
// pre-trade risk reservation. Enqueue must not block: it runs on the
// dispatcher goroutine.
type RiskSink interface {
	Enqueue(risk.Op)
}

type EventLog interface {
	// Append takes ownership of evs.
	Append(evs []engine.Event)
	// Backlogged reports that new commands must be refused.
	Backlogged() bool
}

// Restored is engine state recovered from the event log.
type Restored struct {
	Book        *engine.Book
	NextOrderID uint64 // the first order id to assign
}

// Config configures a Pipeline. Zero values get defaults.
type Config struct {
	Book         engine.Config
	Restore      *Restored // start from recovered state instead of an empty book
	Log          EventLog  // nil: events are not persisted
	Risk         RiskSink  // nil: reservations are not settled (risk disabled)
	InputSize    int       // MPSC ring capacity, power of two (default 65536)
	OutputSize   int       // SPSC ring capacity, power of two (default 65536)
	MaxInFlight  int       // concurrent Submit calls (default 32768)
	ReportBuffer int       // per-subscriber report buffer (default 4096)
	Registerer   prometheus.Registerer
	// LockEngineThread pins the engine goroutine to one OS thread
	// (runtime.LockOSThread). Off by default; phase 8 experiment 9 measures it.
	LockEngineThread bool
	// UseChannels replaces the input MPSC ring and the output SPSC ring with buffered
	// channels of the same capacity (and no spinning: the consumers block). Off by default;
	// phase 8 experiment 3 compares the two at the level of the whole gateway.
	UseChannels bool
}

// Stats are cumulative counters maintained by the dispatcher.
type Stats struct {
	Accepted, Rejected, Canceled, Trades uint64
	LastSeq                              uint64 // last event sequence the dispatcher handled
}

type inItem struct {
	cmd    engine.Command
	ticket uint32
	enq    int64 // ns since Pipeline.base when pushed
}

type outItem struct {
	ev     engine.Event
	ticket uint32 // non-zero on the first event of a command
	last   bool   // last event of its command
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
	chIn       chan inItem  // set only with Config.UseChannels
	chOut      chan outItem // set only with Config.UseChannels
	stopIn     chan struct{}
	lockThread bool
	book       *engine.Book
	in         *ring.MPSC[inItem]
	out        *ring.SPSC[outItem]

	slots []slot
	free  chan uint32

	inPark, outPark *parker
	hub             *hub
	log             EventLog
	risk            RiskSink

	base     time.Time
	baseUnix int64
	nextID   uint64 // engine goroutine only

	live  map[uint64]liveOrder // dispatcher goroutine only
	batch []engine.Event       // dispatcher goroutine only: events of the command being assembled

	closed     atomic.Bool
	engineDone atomic.Bool
	wg         sync.WaitGroup

	accepted, rejected, canceled, trades, lastSeq atomic.Uint64

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

	book := engine.NewBook(cfg.Book)
	var nextID uint64
	if cfg.Restore != nil {
		book, nextID = cfg.Restore.Book, cfg.Restore.NextOrderID-1
	}

	now := time.Now()
	p := &Pipeline{
		stopIn:     make(chan struct{}),
		book:       book,
		log:        cfg.Log,
		risk:       cfg.Risk,
		nextID:     nextID,
		lockThread: cfg.LockEngineThread,
		in:         ring.NewMPSC[inItem](cfg.InputSize),
		out:        ring.NewSPSC[outItem](cfg.OutputSize),
		slots:      make([]slot, cfg.MaxInFlight+1),
		free:       make(chan uint32, cfg.MaxInFlight),
		inPark:     newParker(),
		outPark:    newParker(),
		hub:        newHub(cfg.ReportBuffer),
		base:       now,
		baseUnix:   now.UnixNano(),
		live:       make(map[uint64]liveOrder, cfg.Book.MaxOrders),
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
	// Orders already resting after a recovery have no client label, but the
	// dispatcher still needs their remaining quantity to compute leaves.
	book.Orders(func(side engine.Side, _ int64, id, account uint64, qty int64) {
		p.live[id] = liveOrder{account: account, side: side, remaining: qty}
	})
	f.NewGaugeFunc(prometheus.GaugeOpts{Name: "mx_engine_last_sequence", Help: "Last engine event sequence handled by the dispatcher."},
		func() float64 { return float64(p.lastSeq.Load()) })
	p.lastSeq.Store(book.Seq())
	for i := 1; i <= cfg.MaxInFlight; i++ {
		p.slots[i].ch = make(chan engine.Event, 1)
		p.free <- uint32(i)
	}
	p.wg.Add(2)
	if cfg.UseChannels {
		p.chIn, p.chOut = make(chan inItem, cfg.InputSize), make(chan outItem, cfg.OutputSize)
	}
	go p.engineLoop()
	go p.dispatchLoop()
	return p
}

func (p *Pipeline) now() int64 { return int64(time.Since(p.base)) }

// Stats returns the cumulative counters.
func (p *Pipeline) Stats() Stats {
	return Stats{p.accepted.Load(), p.rejected.Load(), p.canceled.Load(), p.trades.Load(), p.lastSeq.Load()}
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
	if p.log != nil && p.log.Backlogged() {
		p.overloaded.Inc()
		return engine.Event{}, ErrLogBacklogged
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
	item := inItem{cmd: cmd, ticket: t, enq: p.now()}
	if p.chIn != nil {
		select {
		case p.chIn <- item:
		default:
			p.free <- t
			p.overloaded.Inc()
			return engine.Event{}, ErrOverloaded
		}
	} else {
		if !p.in.Push(item) {
			p.free <- t
			p.overloaded.Inc()
			return engine.Event{}, ErrOverloaded
		}
		p.inPark.notify()
	}

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
	close(p.stopIn)
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
	if p.chIn != nil {
		select {
		case it := <-p.chIn:
			return it, true
		case <-p.stopIn:
			select {
			case it := <-p.chIn:
				return it, true
			default:
				return inItem{}, false
			}
		}
	}
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
	if p.lockThread {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
	}
	defer func() {
		p.engineDone.Store(true)
		p.outPark.notify()
		if p.chOut != nil {
			close(p.chOut)
		}
	}()
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
			o := outItem{ev: evs[i], last: i == len(evs)-1}
			if i == 0 {
				o.ticket = it.ticket
			}
			if p.chOut != nil {
				p.chOut <- o
				continue
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
	if p.chOut != nil {
		it, ok := <-p.chOut
		return it, ok
	}
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
	p.lastSeq.Store(ev.Seq)
	if p.log != nil {
		if p.batch == nil {
			p.batch = make([]engine.Event, 0, 4)
		}
		p.batch = append(p.batch, *ev)
		if it.last {
			p.log.Append(p.batch)
			p.batch = nil
		}
	}
	var client string
	if it.ticket != 0 {
		client = p.slots[it.ticket].client // read before the ticket is released
	}
	switch ev.Kind {
	case engine.EvAccepted:
		p.accepted.Add(1)
		p.live[ev.OrderID] = liveOrder{client: client, account: ev.AccountID, side: ev.Side, remaining: ev.Qty}
		if p.risk != nil {
			p.risk.Enqueue(risk.Op{Kind: risk.OpBind, Seq: ev.Seq, Account: ev.AccountID, OrderID: ev.OrderID, Client: client})
		}
		p.hub.publish(&Report{Type: ReportNew, AccountID: ev.AccountID, OrderID: ev.OrderID, ClientOrderID: client,
			Side: ev.Side, Price: ev.Price, Qty: ev.Qty, Leaves: ev.Qty, Seq: ev.Seq, Ts: ev.Ts})
	case engine.EvRejected:
		p.rejected.Add(1)
		if p.risk != nil && ev.Reject != engine.RejectUnknownOrder { // UnknownOrder is a rejected cancel: nothing was reserved by it
			p.risk.Enqueue(risk.Op{Kind: risk.OpRelease, Seq: ev.Seq, Account: ev.AccountID, Client: client})
		}
		p.hub.publish(&Report{Type: ReportRejected, AccountID: ev.AccountID, OrderID: ev.OrderID, ClientOrderID: client,
			Side: ev.Side, Price: ev.Price, Qty: ev.Qty, Reject: ev.Reject, Seq: ev.Seq, Ts: ev.Ts})
	case engine.EvCanceled:
		p.canceled.Add(1)
		o := p.live[ev.OrderID]
		delete(p.live, ev.OrderID)
		if p.risk != nil {
			p.risk.Enqueue(risk.Op{Kind: risk.OpCancel, Seq: ev.Seq, Account: ev.AccountID, OrderID: ev.OrderID})
		}
		p.hub.publish(&Report{Type: ReportCanceled, AccountID: ev.AccountID, OrderID: ev.OrderID, ClientOrderID: o.client,
			Side: ev.Side, Price: ev.Price, Qty: ev.Qty, Seq: ev.Seq, Ts: ev.Ts})
	case engine.EvTrade:
		p.trades.Add(1)
		taker, maker := p.live[ev.OrderID], p.live[ev.MakerOrderID]
		if p.risk != nil {
			p.risk.Enqueue(risk.Op{Kind: risk.OpFill, Seq: ev.Seq, Account: ev.AccountID, OrderID: ev.OrderID, Price: ev.Price, Qty: ev.Qty})
			p.risk.Enqueue(risk.Op{Kind: risk.OpFill, Seq: ev.Seq, Account: ev.MakerAccountID, OrderID: ev.MakerOrderID, Price: ev.Price, Qty: ev.Qty})
		}
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
