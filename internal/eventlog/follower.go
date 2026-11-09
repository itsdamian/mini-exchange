package eventlog

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/itsdamian/mini-exchange/internal/engine"
)

// Follower maintains a copy of the book built ONLY from batches Kafka has
// acknowledged, and snapshots that copy. Because it never sees an event the
// log does not hold, a snapshot can never be ahead of the log, which is the
// invariant recovery depends on. It also keeps snapshotting off the matcher:
// the engine goroutine never serialises its own book.
type Follower struct {
	symbol string
	store  *SnapshotStore
	every  uint64 // snapshot after this many events (0 disables periodic snapshots)
	log    *slog.Logger

	mu          sync.Mutex
	book        *engine.Book
	offset      int64
	waiting     map[uint64]ackedBatch // acked out of order, keyed by first sequence
	lastSnapSeq uint64
	err         error
	snapping    bool
	wg          sync.WaitGroup
}

type ackedBatch struct {
	evs    []engine.Event
	offset int64
}

// NewFollower starts from book (which the Follower owns from now on) at the
// given log offset (-1 for an empty log).
func NewFollower(symbol string, book *engine.Book, offset int64, store *SnapshotStore, every uint64, log *slog.Logger) *Follower {
	if log == nil {
		log = slog.Default()
	}
	return &Follower{
		symbol: symbol, store: store, every: every, log: log,
		book: book, offset: offset, waiting: map[uint64]ackedBatch{}, lastSnapSeq: book.Seq(),
	}
}

// OnDurable records one acknowledged batch. Batches may arrive in any order;
// they are applied in sequence order.
func (f *Follower) OnDurable(evs []engine.Event, offset int64) {
	if len(evs) == 0 {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return
	}
	f.waiting[evs[0].Seq] = ackedBatch{evs, offset}
	for {
		b, ok := f.waiting[f.book.Seq()+1]
		if !ok {
			break
		}
		delete(f.waiting, b.evs[0].Seq)
		if err := f.book.ApplyEvents(b.evs); err != nil {
			// The log and the engine disagree: stop, loudly. Recovery from
			// here would restore a wrong state.
			f.err = fmt.Errorf("follower: acknowledged batch does not apply: %w", err)
			f.log.Error("event log inconsistent with engine state", "err", f.err)
			return
		}
		f.offset = b.offset
	}
	if f.every > 0 && f.book.Seq()-f.lastSnapSeq >= f.every && !f.snapping {
		f.startSnapshotLocked()
	}
}

// startSnapshotLocked captures the book under the lock (a memory copy) and
// writes the file outside it.
func (f *Follower) startSnapshotLocked() {
	if f.store == nil || f.book.Seq() == f.lastSnapSeq {
		return
	}
	snap := BuildSnapshot(f.symbol, f.book, f.offset)
	f.lastSnapSeq = snap.GetLastSequence()
	f.snapping = true
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		err := f.store.Write(snap)
		f.mu.Lock()
		f.snapping = false
		f.mu.Unlock()
		if err != nil {
			f.log.Error("snapshot write failed", "seq", snap.GetLastSequence(), "err", err)
			return
		}
		f.log.Info("snapshot written", "seq", snap.GetLastSequence(), "offset", snap.GetLogOffset(), "orders", len(snap.GetOrders()))
	}()
}

// Err returns the first inconsistency found, if any.
func (f *Follower) Err() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.err
}

// Durable returns the last sequence and Kafka offset known to be in the log.
func (f *Follower) Durable() (seq uint64, offset int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.book.Seq(), f.offset
}

// BookHash returns the hash of the durable book (for tests and checks).
func (f *Follower) BookHash() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.book.Hash()
}

// Close waits for a running snapshot and writes a final one so a graceful
// restart has nothing to replay.
func (f *Follower) Close() {
	f.wg.Wait()
	f.mu.Lock()
	if f.err == nil {
		f.startSnapshotLocked()
	}
	f.mu.Unlock()
	f.wg.Wait()
}

// AckedSeqForTest is Durable's sequence, named for tests that wait on it.
func (f *Follower) AckedSeqForTest() uint64 { s, _ := f.Durable(); return s }
