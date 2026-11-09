package gateway

import "sync"

// key scopes an idempotency key to an account.
type key struct {
	account uint64
	id      string
}

// call is one in-flight or completed request. done is closed when val/err
// are set; waiters block on it.
type call[V any] struct {
	done chan struct{}
	val  V
	err  error
}

// onceStore runs at most one execution per key and replays its outcome to
// every duplicate, including duplicates that arrive while the first is still
// in flight (single-flight semantics).
//
// Known limitation (phase 2): entries are kept for the life of the process.
// Bounding them by expiry needs the durable idempotency record planned with
// the event log; see ADR 0006.
type onceStore[V any] struct {
	mu    sync.Mutex
	calls map[key]*call[V]
}

func newOnceStore[V any]() *onceStore[V] {
	return &onceStore[V]{calls: make(map[key]*call[V])}
}

// begin returns the call for k and whether the caller is the first (and so
// must execute and then finish it).
func (s *onceStore[V]) begin(k key) (c *call[V], first bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.calls[k]; ok {
		return c, false
	}
	c = &call[V]{done: make(chan struct{})}
	s.calls[k] = c
	return c, true
}

// finish publishes the outcome to all waiters. A call that failed with a
// transient error (nothing was enqueued) is forgotten so the client can retry.
func (s *onceStore[V]) finish(k key, c *call[V], val V, err error, forget bool) {
	c.val, c.err = val, err
	if forget {
		s.mu.Lock()
		delete(s.calls, k)
		s.mu.Unlock()
	}
	close(c.done)
}

// peek returns a completed, successful call for k.
func (s *onceStore[V]) peek(k key) (V, bool) {
	s.mu.Lock()
	c, ok := s.calls[k]
	s.mu.Unlock()
	var zero V
	if !ok {
		return zero, false
	}
	select {
	case <-c.done:
		if c.err != nil {
			return zero, false
		}
		return c.val, true
	default:
		return zero, false
	}
}
