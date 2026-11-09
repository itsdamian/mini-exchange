package ring_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/itsdamian/mini-exchange/internal/ring"
)

func TestSPSCFullEmptyAndOrder(t *testing.T) {
	q := ring.NewSPSC[int](4)
	_, ok := q.Pop()
	require.False(t, ok, "empty")
	for i := 1; i <= 4; i++ {
		require.True(t, q.Push(i))
	}
	require.False(t, q.Push(5), "full")
	require.Equal(t, 4, q.Len())
	for i := 1; i <= 4; i++ {
		v, ok := q.Pop()
		require.True(t, ok)
		require.Equal(t, i, v)
	}
	// wrap around many times
	for i := 0; i < 100; i++ {
		require.True(t, q.Push(i))
		v, ok := q.Pop()
		require.True(t, ok)
		require.Equal(t, i, v)
	}
}

func TestSPSCConcurrentKeepsOrder(t *testing.T) {
	const n = 200_000
	q := ring.NewSPSC[int](1024)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			for !q.Push(i) {
			}
		}
	}()
	for want := 0; want < n; {
		if v, ok := q.Pop(); ok {
			require.Equal(t, want, v)
			want++
		}
	}
	wg.Wait()
}

func TestMPSCFullEmpty(t *testing.T) {
	q := ring.NewMPSC[int](4)
	_, ok := q.Pop()
	require.False(t, ok)
	for i := 1; i <= 4; i++ {
		require.True(t, q.Push(i))
	}
	require.False(t, q.Push(5), "full")
	v, ok := q.Pop()
	require.True(t, ok)
	require.Equal(t, 1, v)
	require.True(t, q.Push(5), "a slot was freed")
}

func TestMPSCManyProducersNoLossNoDuplicates(t *testing.T) {
	const producers, perProducer = 8, 50_000
	q := ring.NewMPSC[uint64](256)
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				v := uint64(p)<<32 | uint64(i)
				for !q.Push(v) {
				}
			}
		}()
	}
	next := make([]uint64, producers) // per-producer FIFO must hold
	for got := 0; got < producers*perProducer; {
		v, ok := q.Pop()
		if !ok {
			continue
		}
		p, i := int(v>>32), v&0xffffffff
		require.Equal(t, next[p], i, "producer %d out of order", p)
		next[p]++
		got++
	}
	wg.Wait()
	_, ok := q.Pop()
	require.False(t, ok)
}

func TestSizeMustBePowerOfTwo(t *testing.T) {
	require.Panics(t, func() { ring.NewSPSC[int](3) })
	require.Panics(t, func() { ring.NewMPSC[int](1) })
}
