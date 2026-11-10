package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func TestIDTableBasics(t *testing.T) {
	tb := newIDTable(4)
	_, ok := tb.get(7)
	require.False(t, ok)
	tb.put(7, 1)
	tb.put(0, 9) // key 0 is legal for the book
	v, ok := tb.get(7)
	require.True(t, ok)
	require.EqualValues(t, 1, v)
	v, ok = tb.get(0)
	require.True(t, ok)
	require.EqualValues(t, 9, v)
	require.Equal(t, 2, tb.len())
	tb.put(7, 5) // overwrite
	v, _ = tb.get(7)
	require.EqualValues(t, 5, v)
	tb.del(7)
	tb.del(0)
	tb.del(12345) // absent: no-op
	require.Zero(t, tb.len())
	_, ok = tb.get(7)
	require.False(t, ok)
}

// The table behaves exactly like a map under any sequence of operations,
// including collisions (small table), deletions in the middle of probe runs
// and growth.
func TestIDTableMatchesAMapProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		tb := newIDTable(rapid.IntRange(1, 16).Draw(t, "hint"))
		ref := map[uint64]int32{}
		keys := rapid.SliceOfN(rapid.Uint64Range(0, 64), 1, 40).Draw(t, "keyspace")
		for range rapid.IntRange(0, 400).Draw(t, "ops") {
			k := rapid.SampledFrom(keys).Draw(t, "key")
			switch rapid.IntRange(0, 2).Draw(t, "op") {
			case 0:
				v := rapid.Int32Range(0, 1000).Draw(t, "val")
				tb.put(k, v)
				ref[k] = v
			case 1:
				tb.del(k)
				delete(ref, k)
			default:
				v, ok := tb.get(k)
				rv, rok := ref[k]
				require.Equal(t, rok, ok)
				require.Equal(t, rv, v)
			}
			require.Equal(t, len(ref), tb.len())
		}
		for k, rv := range ref {
			v, ok := tb.get(k)
			require.True(t, ok)
			require.Equal(t, rv, v)
		}
	})
}

func TestIDTableChurnDoesNotAllocateOrDegrade(t *testing.T) {
	tb := newIDTable(1000)
	next := uint64(1)
	for i := 0; i < 500; i++ {
		tb.put(next, int32(i))
		next++
	}
	allocs := testing.AllocsPerRun(100, func() {
		for i := 0; i < 5000; i++ { // 500 live ids, sliding window, far more churn than capacity
			tb.put(next, 1)
			tb.del(next - 500)
			next++
		}
	})
	require.Zero(t, allocs)
	require.Equal(t, 500, tb.len())
	for id := next - 500; id < next; id++ {
		_, ok := tb.get(id)
		require.True(t, ok)
	}
}
