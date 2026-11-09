package engine_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/itsdamian/mini-exchange/internal/engine"
	"github.com/itsdamian/mini-exchange/internal/enginetest"
	"github.com/itsdamian/mini-exchange/internal/refbook"
)

// TestDifferentialAgainstReference runs one million random operations through
// the optimized engine and the naive reference and requires identical events
// for every operation and identical book state at regular checkpoints.
func TestDifferentialAgainstReference(t *testing.T) {
	n := 1_000_000
	if testing.Short() {
		n = 100_000
	}
	for _, seed := range []uint64{1, 2} {
		cmds := enginetest.Generate(n/2, seed, enginetest.DefaultProfile())
		fast := engine.NewBook(engine.Config{MaxOrders: 2048, MaxLevels: 256})
		ref := refbook.New()
		var fo, ro []engine.Event
		trades := 0
		for i := range cmds {
			fo = fast.Apply(&cmds[i], fo[:0])
			ro = ref.Apply(&cmds[i], ro[:0])
			if len(fo) != len(ro) {
				require.Equal(t, ro, fo, "seed %d op %d: %+v", seed, i, cmds[i])
			}
			for j := range fo {
				if fo[j] != ro[j] {
					require.Equal(t, ro[j], fo[j], "seed %d op %d event %d: %+v", seed, i, j, cmds[i])
				}
				if fo[j].Kind == engine.EvTrade {
					trades++
				}
			}
			if i%10_000 == 0 {
				require.Equal(t, ref.Hash(), fast.Hash(), "seed %d: book state diverged at op %d", seed, i)
			}
		}
		require.Equal(t, ref.Hash(), fast.Hash())
		require.Equal(t, ref.Len(), fast.Len())
		require.Greater(t, trades, len(cmds)/20, "workload must actually trade")
		t.Logf("seed %d: %d ops, %d trades, %d resting at end", seed, len(cmds), trades, fast.Len())
	}
}

func TestGenerateIsDeterministic(t *testing.T) {
	a := enginetest.Generate(5000, 42, enginetest.DefaultProfile())
	b := enginetest.Generate(5000, 42, enginetest.DefaultProfile())
	require.Equal(t, a, b)
	c := enginetest.Generate(5000, 43, enginetest.DefaultProfile())
	require.NotEqual(t, a, c)
}
