package engine_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/itsdamian/mini-exchange/internal/engine"
	"github.com/itsdamian/mini-exchange/internal/enginetest"
)

// feed runs cmds on a live book and replays each command's events into a
// second book; both must be identical after every command.
func feed(t require.TestingT, cmds []engine.Command, live, replayed *engine.Book, checkEvery int) {
	var out []engine.Event
	for i := range cmds {
		out = live.Apply(&cmds[i], out[:0])
		require.NoError(t, replayed.ApplyEvents(out), "op %d (%+v)", i, cmds[i])
		if checkEvery > 0 && i%checkEvery == 0 {
			require.Equal(t, live.Hash(), replayed.Hash(), "state diverged after op %d", i)
		}
	}
	require.Equal(t, live.Hash(), replayed.Hash())
	require.Equal(t, live.Seq(), replayed.Seq())
	require.Equal(t, live.Len(), replayed.Len())
}

func TestReplayMatchesLiveBookOnRandomFlow(t *testing.T) {
	n := 300_000
	if testing.Short() {
		n = 60_000
	}
	cmds := enginetest.Generate(n, 11, enginetest.DefaultProfile())
	live := engine.NewBook(engine.Config{MaxOrders: 4096, MaxLevels: 256})
	replayed := engine.NewBook(engine.Config{MaxOrders: 16, MaxLevels: 2}) // forces growth paths
	feed(t, cmds, live, replayed, 1)
}

func TestPropertyReplayMatchesLiveBook(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		cmds := genCommands(t)
		live := engine.NewBook(engine.Config{MaxOrders: 4, MaxLevels: 2})
		replayed := engine.NewBook(engine.Config{MaxOrders: 4, MaxLevels: 2})
		feed(t, cmds, live, replayed, 1)
	})
}

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	cmds := enginetest.Generate(50_000, 5, enginetest.DefaultProfile())
	live := engine.NewBook(engine.Config{MaxOrders: 4096, MaxLevels: 256})
	var out []engine.Event
	for i := range cmds {
		out = live.Apply(&cmds[i], out[:0])
	}

	// snapshot = ordered orders + counters
	type ord struct {
		side     engine.Side
		price    int64
		id, acct uint64
		qty      int64
	}
	var orders []ord
	live.Orders(func(side engine.Side, price int64, id, account uint64, qty int64) {
		orders = append(orders, ord{side, price, id, account, qty})
	})
	require.Equal(t, live.Len(), len(orders))

	restored := engine.NewBook(engine.Config{MaxOrders: 8, MaxLevels: 2})
	for _, o := range orders {
		require.NoError(t, restored.RestoreOrder(o.side, o.price, o.id, o.acct, o.qty))
	}
	restored.SetState(live.Seq(), 50_000)
	require.Equal(t, live.Hash(), restored.Hash(), "restored book must hash like the original")

	// and keeps matching identically afterwards
	more := enginetest.Generate(20_000, 6, enginetest.DefaultProfile())
	for i := range more {
		more[i].OrderID += 1 << 30 // avoid id clashes with the first flow
		if more[i].Kind == engine.CmdCancel {
			continue // targets orders of the second stream only
		}
		a := live.Apply(&more[i], nil)
		b := restored.Apply(&more[i], nil)
		require.Equal(t, a, b, "op %d", i)
	}
	require.Equal(t, live.Hash(), restored.Hash())
}

func TestApplyEventsRejectsInconsistentStreams(t *testing.T) {
	newBook := func() *engine.Book { return engine.NewBook(engine.Config{MaxOrders: 8, MaxLevels: 4}) }
	acc := func(seq, id uint64, qty int64) engine.Event {
		return engine.Event{Kind: engine.EvAccepted, Seq: seq, OrderID: id, AccountID: 1, Side: engine.Buy, Price: 100, Qty: qty}
	}

	t.Run("gap in sequence", func(t *testing.T) {
		b := newBook()
		require.NoError(t, b.ApplyEvents([]engine.Event{acc(1, 1, 5)}))
		err := b.ApplyEvents([]engine.Event{acc(3, 2, 5)})
		require.ErrorAs(t, err, new(*engine.ReplayError))
	})
	t.Run("duplicate sequence", func(t *testing.T) {
		b := newBook()
		require.NoError(t, b.ApplyEvents([]engine.Event{acc(1, 1, 5)}))
		require.Error(t, b.ApplyEvents([]engine.Event{acc(1, 2, 5)}))
	})
	t.Run("trade against unknown maker", func(t *testing.T) {
		b := newBook()
		err := b.ApplyEvents([]engine.Event{
			acc(1, 1, 5),
			{Kind: engine.EvTrade, Seq: 2, OrderID: 1, AccountID: 1, MakerOrderID: 99, MakerAccountID: 2, Price: 100, Qty: 5},
		})
		require.Error(t, err)
	})
	t.Run("cancel of unknown order", func(t *testing.T) {
		b := newBook()
		err := b.ApplyEvents([]engine.Event{{Kind: engine.EvCanceled, Seq: 1, OrderID: 7, AccountID: 1, Qty: 1}})
		require.Error(t, err)
	})
	t.Run("overfill of the taker", func(t *testing.T) {
		b := newBook()
		require.NoError(t, b.ApplyEvents([]engine.Event{
			{Kind: engine.EvAccepted, Seq: 1, OrderID: 1, AccountID: 2, Side: engine.Sell, Price: 100, Qty: 10},
		}))
		err := b.ApplyEvents([]engine.Event{
			acc(2, 2, 3),
			{Kind: engine.EvTrade, Seq: 3, OrderID: 2, AccountID: 1, MakerOrderID: 1, MakerAccountID: 2, Price: 100, Qty: 5},
		})
		require.Error(t, err)
	})
}
