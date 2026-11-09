package eventlog_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/itsdamian/mini-exchange/internal/engine"
	"github.com/itsdamian/mini-exchange/internal/enginetest"
	"github.com/itsdamian/mini-exchange/internal/eventlog"

	exchangev1 "github.com/itsdamian/mini-exchange/gen/exchange/v1"
)

var bookCfg = engine.Config{MaxOrders: 4096, MaxLevels: 256}

// history runs a generated flow and returns the events of each command plus
// the resulting live book.
func history(t testing.TB, commands int, seed uint64) ([][]engine.Event, *engine.Book) {
	t.Helper()
	cmds := enginetest.Generate(commands, seed, enginetest.DefaultProfile())
	book := engine.NewBook(bookCfg)
	batches := make([][]engine.Event, 0, len(cmds))
	for i := range cmds {
		c := cmds[i]
		c.Ts = int64(i + 1)
		batches = append(batches, book.Apply(&c, nil))
	}
	return batches, book
}

func TestCodecRoundTrip(t *testing.T) {
	batches, _ := history(t, 5000, 1)
	for _, evs := range batches {
		pb := eventlog.ToProtoBatch("BTC-USD", evs)
		raw, err := proto.Marshal(pb)
		require.NoError(t, err)
		var back exchangev1.EventBatch
		require.NoError(t, proto.Unmarshal(raw, &back))
		got, err := eventlog.FromProtoBatch(&back, "BTC-USD")
		require.NoError(t, err)
		require.Equal(t, evs, got)
	}
}

func TestCodecRejectsBadBatches(t *testing.T) {
	evs := []engine.Event{{Kind: engine.EvAccepted, Seq: 1, OrderID: 1, AccountID: 1, Side: engine.Buy, Price: 1, Qty: 1}}
	good := func() *exchangev1.EventBatch { return eventlog.ToProtoBatch("S", evs) }

	b := good()
	b.SchemaVersion = 99
	_, err := eventlog.FromProtoBatch(b, "S")
	require.ErrorContains(t, err, "schema version")

	_, err = eventlog.FromProtoBatch(good(), "OTHER")
	require.ErrorContains(t, err, "symbol")

	_, err = eventlog.FromProtoBatch(&exchangev1.EventBatch{SchemaVersion: eventlog.SchemaVersion, Symbol: "S"}, "S")
	require.ErrorContains(t, err, "empty")

	b = eventlog.ToProtoBatch("S", []engine.Event{
		{Kind: engine.EvAccepted, Seq: 1, OrderID: 1, Side: engine.Buy, Price: 1, Qty: 1},
		{Kind: engine.EvCanceled, Seq: 3, OrderID: 1, Side: engine.Buy, Qty: 1},
	})
	_, err = eventlog.FromProtoBatch(b, "S")
	require.ErrorContains(t, err, "non-contiguous")

	b = good()
	b.Events[0].Type = exchangev1.EventType_EVENT_TYPE_UNSPECIFIED
	_, err = eventlog.FromProtoBatch(b, "S")
	require.Error(t, err)
}

func TestSnapshotRoundTripAndHashCheck(t *testing.T) {
	_, book := history(t, 30_000, 2)
	require.NotZero(t, book.Len())
	store := &eventlog.SnapshotStore{Dir: t.TempDir(), Symbol: "S"}
	require.NoError(t, store.Write(eventlog.BuildSnapshot("S", book, 1234)))

	snap, restored, skipped := store.Load(bookCfg)
	require.Empty(t, skipped)
	require.NotNil(t, snap)
	require.Equal(t, int64(1234), snap.GetLogOffset())
	require.Equal(t, book.Seq(), snap.GetLastSequence())
	require.Equal(t, book.Hash(), restored.Hash())
	require.Equal(t, book.MaxOrderID(), restored.MaxOrderID())

	// a tampered snapshot is rejected, not loaded
	bad := eventlog.BuildSnapshot("S", book, 1)
	bad.Orders[0].Quantity++
	_, err := eventlog.RestoreBook(bad, "S", bookCfg)
	require.ErrorContains(t, err, "hash mismatch")
	_, err = eventlog.RestoreBook(eventlog.BuildSnapshot("S", book, 1), "OTHER", bookCfg)
	require.ErrorContains(t, err, "symbol")
}

func TestSnapshotStoreFallsBackToOlderOnCorruption(t *testing.T) {
	dir := t.TempDir()
	store := &eventlog.SnapshotStore{Dir: dir, Symbol: "S", Keep: 3}

	batches, _ := history(t, 4000, 3)
	b := engine.NewBook(bookCfg)
	var older uint64
	for i, evs := range batches {
		require.NoError(t, b.ApplyEvents(evs))
		if i == 2000 || i == 3999 {
			require.NoError(t, store.Write(eventlog.BuildSnapshot("S", b, int64(i))))
			if i == 2000 {
				older = b.Seq()
			}
		}
	}
	files, _ := filepath.Glob(filepath.Join(dir, "snapshot-S-*.pb"))
	require.Len(t, files, 2)
	require.NoError(t, os.WriteFile(files[1], []byte("garbage"), 0o644)) // newest (zero-padded names sort by seq)

	snap, _, skipped := store.Load(bookCfg)
	require.Len(t, skipped, 1)
	require.Equal(t, older, snap.GetLastSequence(), "falls back to the previous snapshot")
}

func TestSnapshotStorePrunesAndIgnoresEmptyDir(t *testing.T) {
	store := &eventlog.SnapshotStore{Dir: filepath.Join(t.TempDir(), "missing"), Symbol: "S", Keep: 2}
	snap, book, skipped := store.Load(bookCfg)
	require.Nil(t, snap)
	require.Nil(t, book)
	require.Empty(t, skipped)

	b := engine.NewBook(bookCfg)
	batches, _ := history(t, 3000, 4)
	for i, evs := range batches {
		require.NoError(t, b.ApplyEvents(evs))
		if i%500 == 499 {
			require.NoError(t, store.Write(eventlog.BuildSnapshot("S", b, int64(i))))
		}
	}
	files, _ := filepath.Glob(filepath.Join(store.Dir, "snapshot-S-*.pb"))
	require.Len(t, files, 2)
}

func TestFollowerAppliesOutOfOrderAcksInOrder(t *testing.T) {
	batches, live := history(t, 3000, 5)
	f := eventlog.NewFollower("S", engine.NewBook(bookCfg), -1, nil, 0, nil)

	// deliver in a scrambled order: odd positions first, then even
	for i := 1; i < len(batches); i += 2 {
		f.OnDurable(batches[i], int64(i))
	}
	seq, _ := f.Durable()
	require.Zero(t, seq, "nothing applies while the first batch is missing")
	for i := 0; i < len(batches); i += 2 {
		f.OnDurable(batches[i], int64(i))
	}
	require.NoError(t, f.Err())
	seq, off := f.Durable()
	require.Equal(t, live.Seq(), seq)
	require.Equal(t, int64(len(batches)-1), off)
	require.Equal(t, live.Hash(), f.BookHash())
}

func TestFollowerStopsOnInconsistentBatch(t *testing.T) {
	f := eventlog.NewFollower("S", engine.NewBook(bookCfg), -1, nil, 0, nil)
	f.OnDurable([]engine.Event{{Kind: engine.EvTrade, Seq: 1, OrderID: 1, MakerOrderID: 2, Qty: 1}}, 0)
	require.Error(t, f.Err())
	before, _ := f.Durable()
	f.OnDurable([]engine.Event{{Kind: engine.EvAccepted, Seq: 1, OrderID: 1, Side: engine.Buy, Price: 1, Qty: 1}}, 1)
	after, _ := f.Durable()
	require.Equal(t, before, after, "a failed follower applies nothing further")
}

func TestFollowerWritesPeriodicAndFinalSnapshots(t *testing.T) {
	batches, live := history(t, 6000, 6)
	store := &eventlog.SnapshotStore{Dir: t.TempDir(), Symbol: "S"}
	f := eventlog.NewFollower("S", engine.NewBook(bookCfg), -1, store, 1000, nil)
	for i, evs := range batches {
		f.OnDurable(evs, int64(i))
	}
	f.Close() // final snapshot

	snap, restored, skipped := store.Load(bookCfg)
	require.Empty(t, skipped)
	require.Equal(t, live.Seq(), snap.GetLastSequence(), "graceful close snapshots everything: nothing to replay")
	require.Equal(t, int64(len(batches)-1), snap.GetLogOffset())
	require.Equal(t, live.Hash(), restored.Hash())
}
