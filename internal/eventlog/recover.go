package eventlog

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/itsdamian/mini-exchange/internal/engine"

	exchangev1 "github.com/itsdamian/mini-exchange/gen/exchange/v1"
)

// RecoverStats describes one recovery run.
type RecoverStats struct {
	FromSnapshot   bool
	SnapshotSeq    uint64
	SnapshotOffset int64
	SkippedSnaps   []error
	Batches        int
	Events         int
	LastSeq        uint64
	LastOffset     int64 // offset of the last applied record, -1 if none
	LogStart       int64
	LogEnd         int64 // one past the last record at the time recovery started
	SnapshotLoad   time.Duration
	Replay         time.Duration
	Total          time.Duration
	BookHash       uint64
}

// Recover rebuilds engine state: the newest valid snapshot (if store is not
// nil and holds one) plus a replay of every log record after it, up to the
// log end observed when recovery starts. With store == nil it is a clean
// replay from the start of the log, which is what the recovery check compares
// against.
//
// It refuses to continue (returns an error) rather than guess if the log is
// shorter than the snapshot, was truncated before the snapshot, or contains a
// gap, a duplicate or an event that does not apply.
func Recover(ctx context.Context, brokers []string, symbol string, store *SnapshotStore, cfg engine.Config, log *slog.Logger) (*engine.Book, *RecoverStats, error) {
	if log == nil {
		log = slog.Default()
	}
	t0 := time.Now()
	st := &RecoverStats{LastOffset: -1}

	var book *engine.Book
	var snap *exchangev1.Snapshot
	if store != nil {
		snap, book, st.SkippedSnaps = store.Load(cfg)
		for _, err := range st.SkippedSnaps {
			log.Warn("skipping unusable snapshot", "err", err)
		}
	}
	if book == nil {
		book = engine.NewBook(cfg)
	} else {
		st.FromSnapshot, st.SnapshotSeq, st.SnapshotOffset = true, snap.GetLastSequence(), snap.GetLogOffset()
		st.LastOffset = snap.GetLogOffset()
	}
	st.SnapshotLoad = time.Since(t0)

	topic := Topic(symbol)
	adm, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, nil, err
	}
	starts, err := kadm.NewClient(adm).ListStartOffsets(ctx, topic)
	if err == nil {
		err = starts.Error()
	}
	var ends kadm.ListedOffsets
	if err == nil {
		ends, err = kadm.NewClient(adm).ListEndOffsets(ctx, topic)
		if err == nil {
			err = ends.Error()
		}
	}
	adm.Close()
	if err != nil {
		if errors.Is(err, kerr.UnknownTopicOrPartition) {
			return finish(book, st, t0) // no log yet
		}
		return nil, nil, fmt.Errorf("list offsets for %s: %w", topic, err)
	}
	so, _ := starts.Lookup(topic, 0)
	eo, _ := ends.Lookup(topic, 0)
	st.LogStart, st.LogEnd = so.Offset, eo.Offset

	from := st.LogStart
	if st.FromSnapshot {
		from = st.SnapshotOffset + 1
		if from > st.LogEnd {
			return nil, nil, fmt.Errorf("snapshot (seq %d, offset %d) is ahead of the log (end offset %d): refusing to start from a state the log does not hold",
				st.SnapshotSeq, st.SnapshotOffset, st.LogEnd)
		}
	}
	if from < st.LogStart {
		return nil, nil, fmt.Errorf("log was truncated: it starts at offset %d but recovery needs offset %d", st.LogStart, from)
	}
	if from < st.LogEnd {
		if err := replayRange(ctx, brokers, topic, symbol, book, from, st); err != nil {
			return nil, nil, err
		}
	}
	return finish(book, st, t0)
}

func finish(book *engine.Book, st *RecoverStats, t0 time.Time) (*engine.Book, *RecoverStats, error) {
	st.LastSeq = book.Seq()
	st.BookHash = book.Hash()
	st.Total = time.Since(t0)
	return book, st, nil
}

func replayRange(ctx context.Context, brokers []string, topic, symbol string, book *engine.Book, from int64, st *RecoverStats) error {
	t := time.Now()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().At(from)}}),
		kgo.FetchMaxBytes(64<<20), kgo.FetchMaxPartitionBytes(32<<20),
	)
	if err != nil {
		return err
	}
	defer cl.Close()

	var applyErr error
	for st.LastOffset < st.LogEnd-1 && applyErr == nil {
		fetches := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return err
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("fetch %s: %w", topic, errs[0].Err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if applyErr != nil || r.Offset >= st.LogEnd {
				return
			}
			var b exchangev1.EventBatch
			if err := proto.Unmarshal(r.Value, &b); err != nil {
				applyErr = fmt.Errorf("offset %d: %w", r.Offset, err)
				return
			}
			evs, err := FromProtoBatch(&b, symbol)
			if err != nil {
				applyErr = fmt.Errorf("offset %d: %w", r.Offset, err)
				return
			}
			if err := book.ApplyEvents(evs); err != nil {
				applyErr = fmt.Errorf("offset %d: %w", r.Offset, err)
				return
			}
			st.Batches++
			st.Events += len(evs)
			st.LastOffset = r.Offset
		})
	}
	st.Replay = time.Since(t)
	return applyErr
}

// Scan reads the whole event log of a symbol from its first record up to the
// end observed when Scan starts, calling fn with every record's offset and
// decoded events in log order. It validates each batch (schema version,
// symbol, contiguity inside the batch) and returns an error on a record it
// cannot decode, so callers that must tolerate bad records should not use it.
// A missing topic is an empty log.
func Scan(ctx context.Context, brokers []string, symbol string, fn func(offset int64, evs []engine.Event) error) error {
	topic := Topic(symbol)
	adm, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	ka := kadm.NewClient(adm)
	starts, err := ka.ListStartOffsets(ctx, topic)
	if err == nil {
		err = starts.Error()
	}
	var ends kadm.ListedOffsets
	if err == nil {
		ends, err = ka.ListEndOffsets(ctx, topic)
		if err == nil {
			err = ends.Error()
		}
	}
	adm.Close()
	if err != nil {
		if errors.Is(err, kerr.UnknownTopicOrPartition) {
			return nil
		}
		return fmt.Errorf("list offsets for %s: %w", topic, err)
	}
	so, _ := starts.Lookup(topic, 0)
	eo, _ := ends.Lookup(topic, 0)
	if so.Offset >= eo.Offset {
		return nil
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().At(so.Offset)}}),
		kgo.FetchMaxBytes(64<<20), kgo.FetchMaxPartitionBytes(32<<20),
	)
	if err != nil {
		return err
	}
	defer cl.Close()

	last := so.Offset - 1
	var scanErr error
	for last < eo.Offset-1 && scanErr == nil {
		fetches := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return err
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("fetch %s: %w", topic, errs[0].Err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if scanErr != nil || r.Offset >= eo.Offset {
				return
			}
			var b exchangev1.EventBatch
			if err := proto.Unmarshal(r.Value, &b); err != nil {
				scanErr = fmt.Errorf("offset %d: %w", r.Offset, err)
				return
			}
			evs, err := FromProtoBatch(&b, symbol)
			if err != nil {
				scanErr = fmt.Errorf("offset %d: %w", r.Offset, err)
				return
			}
			if err := fn(r.Offset, evs); err != nil {
				scanErr = err
				return
			}
			last = r.Offset
		})
	}
	return scanErr
}
