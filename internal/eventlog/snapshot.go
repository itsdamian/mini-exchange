package eventlog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/itsdamian/mini-exchange/internal/engine"

	exchangev1 "github.com/itsdamian/mini-exchange/gen/exchange/v1"
)

// SnapshotStore keeps book snapshots as files in a directory. Writes are
// atomic (temp file, fsync, rename, directory fsync); the newest valid
// snapshot wins on load and older ones are the fallback if it is corrupt.
type SnapshotStore struct {
	Dir    string
	Symbol string
	Keep   int // snapshots to retain (default 3)
}

func (s *SnapshotStore) prefix() string { return "snapshot-" + s.Symbol + "-" }

func (s *SnapshotStore) path(seq uint64) string {
	return filepath.Join(s.Dir, fmt.Sprintf("%s%020d.pb", s.prefix(), seq))
}

// BuildSnapshot captures the book. offset is the Kafka offset of the record
// that holds the book's last event.
func BuildSnapshot(symbol string, b *engine.Book, offset int64) *exchangev1.Snapshot {
	snap := &exchangev1.Snapshot{
		SchemaVersion: SchemaVersion, Symbol: symbol, LastSequence: b.Seq(), LogOffset: offset,
		MaxOrderId: b.MaxOrderID(), BookHash: b.Hash(), CreatedUnixNs: time.Now().UnixNano(),
		Orders: make([]*exchangev1.SnapshotOrder, 0, b.Len()),
	}
	b.Orders(func(side engine.Side, price int64, id, account uint64, qty int64) {
		snap.Orders = append(snap.Orders, &exchangev1.SnapshotOrder{
			OrderId: id, AccountId: account, Side: toProtoSide(side), Price: price, Quantity: qty,
		})
	})
	return snap
}

// Write stores snap durably and prunes old snapshots.
func (s *SnapshotStore) Write(snap *exchangev1.Snapshot) error {
	if err := os.MkdirAll(s.Dir, 0o755); err != nil {
		return err
	}
	data, err := proto.Marshal(snap)
	if err != nil {
		return err
	}
	final := s.path(snap.GetLastSequence())
	tmp, err := os.CreateTemp(s.Dir, ".snapshot-tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name()) // no-op after a successful rename
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), final); err != nil {
		return err
	}
	if d, err := os.Open(s.Dir); err == nil { // make the rename itself durable
		_ = d.Sync()
		_ = d.Close()
	}
	return s.prune()
}

func (s *SnapshotStore) files() ([]string, error) {
	entries, err := os.ReadDir(s.Dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), s.prefix()) && strings.HasSuffix(e.Name(), ".pb") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names) // zero-padded sequence: lexical == numeric
	return names, nil
}

func (s *SnapshotStore) prune() error {
	keep := s.Keep
	if keep <= 0 {
		keep = 3
	}
	names, err := s.files()
	if err != nil || len(names) <= keep {
		return err
	}
	for _, n := range names[:len(names)-keep] {
		_ = os.Remove(filepath.Join(s.Dir, n))
	}
	return nil
}

// Load returns the newest snapshot that parses, matches this symbol and
// schema, and restores to a book with the recorded hash. Invalid newer
// snapshots are skipped and reported in skipped. (nil, nil, nil) means there
// is no usable snapshot.
func (s *SnapshotStore) Load(cfg engine.Config) (snap *exchangev1.Snapshot, book *engine.Book, skipped []error) {
	names, err := s.files()
	if err != nil {
		return nil, nil, []error{err}
	}
	for i := len(names) - 1; i >= 0; i-- {
		path := filepath.Join(s.Dir, names[i])
		data, err := os.ReadFile(path)
		if err != nil {
			skipped = append(skipped, err)
			continue
		}
		var sn exchangev1.Snapshot
		if err := proto.Unmarshal(data, &sn); err != nil {
			skipped = append(skipped, fmt.Errorf("%s: %w", names[i], err))
			continue
		}
		b, err := RestoreBook(&sn, s.Symbol, cfg)
		if err != nil {
			skipped = append(skipped, fmt.Errorf("%s: %w", names[i], err))
			continue
		}
		return &sn, b, skipped
	}
	return nil, nil, skipped
}

// RestoreBook rebuilds a Book from a snapshot and verifies its hash.
func RestoreBook(snap *exchangev1.Snapshot, symbol string, cfg engine.Config) (*engine.Book, error) {
	if snap.GetSchemaVersion() != SchemaVersion {
		return nil, fmt.Errorf("unsupported snapshot schema version %d", snap.GetSchemaVersion())
	}
	if snap.GetSymbol() != symbol {
		return nil, fmt.Errorf("snapshot of symbol %q, expected %q", snap.GetSymbol(), symbol)
	}
	cfg.MaxOrders = max(cfg.MaxOrders, len(snap.GetOrders())+1)
	b := engine.NewBook(cfg)
	for _, o := range snap.GetOrders() {
		if err := b.RestoreOrder(fromProtoSide(o.GetSide()), o.GetPrice(), o.GetOrderId(), o.GetAccountId(), o.GetQuantity()); err != nil {
			return nil, err
		}
	}
	b.SetState(snap.GetLastSequence(), snap.GetMaxOrderId())
	if got := b.Hash(); got != snap.GetBookHash() {
		return nil, fmt.Errorf("snapshot hash mismatch: restored %016x, recorded %016x", got, snap.GetBookHash())
	}
	return b, nil
}
