package ledger

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Store is the PostgreSQL side of the ledger.
type Store struct{ pool *pgxpool.Pool }

// Open connects to PostgreSQL and applies the migrations.
func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	s := &Store{pool: pool}
	if err := s.Migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the connections.
func (s *Store) Close() { s.pool.Close() }

// Pool exposes the connection pool (tests and tools).
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Migrate applies the embedded goose migrations.
func (s *Store) Migrate(ctx context.Context) error {
	db := stdlib.OpenDBFromPool(s.pool)
	defer db.Close()
	return migrate(ctx, db)
}

func migrate(ctx context.Context, db *sql.DB) error {
	fsys, err := migrationsSub()
	if err != nil {
		return err
	}
	p, err := goose.NewProvider(goose.DialectPostgres, db, fsys)
	if err != nil {
		return err
	}
	if _, err := p.Up(ctx); err != nil {
		return fmt.Errorf("ledger: migrate: %w", err)
	}
	return nil
}

// applySQL settles one posting through the apply_posting function (see the
// migration): the settlement row is the idempotency key, so a repeated event
// id returns false and changes nothing.
const applySQL = `SELECT apply_posting($1, $2, $3, $4, $5::text[], $6::text[], $7::bigint[], $8::text[], $9::bigint[])`

// Apply settles postings in ONE transaction: either all of them are applied
// or none. A posting whose event id was settled before is skipped, so
// applying the same postings again changes nothing. It returns how many
// postings were new.
func (s *Store) Apply(ctx context.Context, ps []Posting) (applied int, err error) {
	if len(ps) == 0 {
		return 0, nil
	}
	b := &pgx.Batch{}
	for i := range ps {
		p := &ps[i]
		n := len(p.Entries)
		kinds, aks, owners, assets, deltas := make([]string, n), make([]string, n), make([]int64, n), make([]string, n), make([]int64, n)
		for j, e := range p.Entries {
			if e.Owner > math.MaxInt64 {
				return 0, poison("account id %d is out of range", e.Owner)
			}
			kinds[j], aks[j], owners[j], assets[j], deltas[j] = e.Kind, string(e.Acct), int64(e.Owner), e.Asset, e.Delta
		}
		var seq any
		if p.Kind == "trade" {
			seq = int64(p.Seq)
		}
		b.Queue(applySQL, p.EventID, p.Kind, p.Symbol, seq, kinds, aks, owners, assets, deltas)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	res := tx.SendBatch(ctx, b)
	for range ps {
		var isNew bool
		if err := res.QueryRow().Scan(&isNew); err != nil {
			_ = res.Close()
			return 0, err
		}
		if isNew {
			applied++
		}
	}
	if err := res.Close(); err != nil {
		return 0, err
	}
	return applied, tx.Commit(ctx)
}

// IsPoison reports whether err is an input error that no retry can fix: our
// own PoisonError, or a PostgreSQL integrity (class 23) or data (class 22)
// violation such as a negative customer balance.
func IsPoison(err error) bool {
	var pe *PoisonError
	if errors.As(err, &pe) {
		return true
	}
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		return strings.HasPrefix(pg.Code, "23") || strings.HasPrefix(pg.Code, "22")
	}
	return false
}

// AccountBalance is one row of the accounts table.
type AccountBalance struct {
	Acct   AcctKind
	Owner  uint64
	Asset  string
	Amount int64
}

// Balances returns every account balance, ordered.
func (s *Store) Balances(ctx context.Context) ([]AccountBalance, error) {
	rows, err := s.pool.Query(ctx, `SELECT acct_kind, owner, asset, amount FROM accounts ORDER BY acct_kind, owner, asset`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AccountBalance
	for rows.Next() {
		var a AccountBalance
		var k string
		var o int64
		if err := rows.Scan(&k, &o, &a.Asset, &a.Amount); err != nil {
			return nil, err
		}
		a.Acct, a.Owner = AcctKind(k), uint64(o)
		out = append(out, a)
	}
	return out, rows.Err()
}

// TradeCount returns the number of settled trades of a symbol.
func (s *Store) TradeCount(ctx context.Context, symbol string) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM settlements WHERE kind = 'trade' AND symbol = $1`, symbol).Scan(&n)
	return n, err
}
