package ledger

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/itsdamian/mini-exchange/internal/engine"
	"github.com/itsdamian/mini-exchange/internal/eventlog"
	"github.com/itsdamian/mini-exchange/internal/risk"
)

// Discrepancy kinds reported by Reconcile.
const (
	DLogGap            = "log_gap"            // the event log's sequence is not contiguous
	DLogInvalidTrade   = "log_invalid_trade"  // a trade in the log cannot be posted
	DMissingSettlement = "missing_settlement" // a trade in the log has no settlement
	DOrphanSettlement  = "orphan_settlement"  // a settlement matches no trade in the log
	DBalanceVsLog      = "balance_vs_log"     // ledger balance differs from the recomputation (log trades + deposits)
	DBalanceVsEntries  = "balance_vs_entries" // balance differs from the sum of its entries
	DUnbalancedEvent   = "unbalanced_event"   // an event's entries do not sum to zero per asset
	DEmptySettlement   = "empty_settlement"   // a settlement has no entries
	DAssetNotZero      = "asset_not_zero"     // sum of all balances of an asset is not zero
	DNegativeBalance   = "negative_balance"   // a customer balance is negative
	DBalanceVsRedis    = "balance_vs_redis"   // ledger balance differs from Redis (available + held)
	DRedisNegative     = "redis_negative"     // a Redis balance field is negative
)

// Discrepancy is one finding.
type Discrepancy struct {
	Kind   string `json:"kind"`
	Detail string `json:"detail"`
}

// Report is the result of a reconciliation run.
type Report struct {
	Symbol        string        `json:"symbol"`
	LogEvents     int           `json:"log_events"`
	LogTrades     int           `json:"log_trades"`
	LedgerTrades  int           `json:"ledger_trades"`
	Accounts      int           `json:"customer_accounts"`
	RedisChecked  int           `json:"redis_accounts_checked"`
	Attempts      int           `json:"attempts"`
	Elapsed       time.Duration `json:"elapsed_ns"`
	Discrepancies []Discrepancy `json:"discrepancies"`
}

// OK reports whether nothing was found.
func (r *Report) OK() bool { return len(r.Discrepancies) == 0 }

// RedisBalances reads the pre-trade risk balances (phase 4). *risk.Checker
// implements it.
type RedisBalances interface {
	Balance(ctx context.Context, account uint64) (risk.Balance, error)
}

// ReconcileConfig configures Reconcile.
type ReconcileConfig struct {
	Brokers []string
	Symbol  string
	Fees    Fees          // must match the consumer's, they are used to recompute
	Redis   RedisBalances // optional: skip the Redis comparison when nil
	Wait    time.Duration // keep re-checking for this long while discrepancies remain (the ledger and Redis settle asynchronously)
}

type acctKey struct {
	kind  AcctKind
	owner uint64
	asset string
}

// Reconcile compares the ledger with (1) a recomputation from the event log
// plus the recorded deposits and (2) the Redis balances, and checks the ledger
// invariants. Redis and the ledger settle asynchronously behind the engine, so
// the comparison is meaningful when the system is quiescent; with Wait > 0 it
// is repeated until it is clean or the time is up, and the last report is
// returned. A non-nil error means the check itself could not run.
func (s *Store) Reconcile(ctx context.Context, cfg ReconcileConfig) (*Report, error) {
	start := time.Now()
	for attempt := 1; ; attempt++ {
		rep, err := s.reconcileOnce(ctx, cfg)
		if err != nil {
			return nil, err
		}
		rep.Attempts = attempt
		rep.Elapsed = time.Since(start)
		if rep.OK() || time.Since(start) >= cfg.Wait {
			return rep, nil
		}
		select {
		case <-ctx.Done():
			return rep, nil
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (s *Store) reconcileOnce(ctx context.Context, cfg ReconcileConfig) (*Report, error) {
	a, err := SplitSymbol(cfg.Symbol)
	if err != nil {
		return nil, err
	}
	rep := &Report{Symbol: cfg.Symbol, Discrepancies: []Discrepancy{}}
	add := func(kind, format string, args ...any) {
		rep.Discrepancies = append(rep.Discrepancies, Discrepancy{Kind: kind, Detail: fmt.Sprintf(format, args...)})
	}

	// 1. Recompute from the event log.
	expected := map[acctKey]int64{}
	var logTrades []uint64
	var prev uint64
	err = eventlog.Scan(ctx, cfg.Brokers, cfg.Symbol, func(_ int64, evs []engine.Event) error {
		for i := range evs {
			rep.LogEvents++
			if evs[i].Seq != prev+1 {
				add(DLogGap, "sequence %d follows %d", evs[i].Seq, prev)
			}
			prev = evs[i].Seq
			if evs[i].Kind != engine.EvTrade {
				continue
			}
			p, err := TradePosting(cfg.Symbol, a, cfg.Fees, evs[i])
			if err != nil {
				add(DLogInvalidTrade, "seq %d: %v", evs[i].Seq, err)
				continue
			}
			logTrades = append(logTrades, evs[i].Seq)
			for _, e := range p.Entries {
				expected[acctKey{e.Acct, e.Owner, e.Asset}] += e.Delta
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scan event log: %w", err)
	}
	rep.LogTrades = len(logTrades)

	// 2. Settlements against the log's trades, exactly one each.
	ledgerTrades, err := s.tradeSeqs(ctx, cfg.Symbol)
	if err != nil {
		return nil, err
	}
	rep.LedgerTrades = len(ledgerTrades)
	missing, orphan := diffSorted(logTrades, ledgerTrades)
	for _, q := range missing {
		add(DMissingSettlement, "trade seq %d is in the event log but not settled", q)
	}
	for _, q := range orphan {
		add(DOrphanSettlement, "settlement for seq %d has no trade in the event log", q)
	}

	// 3. Deposits are not in the log: they are added to the recomputation.
	deps, err := s.sumEntries(ctx, `SELECT acct_kind, owner, asset, sum(delta) FROM entries WHERE entry_kind = 'deposit' GROUP BY 1, 2, 3`)
	if err != nil {
		return nil, err
	}
	for k, v := range deps {
		expected[k] += v
	}

	balances, err := s.Balances(ctx)
	if err != nil {
		return nil, err
	}
	actual := make(map[acctKey]int64, len(balances))
	for _, b := range balances {
		actual[acctKey{b.Acct, b.Owner, b.Asset}] = b.Amount
	}
	for _, k := range sortedKeys(expected, actual) {
		if expected[k] != actual[k] {
			add(DBalanceVsLog, "%s %d %s: ledger %d, recomputed %d", k.kind, k.owner, k.asset, actual[k], expected[k])
		}
	}

	// 4. Invariants inside the ledger.
	sums, err := s.sumEntries(ctx, `SELECT acct_kind, owner, asset, sum(delta) FROM entries GROUP BY 1, 2, 3`)
	if err != nil {
		return nil, err
	}
	for _, k := range sortedKeys(sums, actual) {
		if sums[k] != actual[k] {
			add(DBalanceVsEntries, "%s %d %s: balance %d, entries sum %d", k.kind, k.owner, k.asset, actual[k], sums[k])
		}
	}
	rows, err := s.pool.Query(ctx, `SELECT event_id, asset, sum(delta) FROM entries GROUP BY 1, 2 HAVING sum(delta) <> 0 ORDER BY 1, 2`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id, asset string
		var d int64
		if err := rows.Scan(&id, &asset, &d); err != nil {
			rows.Close()
			return nil, err
		}
		add(DUnbalancedEvent, "%s is off by %d %s", id, d, asset)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows, err = s.pool.Query(ctx, `SELECT event_id FROM settlements s WHERE NOT EXISTS (SELECT 1 FROM entries e WHERE e.event_id = s.event_id) ORDER BY 1`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		add(DEmptySettlement, "%s has no entries", id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	perAsset := map[string]int64{}
	for _, b := range balances {
		perAsset[b.Asset] += b.Amount
		if b.Acct == Customer && b.Amount < 0 {
			add(DNegativeBalance, "account %d %s is %d", b.Owner, b.Asset, b.Amount)
		}
	}
	for _, asset := range []string{a.Base, a.Quote} {
		if perAsset[asset] != 0 {
			add(DAssetNotZero, "%s balances sum to %d", asset, perAsset[asset])
		}
	}

	// 5. Redis against the ledger.
	owners := map[uint64]bool{}
	for _, b := range balances {
		if b.Acct == Customer {
			owners[b.Owner] = true
		}
	}
	rep.Accounts = len(owners)
	if cfg.Redis != nil {
		fees, err := s.sumEntries(ctx, `SELECT acct_kind, owner, asset, sum(delta) FROM entries WHERE entry_kind = 'fee' AND acct_kind = 'customer' GROUP BY 1, 2, 3`)
		if err != nil {
			return nil, err
		}
		ids := make([]uint64, 0, len(owners))
		for o := range owners {
			ids = append(ids, o)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		for _, id := range ids {
			rb, err := cfg.Redis.Balance(ctx, id)
			if err != nil {
				return nil, fmt.Errorf("read redis balance of %d: %w", id, err)
			}
			rep.RedisChecked++
			if rb.BaseAvail < 0 || rb.BaseHeld < 0 || rb.QuoteAvail < 0 || rb.QuoteHeld < 0 {
				add(DRedisNegative, "account %d: %+v", id, rb)
			}
			// Redis does not charge fees (risk reserves the notional only), so
			// a trader's Redis quote balance is the ledger balance plus the
			// fees the ledger took.
			wantBase := actual[acctKey{Customer, id, a.Base}]
			wantQuote := actual[acctKey{Customer, id, a.Quote}] - fees[acctKey{Customer, id, a.Quote}]
			if got := rb.BaseAvail + rb.BaseHeld; got != wantBase {
				add(DBalanceVsRedis, "account %d %s: redis %d (avail %d + held %d), ledger %d", id, a.Base, got, rb.BaseAvail, rb.BaseHeld, wantBase)
			}
			if got := rb.QuoteAvail + rb.QuoteHeld; got != wantQuote {
				add(DBalanceVsRedis, "account %d %s: redis %d (avail %d + held %d), ledger %d (before fees)", id, a.Quote, got, rb.QuoteAvail, rb.QuoteHeld, wantQuote)
			}
		}
	}
	return rep, nil
}

func (s *Store) tradeSeqs(ctx context.Context, symbol string) ([]uint64, error) {
	rows, err := s.pool.Query(ctx, `SELECT seq FROM settlements WHERE kind = 'trade' AND symbol = $1 ORDER BY seq`, symbol)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []uint64
	for rows.Next() {
		var q int64
		if err := rows.Scan(&q); err != nil {
			return nil, err
		}
		out = append(out, uint64(q))
	}
	return out, rows.Err()
}

func (s *Store) sumEntries(ctx context.Context, query string) (map[acctKey]int64, error) {
	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[acctKey]int64{}
	for rows.Next() {
		var k string
		var o, v int64
		var asset string
		if err := rows.Scan(&k, &o, &asset, &v); err != nil {
			return nil, err
		}
		out[acctKey{AcctKind(k), uint64(o), asset}] = v
	}
	return out, rows.Err()
}

func sortedKeys(ms ...map[acctKey]int64) []acctKey {
	seen := map[acctKey]bool{}
	for _, m := range ms {
		for k := range m {
			seen[k] = true
		}
	}
	out := make([]acctKey, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.kind != b.kind {
			return a.kind < b.kind
		}
		if a.owner != b.owner {
			return a.owner < b.owner
		}
		return a.asset < b.asset
	})
	return out
}

// diffSorted returns the values only in a, and only in b. Both are ascending.
func diffSorted(a, b []uint64) (onlyA, onlyB []uint64) {
	i, j := 0, 0
	for i < len(a) || j < len(b) {
		switch {
		case j >= len(b) || (i < len(a) && a[i] < b[j]):
			onlyA = append(onlyA, a[i])
			i++
		case i >= len(a) || b[j] < a[i]:
			onlyB = append(onlyB, b[j])
			j++
		default:
			i++
			j++
		}
	}
	return
}
