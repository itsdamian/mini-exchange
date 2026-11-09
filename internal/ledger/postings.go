// Package ledger is the double-entry ledger in PostgreSQL (FR-6): it turns
// trade events from the Kafka event log into balanced entries, idempotently,
// isolates poison records in a dead-letter topic, and reconciles its balances
// against Redis and against a recomputation from the log.
package ledger

import (
	"errors"
	"fmt"
	"math"
	"math/bits"
	"strings"

	"github.com/itsdamian/mini-exchange/internal/engine"
)

// AcctKind separates customer accounts from the system accounts.
type AcctKind string

// Account kinds. A customer balance can never be negative (a database check);
// the fee account collects fees; the external account is the counterparty of
// deposits, so its balance is minus everything deposited.
const (
	Customer AcctKind = "customer"
	FeeAcct  AcctKind = "fee"
	External AcctKind = "external"
)

// Entry kinds.
const (
	EntryCash    = "cash"    // quote asset moves between buyer and seller
	EntryAsset   = "asset"   // base asset moves between seller and buyer
	EntryFee     = "fee"     // fee moves from a trader to the fee account
	EntryDeposit = "deposit" // funds enter from the external account
)

// Assets are the two sides of a symbol, e.g. BTC-USD is base BTC, quote USD.
type Assets struct{ Base, Quote string }

// SplitSymbol splits "BASE-QUOTE".
func SplitSymbol(symbol string) (Assets, error) {
	b, q, ok := strings.Cut(symbol, "-")
	if !ok || b == "" || q == "" || strings.Contains(q, "-") {
		return Assets{}, fmt.Errorf("ledger: symbol %q is not BASE-QUOTE", symbol)
	}
	return Assets{Base: b, Quote: q}, nil
}

// Fees are charged in the quote asset on the notional (price*qty), in basis
// points, to each side of a trade: the taker pays TakerBps, the maker MakerBps.
// Zero means no fee. Risk (phase 4) does not reserve fees, so with a non-zero
// fee a buyer who spent everything can be refused by the ledger (see ADR 0010).
type Fees struct{ TakerBps, MakerBps int64 }

// Entry is one line of a posting. Delta is the effect on the account balance:
// a debit (money leaves) is negative, the matching credit positive.
type Entry struct {
	Kind  string
	Acct  AcctKind
	Owner uint64 // customer account id; 0 for system accounts
	Asset string
	Delta int64
}

// Posting is the set of entries of one settlement, identified by EventID.
type Posting struct {
	EventID string
	Kind    string // "trade" or "deposit"
	Symbol  string
	Seq     uint64 // engine sequence (trades), 0 for deposits
	Entries []Entry
}

// PoisonError marks input that can never be applied, however often it is
// retried. The consumer sends such records to the dead-letter topic.
type PoisonError struct{ Reason string }

func (e *PoisonError) Error() string { return e.Reason }

func poison(format string, a ...any) error { return &PoisonError{Reason: fmt.Sprintf(format, a...)} }

// TradeEventID is the idempotency key of a trade: unique per symbol and seq.
func TradeEventID(symbol string, seq uint64) string { return fmt.Sprintf("%s:%d", symbol, seq) }

// TradePosting builds the balanced entries of one trade event:
//
//	buyer  quote -notional     seller quote +notional   (cash)
//	seller base  -qty          buyer  base  +qty        (asset)
//	buyer / seller quote -fee, fee account quote +fee   (fee, only if > 0)
//
// The trade price is the maker's resting price, which is what the buyer pays
// and the seller receives; any difference to a buyer's limit price is never
// charged (the risk layer holds funds at the limit and releases the rest).
func TradePosting(symbol string, a Assets, f Fees, ev engine.Event) (Posting, error) {
	if ev.Kind != engine.EvTrade {
		return Posting{}, poison("event seq %d is not a trade", ev.Seq)
	}
	if ev.Qty <= 0 || ev.Price <= 0 {
		return Posting{}, poison("trade seq %d has price %d quantity %d", ev.Seq, ev.Price, ev.Qty)
	}
	if ev.AccountID == 0 || ev.MakerAccountID == 0 || ev.AccountID > math.MaxInt64 || ev.MakerAccountID > math.MaxInt64 {
		return Posting{}, poison("trade seq %d has invalid account ids %d / %d", ev.Seq, ev.AccountID, ev.MakerAccountID)
	}
	notional, ok := mulChecked(ev.Qty, ev.Price)
	if !ok {
		return Posting{}, poison("trade seq %d notional overflows", ev.Seq)
	}
	buyer, seller := ev.AccountID, ev.MakerAccountID
	buyerBps, sellerBps := f.TakerBps, f.MakerBps
	switch ev.Side {
	case engine.Buy:
	case engine.Sell:
		buyer, seller = seller, buyer
		buyerBps, sellerBps = sellerBps, buyerBps
	default:
		return Posting{}, poison("trade seq %d has no taker side", ev.Seq)
	}

	es := make([]Entry, 0, 6)
	es = append(es,
		Entry{EntryCash, Customer, buyer, a.Quote, -notional},
		Entry{EntryCash, Customer, seller, a.Quote, notional},
		Entry{EntryAsset, Customer, seller, a.Base, -ev.Qty},
		Entry{EntryAsset, Customer, buyer, a.Base, ev.Qty},
	)
	for _, side := range []struct {
		acct uint64
		bps  int64
	}{{buyer, buyerBps}, {seller, sellerBps}} {
		fee, ok := feeOf(notional, side.bps)
		if !ok {
			return Posting{}, poison("trade seq %d fee overflows", ev.Seq)
		}
		if fee > 0 {
			es = append(es,
				Entry{EntryFee, Customer, side.acct, a.Quote, -fee},
				Entry{EntryFee, FeeAcct, 0, a.Quote, fee})
		}
	}
	p := Posting{EventID: TradeEventID(symbol, ev.Seq), Kind: "trade", Symbol: symbol, Seq: ev.Seq, Entries: es}
	return p, p.Balanced()
}

// DepositPosting credits an account from the external account.
func DepositPosting(ref string, symbol string, a Assets, account uint64, base, quote int64) (Posting, error) {
	if ref == "" {
		return Posting{}, errors.New("ledger: deposit needs a reference")
	}
	if account == 0 || account > math.MaxInt64 {
		return Posting{}, fmt.Errorf("ledger: invalid account %d", account)
	}
	if base < 0 || quote < 0 || base+quote == 0 {
		return Posting{}, errors.New("ledger: deposit amounts must be non-negative and not both zero")
	}
	var es []Entry
	if base > 0 {
		es = append(es, Entry{EntryDeposit, External, 0, a.Base, -base}, Entry{EntryDeposit, Customer, account, a.Base, base})
	}
	if quote > 0 {
		es = append(es, Entry{EntryDeposit, External, 0, a.Quote, -quote}, Entry{EntryDeposit, Customer, account, a.Quote, quote})
	}
	p := Posting{EventID: "deposit:" + ref, Kind: "deposit", Symbol: symbol, Entries: es}
	return p, p.Balanced()
}

// Balanced checks that the entries sum to zero per asset.
func (p Posting) Balanced() error {
	sum := map[string]int64{}
	for _, e := range p.Entries {
		sum[e.Asset] += e.Delta
	}
	for asset, s := range sum {
		if s != 0 {
			return poison("posting %s is unbalanced by %d %s", p.EventID, s, asset)
		}
	}
	return nil
}

func mulChecked(a, b int64) (int64, bool) {
	hi, lo := bits.Mul64(uint64(a), uint64(b))
	if hi != 0 || lo > math.MaxInt64 {
		return 0, false
	}
	return int64(lo), true
}

// feeOf is floor(notional*bps/10000) without overflow.
func feeOf(notional, bps int64) (int64, bool) {
	if bps <= 0 {
		return 0, true
	}
	hi, lo := bits.Mul64(uint64(notional), uint64(bps))
	if hi >= 10000 {
		return 0, false
	}
	q, _ := bits.Div64(hi, lo, 10000)
	if q > math.MaxInt64 {
		return 0, false
	}
	return int64(q), true
}
