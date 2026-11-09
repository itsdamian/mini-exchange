// Package eventlog publishes engine events to Kafka, snapshots the book, and
// recovers engine state from snapshot plus log replay.
//
// The Kafka event stream is the system of record (docs/adr/0007): one topic
// per symbol with a single partition (total order), one record per engine
// command (an EventBatch, so a command is atomic in the log).
package eventlog

import (
	"fmt"

	"github.com/itsdamian/mini-exchange/internal/engine"

	exchangev1 "github.com/itsdamian/mini-exchange/gen/exchange/v1"
)

// SchemaVersion is the EventBatch / Snapshot schema version this build
// writes and accepts.
const SchemaVersion = 1

// Topic returns the Kafka topic of a symbol's event log.
func Topic(symbol string) string { return "mx.events." + symbol }

func toProtoType(k engine.EventKind) exchangev1.EventType {
	switch k {
	case engine.EvAccepted:
		return exchangev1.EventType_EVENT_TYPE_ORDER_ACCEPTED
	case engine.EvRejected:
		return exchangev1.EventType_EVENT_TYPE_ORDER_REJECTED
	case engine.EvCanceled:
		return exchangev1.EventType_EVENT_TYPE_ORDER_CANCELED
	case engine.EvTrade:
		return exchangev1.EventType_EVENT_TYPE_TRADE_EXECUTED
	}
	return exchangev1.EventType_EVENT_TYPE_UNSPECIFIED
}

func fromProtoType(t exchangev1.EventType) (engine.EventKind, error) {
	switch t {
	case exchangev1.EventType_EVENT_TYPE_ORDER_ACCEPTED:
		return engine.EvAccepted, nil
	case exchangev1.EventType_EVENT_TYPE_ORDER_REJECTED:
		return engine.EvRejected, nil
	case exchangev1.EventType_EVENT_TYPE_ORDER_CANCELED:
		return engine.EvCanceled, nil
	case exchangev1.EventType_EVENT_TYPE_TRADE_EXECUTED:
		return engine.EvTrade, nil
	}
	return 0, fmt.Errorf("unknown event type %v", t)
}

func toProtoSide(s engine.Side) exchangev1.Side {
	switch s {
	case engine.Buy:
		return exchangev1.Side_SIDE_BUY
	case engine.Sell:
		return exchangev1.Side_SIDE_SELL
	}
	return exchangev1.Side_SIDE_UNSPECIFIED
}

func fromProtoSide(s exchangev1.Side) engine.Side {
	switch s {
	case exchangev1.Side_SIDE_BUY:
		return engine.Buy
	case exchangev1.Side_SIDE_SELL:
		return engine.Sell
	}
	return 0
}

// The reject enum is numerically identical in engine and proto (both are
// 0..7); the codec still maps explicitly so a mismatch cannot pass silently.
var rejectToProto = map[engine.RejectReason]exchangev1.RejectReason{
	engine.RejectNone:              exchangev1.RejectReason_REJECT_REASON_UNSPECIFIED,
	engine.RejectInvalidPrice:      exchangev1.RejectReason_REJECT_REASON_INVALID_PRICE,
	engine.RejectInvalidQuantity:   exchangev1.RejectReason_REJECT_REASON_INVALID_QUANTITY,
	engine.RejectUnknownOrder:      exchangev1.RejectReason_REJECT_REASON_UNKNOWN_ORDER,
	engine.RejectInsufficientFunds: exchangev1.RejectReason_REJECT_REASON_INSUFFICIENT_FUNDS,
	engine.RejectRateLimited:       exchangev1.RejectReason_REJECT_REASON_RATE_LIMITED,
	engine.RejectInvalidOrder:      exchangev1.RejectReason_REJECT_REASON_INVALID_ORDER,
	engine.RejectDuplicateOrderID:  exchangev1.RejectReason_REJECT_REASON_DUPLICATE_ORDER_ID,
}

func fromProtoReject(r exchangev1.RejectReason) (engine.RejectReason, error) {
	for e, p := range rejectToProto {
		if p == r {
			return e, nil
		}
	}
	return 0, fmt.Errorf("unknown reject reason %v", r)
}

// ToProtoBatch converts the events of one command to an EventBatch.
func ToProtoBatch(symbol string, evs []engine.Event) *exchangev1.EventBatch {
	b := &exchangev1.EventBatch{SchemaVersion: SchemaVersion, Symbol: symbol, Events: make([]*exchangev1.EngineEvent, len(evs))}
	for i := range evs {
		e := &evs[i]
		b.Events[i] = &exchangev1.EngineEvent{
			Sequence: e.Seq, Type: toProtoType(e.Kind), TimestampNs: e.Ts,
			OrderId: e.OrderID, AccountId: e.AccountID, Side: toProtoSide(e.Side),
			Price: e.Price, Quantity: e.Qty, RejectReason: rejectToProto[e.Reject],
			MakerOrderId: e.MakerOrderID, MakerAccountId: e.MakerAccountID,
		}
	}
	return b
}

// FromProtoBatch validates and converts an EventBatch. It rejects unknown
// schema versions, a wrong symbol, empty batches and non-contiguous sequences.
func FromProtoBatch(b *exchangev1.EventBatch, symbol string) ([]engine.Event, error) {
	if b.GetSchemaVersion() != SchemaVersion {
		return nil, fmt.Errorf("unsupported event schema version %d (this build reads %d)", b.GetSchemaVersion(), SchemaVersion)
	}
	if b.GetSymbol() != symbol {
		return nil, fmt.Errorf("event batch for symbol %q in the log of %q", b.GetSymbol(), symbol)
	}
	if len(b.GetEvents()) == 0 {
		return nil, fmt.Errorf("empty event batch")
	}
	evs := make([]engine.Event, len(b.GetEvents()))
	for i, p := range b.GetEvents() {
		kind, err := fromProtoType(p.GetType())
		if err != nil {
			return nil, err
		}
		rej, err := fromProtoReject(p.GetRejectReason())
		if err != nil {
			return nil, err
		}
		evs[i] = engine.Event{
			Kind: kind, Side: fromProtoSide(p.GetSide()), Reject: rej, Seq: p.GetSequence(), Ts: p.GetTimestampNs(),
			OrderID: p.GetOrderId(), AccountID: p.GetAccountId(),
			MakerOrderID: p.GetMakerOrderId(), MakerAccountID: p.GetMakerAccountId(),
			Price: p.GetPrice(), Qty: p.GetQuantity(),
		}
		if i > 0 && evs[i].Seq != evs[i-1].Seq+1 {
			return nil, fmt.Errorf("non-contiguous sequence in batch: %d after %d", evs[i].Seq, evs[i-1].Seq)
		}
	}
	return evs, nil
}
