package gateway

import (
	"github.com/itsdamian/mini-exchange/internal/engine"
	"github.com/itsdamian/mini-exchange/internal/pipeline"

	exchangev1 "github.com/itsdamian/mini-exchange/gen/exchange/v1"
)

func toEngineSide(s exchangev1.Side) engine.Side {
	switch s {
	case exchangev1.Side_SIDE_BUY:
		return engine.Buy
	case exchangev1.Side_SIDE_SELL:
		return engine.Sell
	}
	return 0
}

func fromEngineSide(s engine.Side) exchangev1.Side {
	switch s {
	case engine.Buy:
		return exchangev1.Side_SIDE_BUY
	case engine.Sell:
		return exchangev1.Side_SIDE_SELL
	}
	return exchangev1.Side_SIDE_UNSPECIFIED
}

func toEngineType(t exchangev1.OrderType) engine.OrderType {
	switch t {
	case exchangev1.OrderType_ORDER_TYPE_LIMIT:
		return engine.Limit
	case exchangev1.OrderType_ORDER_TYPE_MARKET:
		return engine.Market
	}
	return 0
}

func toEngineTIF(t exchangev1.TimeInForce) engine.TimeInForce {
	switch t {
	case exchangev1.TimeInForce_TIME_IN_FORCE_GTC:
		return engine.GTC
	case exchangev1.TimeInForce_TIME_IN_FORCE_IOC:
		return engine.IOC
	}
	return 0
}

func fromEngineReject(r engine.RejectReason) exchangev1.RejectReason {
	switch r {
	case engine.RejectInvalidPrice:
		return exchangev1.RejectReason_REJECT_REASON_INVALID_PRICE
	case engine.RejectInvalidQuantity:
		return exchangev1.RejectReason_REJECT_REASON_INVALID_QUANTITY
	case engine.RejectUnknownOrder:
		return exchangev1.RejectReason_REJECT_REASON_UNKNOWN_ORDER
	case engine.RejectInsufficientFunds:
		return exchangev1.RejectReason_REJECT_REASON_INSUFFICIENT_FUNDS
	case engine.RejectRateLimited:
		return exchangev1.RejectReason_REJECT_REASON_RATE_LIMITED
	case engine.RejectInvalidOrder:
		return exchangev1.RejectReason_REJECT_REASON_INVALID_ORDER
	case engine.RejectDuplicateOrderID:
		return exchangev1.RejectReason_REJECT_REASON_DUPLICATE_ORDER_ID
	}
	return exchangev1.RejectReason_REJECT_REASON_UNSPECIFIED
}

func toProtoReport(r *pipeline.Report) *exchangev1.ExecutionReport {
	var t exchangev1.ExecutionType
	switch r.Type {
	case pipeline.ReportNew:
		t = exchangev1.ExecutionType_EXECUTION_TYPE_NEW
	case pipeline.ReportTrade:
		t = exchangev1.ExecutionType_EXECUTION_TYPE_TRADE
	case pipeline.ReportCanceled:
		t = exchangev1.ExecutionType_EXECUTION_TYPE_CANCELED
	case pipeline.ReportRejected:
		t = exchangev1.ExecutionType_EXECUTION_TYPE_REJECTED
	}
	return &exchangev1.ExecutionReport{
		Type: t, AccountId: r.AccountID, OrderId: r.OrderID, ClientOrderId: r.ClientOrderID,
		Side: fromEngineSide(r.Side), Price: r.Price, Quantity: r.Qty, LeavesQuantity: r.Leaves,
		IsMaker: r.IsMaker, RejectReason: fromEngineReject(r.Reject), Sequence: r.Seq, TimestampNs: r.Ts,
	}
}
