package bot

import (
	"context"
	"errors"
	"io"
	"time"

	"google.golang.org/grpc/status"

	exchangev1 "github.com/itsdamian/mini-exchange/gen/exchange/v1"
)

// Stats counts what a run did.
type Stats struct {
	Steps     int
	Placed    int
	Cancelled int
	Rejected  int // typed rejects from the gateway (including cancels of orders that already traded)
	Errors    int // transport errors
}

// Runner sends the planner's actions to the gateway one at a time, in order:
// each request is answered before the next is sent, so the order in which the
// engine sees them is the order the planner produced.
type Runner struct {
	Client   exchangev1.OrderGatewayServiceClient
	Symbol   string
	Planner  *Planner
	Interval time.Duration // pause between steps (0: as fast as possible)
	MaxSteps int           // 0: until the source ends
	OnError  func(error)
}

// Run drives the planner with prices from src until the source ends (io.EOF),
// MaxSteps is reached or ctx ends.
func (r *Runner) Run(ctx context.Context, src PriceSource) (Stats, error) {
	var st Stats
	for r.MaxSteps == 0 || st.Steps < r.MaxSteps {
		price, err := src.Next(ctx)
		if errors.Is(err, io.EOF) {
			return st, nil
		}
		if err != nil {
			return st, err
		}
		for _, a := range r.Planner.Step(price) {
			if ctx.Err() != nil {
				return st, ctx.Err()
			}
			r.send(ctx, a, &st)
		}
		st.Steps++
		if r.Interval > 0 {
			select {
			case <-ctx.Done():
				return st, ctx.Err()
			case <-time.After(r.Interval):
			}
		}
	}
	return st, nil
}

func (r *Runner) send(ctx context.Context, a Action, st *Stats) {
	if a.Cancel {
		resp, err := r.Client.CancelOrder(ctx, &exchangev1.CancelOrderRequest{ClientOrderId: a.ClientID, AccountId: a.Account, Symbol: r.Symbol})
		switch {
		case err != nil:
			r.fail(err, st)
		case resp.Status == exchangev1.OrderStatus_ORDER_STATUS_CANCELED:
			st.Cancelled++
		default:
			st.Rejected++
		}
		return
	}
	req := &exchangev1.PlaceOrderRequest{
		ClientOrderId: a.ClientID, AccountId: a.Account, Symbol: r.Symbol,
		Side: exchangev1.Side_SIDE_SELL, Type: exchangev1.OrderType_ORDER_TYPE_LIMIT,
		TimeInForce: exchangev1.TimeInForce_TIME_IN_FORCE_GTC, Price: a.Price, Quantity: a.Qty,
	}
	if a.Buy {
		req.Side = exchangev1.Side_SIDE_BUY
	}
	if a.Market {
		req.Type, req.Price = exchangev1.OrderType_ORDER_TYPE_MARKET, 0
	}
	if a.IOC {
		req.TimeInForce = exchangev1.TimeInForce_TIME_IN_FORCE_IOC
	}
	resp, err := r.Client.PlaceOrder(ctx, req)
	switch {
	case err != nil:
		r.fail(err, st)
	case resp.Status == exchangev1.OrderStatus_ORDER_STATUS_REJECTED:
		st.Rejected++
	default:
		st.Placed++
	}
}

func (r *Runner) fail(err error, st *Stats) {
	st.Errors++
	if r.OnError != nil {
		r.OnError(status.Convert(err).Err())
	}
}
