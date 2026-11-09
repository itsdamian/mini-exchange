// Package gateway implements the gRPC order entry API on top of the pipeline.
package gateway

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/itsdamian/mini-exchange/internal/engine"
	"github.com/itsdamian/mini-exchange/internal/pipeline"
	"github.com/itsdamian/mini-exchange/internal/risk"

	exchangev1 "github.com/itsdamian/mini-exchange/gen/exchange/v1"
)

// Risk is the pre-trade risk check the gateway calls before the engine.
// *risk.Checker implements it.
type Risk interface {
	// Reserve holds the order's funds or returns a typed reject reason. A
	// non-nil error means the check could not be made (the order is refused).
	Reserve(ctx context.Context, r risk.Request) (risk.Reason, error)
	// Release gives back a reservation for an order that never reached the engine.
	Release(ctx context.Context, account uint64, client string) error
	// Backlogged reports that settlement is too far behind to admit new orders.
	Backlogged() bool
}

// Server implements exchangev1.OrderGatewayServiceServer for one symbol.
type Server struct {
	exchangev1.UnimplementedOrderGatewayServiceServer

	symbol  string
	p       *pipeline.Pipeline
	places  *onceStore[*exchangev1.PlaceOrderResponse]
	cancels *onceStore[*exchangev1.CancelOrderResponse]
	latency *prometheus.HistogramVec
	risk    Risk // nil: no pre-trade risk
}

// New returns a Server. reg may be nil.
func New(symbol string, p *pipeline.Pipeline, reg prometheus.Registerer) *Server {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	return &Server{
		symbol:  symbol,
		p:       p,
		places:  newOnceStore[*exchangev1.PlaceOrderResponse](),
		cancels: newOnceStore[*exchangev1.CancelOrderResponse](),
		latency: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "mx_gateway_request_seconds",
			Help:    "Gateway unary request latency, by RPC and gRPC status code.",
			Buckets: prometheus.ExponentialBuckets(1e-5, 2, 18), // 10 us .. ~1.3 s
		}, []string{"rpc", "code"}),
	}
}

// WithRisk enables pre-trade risk. Call it before serving.
func (s *Server) WithRisk(r Risk) *Server { s.risk = r; return s }

// Register adds the service to a gRPC server.
func (s *Server) Register(g grpc.ServiceRegistrar) {
	exchangev1.RegisterOrderGatewayServiceServer(g, s)
}

// UnaryInterceptor records mx_gateway_request_seconds for unary RPCs.
func (s *Server) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := h(ctx, req)
		s.latency.WithLabelValues(shortName(info.FullMethod), status.Code(err).String()).Observe(time.Since(start).Seconds())
		return resp, err
	}
}

func shortName(full string) string {
	for i := len(full) - 1; i >= 0; i-- {
		if full[i] == '/' {
			return full[i+1:]
		}
	}
	return full
}

func pipelineErr(err error) error {
	switch {
	case errors.Is(err, pipeline.ErrOverloaded):
		return status.Error(codes.ResourceExhausted, "engine queue full, retry later")
	case errors.Is(err, pipeline.ErrLogBacklogged):
		return status.Error(codes.ResourceExhausted, "event log backed up or unavailable, retry later")
	case errors.Is(err, pipeline.ErrStopped):
		return status.Error(codes.Unavailable, "gateway is shutting down")
	}
	return status.Error(codes.Internal, err.Error())
}

// transient reports whether nothing was enqueued, so the key may be retried.
func transient(err error) bool {
	return errors.Is(err, pipeline.ErrOverloaded) || errors.Is(err, pipeline.ErrLogBacklogged) || errors.Is(err, pipeline.ErrStopped)
}

// PlaceOrder implements the idempotent order entry RPC.
func (s *Server) PlaceOrder(ctx context.Context, req *exchangev1.PlaceOrderRequest) (*exchangev1.PlaceOrderResponse, error) {
	cmd, err := buildPlace(req, s.symbol)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	k := key{req.GetAccountId(), req.GetClientOrderId()}
	c, first := s.places.begin(k)
	if !first {
		select {
		case <-c.done:
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		if c.err != nil {
			return nil, c.err
		}
		return proto.Clone(c.val).(*exchangev1.PlaceOrderResponse), nil
	}

	// The first caller executes. The engine is detached from this caller's
	// cancellation: once enqueued the order will run, and the outcome must be
	// recorded so a retry returns it instead of hanging.
	reserved := false
	if s.risk != nil {
		resp, perr := s.reserve(ctx, req, cmd)
		if perr != nil || resp != nil {
			// Neither is cached: no order exists, so a retry with the same key is
			// evaluated afresh (a rate-limited or underfunded client may succeed
			// later) and still can never create a second order.
			s.places.finish(k, c, resp, perr, true)
			if perr != nil {
				return nil, perr
			}
			return proto.Clone(resp).(*exchangev1.PlaceOrderResponse), nil
		}
		reserved = true
	}

	ev, err := s.p.Submit(context.WithoutCancel(ctx), cmd, req.GetClientOrderId())
	if err != nil {
		if reserved {
			s.release(req)
		}
		perr := pipelineErr(err)
		s.places.finish(k, c, nil, perr, transient(err))
		return nil, perr
	}
	resp := &exchangev1.PlaceOrderResponse{ClientOrderId: req.GetClientOrderId(), Sequence: ev.Seq}
	if ev.Kind == engine.EvAccepted {
		resp.OrderId = ev.OrderID
		resp.Status = exchangev1.OrderStatus_ORDER_STATUS_ACCEPTED
	} else {
		resp.Status = exchangev1.OrderStatus_ORDER_STATUS_REJECTED
		resp.RejectReason = fromEngineReject(ev.Reject)
	}
	s.places.finish(k, c, resp, nil, false)
	return proto.Clone(resp).(*exchangev1.PlaceOrderResponse), nil
}

// CancelOrder cancels by the client_order_id the order was placed with.
func (s *Server) CancelOrder(ctx context.Context, req *exchangev1.CancelOrderRequest) (*exchangev1.CancelOrderResponse, error) {
	if err := validateCancel(req, s.symbol); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	k := key{req.GetAccountId(), req.GetClientOrderId()}
	placed, ok := s.places.peek(k)
	if !ok || placed.GetStatus() != exchangev1.OrderStatus_ORDER_STATUS_ACCEPTED {
		// Not (yet) a known accepted order. Not cached, so a later retry works.
		return &exchangev1.CancelOrderResponse{
			ClientOrderId: req.GetClientOrderId(),
			Status:        exchangev1.OrderStatus_ORDER_STATUS_REJECTED,
			RejectReason:  exchangev1.RejectReason_REJECT_REASON_UNKNOWN_ORDER,
		}, nil
	}

	c, first := s.cancels.begin(k)
	if !first {
		select {
		case <-c.done:
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		if c.err != nil {
			return nil, c.err
		}
		return proto.Clone(c.val).(*exchangev1.CancelOrderResponse), nil
	}

	ev, err := s.p.Submit(context.WithoutCancel(ctx),
		engine.Command{Kind: engine.CmdCancel, OrderID: placed.GetOrderId(), AccountID: req.GetAccountId()},
		req.GetClientOrderId())
	if err != nil {
		perr := pipelineErr(err)
		s.cancels.finish(k, c, nil, perr, transient(err))
		return nil, perr
	}
	resp := &exchangev1.CancelOrderResponse{ClientOrderId: req.GetClientOrderId(), OrderId: placed.GetOrderId(), Sequence: ev.Seq}
	if ev.Kind == engine.EvCanceled {
		resp.Status = exchangev1.OrderStatus_ORDER_STATUS_CANCELED
		resp.CanceledQuantity = ev.Qty
	} else {
		resp.Status = exchangev1.OrderStatus_ORDER_STATUS_REJECTED
		resp.RejectReason = fromEngineReject(ev.Reject)
	}
	s.cancels.finish(k, c, resp, nil, false)
	return proto.Clone(resp).(*exchangev1.CancelOrderResponse), nil
}

// StreamExecutionReports streams reports for one account until the client
// goes away, the gateway stops, or the client falls too far behind.
func (s *Server) StreamExecutionReports(req *exchangev1.StreamExecutionReportsRequest, stream grpc.ServerStreamingServer[exchangev1.StreamExecutionReportsResponse]) error {
	if req.GetAccountId() == 0 {
		return status.Error(codes.InvalidArgument, "account_id is required")
	}
	sub, err := s.p.Subscribe(req.GetAccountId())
	if err != nil {
		return pipelineErr(err)
	}
	defer sub.Close()
	// Headers tell the client the subscription is live, so it can place
	// orders without racing the subscription.
	if err := stream.SendHeader(metadata.MD{}); err != nil {
		return err
	}
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case r, ok := <-sub.C:
			if !ok {
				switch err := sub.Err(); {
				case errors.Is(err, pipeline.ErrSlowConsumer):
					return status.Error(codes.ResourceExhausted, "client too slow, resubscribe")
				case err != nil:
					return status.Error(codes.Unavailable, "gateway is shutting down")
				}
				return nil
			}
			if err := stream.Send(&exchangev1.StreamExecutionReportsResponse{Report: toProtoReport(&r)}); err != nil {
				return err
			}
		}
	}
}

// reserve runs the pre-trade check. It returns a rejection response, or a
// transport error, or neither when the funds are held and the order may go to
// the engine.
func (s *Server) reserve(ctx context.Context, req *exchangev1.PlaceOrderRequest, cmd engine.Command) (*exchangev1.PlaceOrderResponse, error) {
	if s.risk.Backlogged() {
		return nil, status.Error(codes.ResourceExhausted, "risk settlement backed up, retry later")
	}
	reason, err := s.risk.Reserve(ctx, risk.Request{
		Account: cmd.AccountID, Client: req.GetClientOrderId(), Side: cmd.Side, Type: cmd.Type, Price: cmd.Price, Qty: cmd.Qty,
	})
	if err != nil {
		// Fail closed: without Redis there is no safe way to admit an order.
		return nil, status.Error(codes.Unavailable, "pre-trade risk check unavailable, retry later")
	}
	if reason == risk.OK {
		return nil, nil
	}
	return &exchangev1.PlaceOrderResponse{
		ClientOrderId: req.GetClientOrderId(),
		Status:        exchangev1.OrderStatus_ORDER_STATUS_REJECTED,
		RejectReason:  fromRiskReason(reason),
	}, nil
}

// release returns the reservation of an order that was refused before the
// engine saw it. If Redis cannot be reached the funds stay held until
// reconciliation; that is logged by the caller's metrics, not hidden.
func (s *Server) release(req *exchangev1.PlaceOrderRequest) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = s.risk.Release(ctx, req.GetAccountId(), req.GetClientOrderId())
}
