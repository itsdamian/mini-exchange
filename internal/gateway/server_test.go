package gateway_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/itsdamian/mini-exchange/internal/gateway"
	"github.com/itsdamian/mini-exchange/internal/pipeline"

	exchangev1 "github.com/itsdamian/mini-exchange/gen/exchange/v1"
)

const symbol = "BTC-USD"

type env struct {
	client exchangev1.OrderGatewayServiceClient
	p      *pipeline.Pipeline
}

func start(t *testing.T, cfg pipeline.Config) *env {
	t.Helper()
	p := pipeline.New(cfg)
	srv := gateway.New(symbol, p, nil)
	lis := bufconn.Listen(1 << 20)
	g := grpc.NewServer(grpc.UnaryInterceptor(srv.UnaryInterceptor()))
	srv.Register(g)
	go func() { _ = g.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
		g.GracefulStop()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, p.Stop(ctx))
	})
	return &env{client: exchangev1.NewOrderGatewayServiceClient(conn), p: p}
}

func place(id string, acct uint64, side exchangev1.Side, price, qty int64) *exchangev1.PlaceOrderRequest {
	return &exchangev1.PlaceOrderRequest{
		ClientOrderId: id, AccountId: acct, Symbol: symbol, Side: side,
		Type: exchangev1.OrderType_ORDER_TYPE_LIMIT, TimeInForce: exchangev1.TimeInForce_TIME_IN_FORCE_GTC,
		Price: price, Quantity: qty,
	}
}

// subscribe opens a report stream and waits until the server confirms it.
func (e *env) subscribe(t *testing.T, acct uint64) exchangev1.OrderGatewayService_StreamExecutionReportsClient {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	st, err := e.client.StreamExecutionReports(ctx, &exchangev1.StreamExecutionReportsRequest{AccountId: acct})
	require.NoError(t, err)
	_, err = st.Header() // blocks until the subscription is registered
	require.NoError(t, err)
	return st
}

func next(t *testing.T, st exchangev1.OrderGatewayService_StreamExecutionReportsClient) *exchangev1.ExecutionReport {
	t.Helper()
	type res struct {
		r   *exchangev1.StreamExecutionReportsResponse
		err error
	}
	ch := make(chan res, 1)
	go func() { r, err := st.Recv(); ch <- res{r, err} }()
	select {
	case x := <-ch:
		require.NoError(t, x.err)
		return x.r.GetReport()
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for an execution report")
		return nil
	}
}

func TestPlaceAcceptedAndRejected(t *testing.T) {
	e := start(t, pipeline.Config{})
	ctx := context.Background()

	r, err := e.client.PlaceOrder(ctx, place("o1", 1, exchangev1.Side_SIDE_BUY, 100, 5))
	require.NoError(t, err)
	require.Equal(t, exchangev1.OrderStatus_ORDER_STATUS_ACCEPTED, r.GetStatus())
	require.Equal(t, uint64(1), r.GetOrderId())
	require.Equal(t, "o1", r.GetClientOrderId())

	// engine-level typed rejects come back as a normal response
	r, err = e.client.PlaceOrder(ctx, place("o2", 1, exchangev1.Side_SIDE_BUY, 0, 5))
	require.NoError(t, err)
	require.Equal(t, exchangev1.OrderStatus_ORDER_STATUS_REJECTED, r.GetStatus())
	require.Equal(t, exchangev1.RejectReason_REJECT_REASON_INVALID_PRICE, r.GetRejectReason())
	require.Zero(t, r.GetOrderId())

	r, err = e.client.PlaceOrder(ctx, place("o3", 1, exchangev1.Side_SIDE_BUY, 100, -1))
	require.NoError(t, err)
	require.Equal(t, exchangev1.RejectReason_REJECT_REASON_INVALID_QUANTITY, r.GetRejectReason())
}

func TestValidationErrors(t *testing.T) {
	e := start(t, pipeline.Config{})
	good := func() *exchangev1.PlaceOrderRequest { return place("ok", 1, exchangev1.Side_SIDE_BUY, 100, 1) }
	mutate := map[string]func(*exchangev1.PlaceOrderRequest){
		"missing client_order_id": func(r *exchangev1.PlaceOrderRequest) { r.ClientOrderId = "" },
		"too long id":             func(r *exchangev1.PlaceOrderRequest) { r.ClientOrderId = string(make([]byte, 65)) },
		"space in id":             func(r *exchangev1.PlaceOrderRequest) { r.ClientOrderId = "a b" },
		"missing account":         func(r *exchangev1.PlaceOrderRequest) { r.AccountId = 0 },
		"unknown symbol":          func(r *exchangev1.PlaceOrderRequest) { r.Symbol = "ETH-USD" },
		"missing side":            func(r *exchangev1.PlaceOrderRequest) { r.Side = exchangev1.Side_SIDE_UNSPECIFIED },
		"missing type":            func(r *exchangev1.PlaceOrderRequest) { r.Type = exchangev1.OrderType_ORDER_TYPE_UNSPECIFIED },
		"limit without tif": func(r *exchangev1.PlaceOrderRequest) {
			r.TimeInForce = exchangev1.TimeInForce_TIME_IN_FORCE_UNSPECIFIED
		},
	}
	for name, m := range mutate {
		t.Run(name, func(t *testing.T) {
			req := good()
			m(req)
			_, err := e.client.PlaceOrder(context.Background(), req)
			require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
		})
	}
	t.Run("market without tif is fine", func(t *testing.T) {
		req := good()
		req.Type, req.TimeInForce = exchangev1.OrderType_ORDER_TYPE_MARKET, exchangev1.TimeInForce_TIME_IN_FORCE_UNSPECIFIED
		_, err := e.client.PlaceOrder(context.Background(), req)
		require.NoError(t, err)
	})
	t.Run("cancel needs the key", func(t *testing.T) {
		_, err := e.client.CancelOrder(context.Background(), &exchangev1.CancelOrderRequest{AccountId: 1, Symbol: symbol})
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})
	t.Run("stream needs an account", func(t *testing.T) {
		st, err := e.client.StreamExecutionReports(context.Background(), &exchangev1.StreamExecutionReportsRequest{})
		require.NoError(t, err)
		_, err = st.Recv()
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})
}

// FR-2 acceptance: the same request retried 100 times concurrently creates
// exactly one order.
func TestIdempotencyConcurrentRetriesCreateExactlyOneOrder(t *testing.T) {
	e := start(t, pipeline.Config{})
	st := e.subscribe(t, 1)

	const n = 100
	resps := make([]*exchangev1.PlaceOrderResponse, n)
	errs := make([]error, n)
	var ready, wg sync.WaitGroup
	start := make(chan struct{})
	ready.Add(n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ready.Done()
			<-start
			resps[i], errs[i] = e.client.PlaceOrder(context.Background(), place("retry-me", 1, exchangev1.Side_SIDE_BUY, 100, 5))
		}()
	}
	ready.Wait()
	close(start)
	wg.Wait()

	for i := 0; i < n; i++ {
		require.NoError(t, errs[i])
		require.Equal(t, uint64(1), resps[i].GetOrderId(), "every retry returns the original order id")
		require.Equal(t, exchangev1.OrderStatus_ORDER_STATUS_ACCEPTED, resps[i].GetStatus())
		require.Equal(t, resps[0].GetSequence(), resps[i].GetSequence())
	}

	// A later retry still returns the original, and the engine saw one order.
	late, err := e.client.PlaceOrder(context.Background(), place("retry-me", 1, exchangev1.Side_SIDE_BUY, 100, 5))
	require.NoError(t, err)
	require.Equal(t, uint64(1), late.GetOrderId())
	require.Equal(t, uint64(1), e.p.Stats().Accepted, "exactly one order reached the engine")

	// Independent observation through the report stream: one NEW report and
	// nothing else.
	require.Equal(t, exchangev1.ExecutionType_EXECUTION_TYPE_NEW, next(t, st).GetType())
	_, err = e.client.PlaceOrder(context.Background(), place("marker", 1, exchangev1.Side_SIDE_BUY, 99, 1))
	require.NoError(t, err)
	r := next(t, st)
	require.Equal(t, "marker", r.GetClientOrderId(), "no second report for retry-me was queued before this one")
}

func TestIdempotencyReplaysRejectsAndIsScopedPerAccount(t *testing.T) {
	e := start(t, pipeline.Config{})
	ctx := context.Background()
	bad := place("dup", 1, exchangev1.Side_SIDE_BUY, 0, 5)
	r1, err := e.client.PlaceOrder(ctx, bad)
	require.NoError(t, err)
	// even a now-valid retry gets the original (rejected) outcome
	r2, err := e.client.PlaceOrder(ctx, place("dup", 1, exchangev1.Side_SIDE_BUY, 100, 5))
	require.NoError(t, err)
	require.Equal(t, r1.GetStatus(), r2.GetStatus())
	require.Equal(t, exchangev1.OrderStatus_ORDER_STATUS_REJECTED, r2.GetStatus())

	// same key, different account: an independent order
	r3, err := e.client.PlaceOrder(ctx, place("dup", 2, exchangev1.Side_SIDE_BUY, 100, 5))
	require.NoError(t, err)
	require.Equal(t, exchangev1.OrderStatus_ORDER_STATUS_ACCEPTED, r3.GetStatus())
}

func TestTradeFlowAndReports(t *testing.T) {
	e := start(t, pipeline.Config{})
	ctx := context.Background()
	seller, buyer := e.subscribe(t, 1), e.subscribe(t, 2)

	_, err := e.client.PlaceOrder(ctx, place("s1", 1, exchangev1.Side_SIDE_SELL, 100, 10))
	require.NoError(t, err)
	_, err = e.client.PlaceOrder(ctx, place("b1", 2, exchangev1.Side_SIDE_BUY, 101, 4))
	require.NoError(t, err)

	require.Equal(t, exchangev1.ExecutionType_EXECUTION_TYPE_NEW, next(t, seller).GetType())
	tr := next(t, seller)
	require.Equal(t, exchangev1.ExecutionType_EXECUTION_TYPE_TRADE, tr.GetType())
	require.True(t, tr.GetIsMaker())
	require.Equal(t, "s1", tr.GetClientOrderId())
	require.Equal(t, int64(100), tr.GetPrice())
	require.Equal(t, int64(4), tr.GetQuantity())
	require.Equal(t, int64(6), tr.GetLeavesQuantity())

	require.Equal(t, exchangev1.ExecutionType_EXECUTION_TYPE_NEW, next(t, buyer).GetType())
	tr = next(t, buyer)
	require.False(t, tr.GetIsMaker())
	require.Equal(t, "b1", tr.GetClientOrderId())
	require.Equal(t, int64(0), tr.GetLeavesQuantity())
}

func TestCancelFlowAndIdempotency(t *testing.T) {
	e := start(t, pipeline.Config{})
	ctx := context.Background()
	st := e.subscribe(t, 1)
	_, err := e.client.PlaceOrder(ctx, place("c1", 1, exchangev1.Side_SIDE_BUY, 100, 8))
	require.NoError(t, err)

	cancel := &exchangev1.CancelOrderRequest{ClientOrderId: "c1", AccountId: 1, Symbol: symbol}
	r1, err := e.client.CancelOrder(ctx, cancel)
	require.NoError(t, err)
	require.Equal(t, exchangev1.OrderStatus_ORDER_STATUS_CANCELED, r1.GetStatus())
	require.Equal(t, int64(8), r1.GetCanceledQuantity())

	r2, err := e.client.CancelOrder(ctx, cancel) // retry returns the original
	require.NoError(t, err)
	require.Equal(t, exchangev1.OrderStatus_ORDER_STATUS_CANCELED, r2.GetStatus())
	require.Equal(t, r1.GetSequence(), r2.GetSequence())

	require.Equal(t, exchangev1.ExecutionType_EXECUTION_TYPE_NEW, next(t, st).GetType())
	rep := next(t, st)
	require.Equal(t, exchangev1.ExecutionType_EXECUTION_TYPE_CANCELED, rep.GetType())
	require.Equal(t, "c1", rep.GetClientOrderId())
}

func TestCancelUnknownOrWrongAccount(t *testing.T) {
	e := start(t, pipeline.Config{})
	ctx := context.Background()
	_, err := e.client.PlaceOrder(ctx, place("w1", 1, exchangev1.Side_SIDE_BUY, 100, 1))
	require.NoError(t, err)

	for _, req := range []*exchangev1.CancelOrderRequest{
		{ClientOrderId: "nope", AccountId: 1, Symbol: symbol}, // never placed
		{ClientOrderId: "w1", AccountId: 2, Symbol: symbol},   // other account's key
	} {
		r, err := e.client.CancelOrder(ctx, req)
		require.NoError(t, err)
		require.Equal(t, exchangev1.OrderStatus_ORDER_STATUS_REJECTED, r.GetStatus())
		require.Equal(t, exchangev1.RejectReason_REJECT_REASON_UNKNOWN_ORDER, r.GetRejectReason())
	}
	// the order is still cancellable by its owner afterwards
	r, err := e.client.CancelOrder(ctx, &exchangev1.CancelOrderRequest{ClientOrderId: "w1", AccountId: 1, Symbol: symbol})
	require.NoError(t, err)
	require.Equal(t, exchangev1.OrderStatus_ORDER_STATUS_CANCELED, r.GetStatus())
}

func TestOverloadIsExplicitAndRetryable(t *testing.T) {
	e := start(t, pipeline.Config{InputSize: 2, MaxInFlight: 2})
	var wg sync.WaitGroup
	var mu sync.Mutex
	exhausted := 0
	for i := 0; i < 300; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := e.client.PlaceOrder(context.Background(), place("ov-"+string(rune('a'+i%26))+string(rune('a'+i/26)), 1, exchangev1.Side_SIDE_BUY, 100, 1))
			if status.Code(err) == codes.ResourceExhausted {
				mu.Lock()
				exhausted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	require.Positive(t, exhausted, "load must be shed with RESOURCE_EXHAUSTED, not dropped or blocked")

	// a shed key is not poisoned: once there is room the same key works
	require.Eventually(t, func() bool {
		_, err := e.client.PlaceOrder(context.Background(), place("ov-aa", 1, exchangev1.Side_SIDE_BUY, 100, 1))
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
}
