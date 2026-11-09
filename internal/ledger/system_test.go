package ledger_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/itsdamian/mini-exchange/internal/eventlog"
	"github.com/itsdamian/mini-exchange/internal/eventlog/kafkatest"
	"github.com/itsdamian/mini-exchange/internal/gateway"
	"github.com/itsdamian/mini-exchange/internal/ledger"
	"github.com/itsdamian/mini-exchange/internal/ledger/pgtest"
	"github.com/itsdamian/mini-exchange/internal/pipeline"
	"github.com/itsdamian/mini-exchange/internal/risk"
	"github.com/itsdamian/mini-exchange/internal/risk/redistest"

	exchangev1 "github.com/itsdamian/mini-exchange/gen/exchange/v1"
)

// The whole path: gRPC gateway -> Redis risk -> engine -> Kafka event log ->
// ledger consumer -> PostgreSQL, with PostgreSQL stopped and restarted while
// orders keep arriving. Afterwards the ledger must agree with the event log
// and with Redis (FR-6 acceptance: zero discrepancies after chaos).
func TestWholePathWithPostgresOutageReconcilesToZero(t *testing.T) {
	const symbol = "SYS-USD"
	fees := ledger.Fees{TakerBps: 2, MakerBps: 1}
	ctx := context.Background()
	kb := kafkatest.Shared(t)
	pg := pgtest.Shared(t)
	rsrv := redistest.Shared(t)

	// Redis risk
	rdb := redis.NewClient(&redis.Options{Addr: rsrv.Addr, DialTimeout: time.Second})
	t.Cleanup(func() { _ = rdb.Close() })
	checker, err := risk.New(ctx, rdb, risk.Config{
		Symbol: symbol, Registerer: prometheus.NewRegistry(), Timeout: 5 * time.Second,
		Limits: risk.Limits{MaxOrderQty: 100, MaxPrice: 1_000, MaxOpenOrders: 1000},
	})
	require.NoError(t, err)

	// Kafka event log
	producer, err := eventlog.NewProducerClient(kb.Brokers(), time.Millisecond, 1<<16)
	require.NoError(t, err)
	t.Cleanup(producer.Close)
	require.NoError(t, eventlog.EnsureTopic(ctx, producer, symbol, 1))
	pub := eventlog.NewPublisher(producer, eventlog.PublisherConfig{Symbol: symbol, HighWatermark: 50_000, QueueCapacity: 60_000})

	// Engine pipeline and gateway
	p := pipeline.New(pipeline.Config{Log: pub, Risk: checker})
	gw := gateway.New(symbol, p, nil).WithRisk(checker)
	lis := bufconn.Listen(1 << 20)
	g := grpc.NewServer(grpc.UnaryInterceptor(gw.UnaryInterceptor()))
	gw.Register(g)
	go func() { _ = g.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	client := exchangev1.NewOrderGatewayServiceClient(conn)
	var once sync.Once
	shutdown := func() {
		once.Do(func() {
			_ = conn.Close()
			g.GracefulStop()
			c, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			require.NoError(t, p.Stop(c))
			require.NoError(t, pub.Close(c))
			require.NoError(t, checker.Close(c))
		})
	}
	t.Cleanup(shutdown)

	// Ledger and the same funds in Redis
	store := openStore(t)
	accts := make([]uint64, 8)
	a, _ := ledger.SplitSymbol(symbol)
	const base, quote = int64(100_000), int64(100_000_000)
	for i := range accts {
		accts[i] = 7000 + uint64(i)
		require.NoError(t, checker.Deposit(ctx, accts[i], base, quote))
		dp, err := ledger.DepositPosting(fmt.Sprintf("sys-%d", accts[i]), symbol, a, accts[i], base, quote)
		require.NoError(t, err)
		_, err = store.Apply(ctx, []ledger.Posting{dp})
		require.NoError(t, err)
	}
	rt := &topicRig{t: t, symbol: symbol, brokers: kb.Brokers()}
	c := startConsumer(t, store, rt, "g-"+symbol, fees)

	// Orders, with the database down for the middle third.
	rng := rand.New(rand.NewPCG(5, 6))
	type ord struct {
		acct uint64
		cid  string
	}
	var open []ord
	const orders = 4000
	for i := range orders {
		if i == orders/3 {
			require.Eventually(t, func() bool { n, _ := store.TradeCount(ctx, symbol); return n > 0 },
				time.Minute, 20*time.Millisecond, "settle something before the outage")
			require.NoError(t, pg.Stop(ctx))
		}
		if i == 2*orders/3 {
			// keep the database down until the consumer has actually hit it
			require.Eventually(t, func() bool { return c.counter(t, "mx_ledger_retries_total") > 0 },
				time.Minute, 20*time.Millisecond, "the consumer must run into the outage")
			require.NoError(t, pg.Restart(ctx))
		}
		acct := accts[rng.IntN(len(accts))]
		cid := fmt.Sprint("s", i)
		if rng.IntN(10) < 2 && len(open) > 0 {
			j := rng.IntN(len(open))
			_, err := client.CancelOrder(ctx, &exchangev1.CancelOrderRequest{ClientOrderId: open[j].cid, AccountId: open[j].acct, Symbol: symbol})
			require.NoError(t, err)
			open = append(open[:j], open[j+1:]...)
			continue
		}
		req := &exchangev1.PlaceOrderRequest{
			ClientOrderId: cid, AccountId: acct, Symbol: symbol, Side: exchangev1.Side_SIDE_BUY,
			Type: exchangev1.OrderType_ORDER_TYPE_LIMIT, TimeInForce: exchangev1.TimeInForce_TIME_IN_FORCE_GTC,
			Price: 480 + rng.Int64N(41), Quantity: 1 + rng.Int64N(50),
		}
		if rng.IntN(2) == 0 {
			req.Side = exchangev1.Side_SIDE_SELL
		}
		if rng.IntN(6) == 0 {
			req.TimeInForce = exchangev1.TimeInForce_TIME_IN_FORCE_IOC
		}
		r, err := client.PlaceOrder(ctx, req)
		require.NoError(t, err)
		if r.Status == exchangev1.OrderStatus_ORDER_STATUS_ACCEPTED {
			open = append(open, ord{acct, cid})
		}
	}
	for _, o := range open {
		_, err := client.CancelOrder(ctx, &exchangev1.CancelOrderRequest{ClientOrderId: o.cid, AccountId: o.acct, Symbol: symbol})
		require.NoError(t, err)
	}
	shutdown() // drain the engine, flush the log, apply the Redis settlements

	require.Positive(t, c.counter(t, "mx_ledger_retries_total"), "the outage must have been felt")
	rcCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	rep, err := store.Reconcile(rcCtx, ledger.ReconcileConfig{Brokers: kb.Brokers(), Symbol: symbol, Fees: fees, Redis: checker, Wait: 90 * time.Second})
	require.NoError(t, err)
	require.True(t, rep.OK(), "discrepancies after chaos: %+v", rep.Discrepancies)
	require.Greater(t, rep.LogTrades, 100)
	require.Equal(t, rep.LogTrades, rep.LedgerTrades)
	require.Equal(t, len(accts), rep.RedisChecked)
	require.Zero(t, c.counter(t, "mx_ledger_dead_letter_total"))
}
