// Command gateway is the order gateway and engine host: a gRPC API in front of
// the matching engine pipeline, with the event log (Kafka), snapshots and
// recovery, plus health probes and Prometheus metrics over HTTP.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof on http.DefaultServeMux, served only with -pprof-addr
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"

	"github.com/itsdamian/mini-exchange/internal/engine"
	"github.com/itsdamian/mini-exchange/internal/eventlog"
	"github.com/itsdamian/mini-exchange/internal/gateway"
	"github.com/itsdamian/mini-exchange/internal/httpserver"
	"github.com/itsdamian/mini-exchange/internal/pipeline"
	"github.com/itsdamian/mini-exchange/internal/risk"
)

func main() {
	if err := run(); err != nil {
		slog.Error("gateway failed", "err", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		httpAddr  = flag.String("http", ":8080", "address for /healthz, /readyz and /metrics")
		grpcAddr  = flag.String("grpc", ":50051", "address for the gRPC order API")
		symbol    = flag.String("symbol", "BTC-USD", "the single symbol this instance trades")
		maxOrders = flag.Int("max-orders", 1<<17, "preallocated resting-order capacity of the book")
		inputSize = flag.Int("input-ring", 1<<16, "input ring capacity (power of two)")
		inFlight  = flag.Int("max-in-flight", 1<<15, "concurrent requests admitted")
		kafka     = flag.String("kafka", "", "comma-separated Kafka brokers; empty runs without an event log (state is lost on exit)")
		dataDir   = flag.String("data-dir", "./data", "directory for book snapshots")
		snapEvery = flag.Uint64("snapshot-every", 100_000, "write a snapshot after this many acknowledged events (0 disables)")
		linger    = flag.Duration("kafka-linger", 5*time.Millisecond, "Kafka producer linger")
		highWater = flag.Int("log-high-watermark", 8192, "unacknowledged batches at which new orders are refused")
		replicas  = flag.Int("kafka-replication", 1, "replication factor for a newly created event topic")

		redisAddr = flag.String("redis", "", "Redis address for pre-trade risk; empty runs without risk checks (any order is admitted)")
		maxQty    = flag.Int64("risk-max-qty", 1_000_000, "largest order quantity (units)")
		maxPrice  = flag.Int64("risk-max-price", 1_000_000, "largest limit price (ticks); a market buy reserves funds at this price")
		maxOpen   = flag.Int64("risk-max-open", 1000, "open orders per account (0 disables)")
		rateLimit = flag.Int64("risk-rate", 1000, "orders per second per account, token bucket refill (0 disables)")
		rateBurst = flag.Int64("risk-burst", 2000, "token bucket capacity per account")
		pprofAddr = flag.String("pprof-addr", "", "serve net/http/pprof on this address (empty: off; use localhost:PORT, it is unauthenticated)")
		useChans  = flag.Bool("queue-channels", false, "experiment: buffered channels instead of the lock-free rings between the gateway, engine and dispatcher")
		lockEng   = flag.Bool("lock-engine-thread", false, "pin the engine goroutine to an OS thread (runtime.LockOSThread)")
		riskTO    = flag.Duration("risk-timeout", 100*time.Millisecond, "timeout of the pre-trade Redis call")
	)
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "gateway")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	bookCfg := engine.Config{MaxOrders: *maxOrders, MaxLevels: 4096}
	pcfg := pipeline.Config{Book: bookCfg, InputSize: *inputSize, MaxInFlight: *inFlight, Registerer: reg, LockEngineThread: *lockEng, UseChannels: *useChans}
	if *pprofAddr != "" {
		go func() { // profiling endpoints on their own listener, never on the public ports
			log.Info("pprof listening", "addr", *pprofAddr)
			_ = http.ListenAndServe(*pprofAddr, nil) //nolint:gosec // opt-in, local
		}()
	}

	var (
		pub      *eventlog.Publisher
		follower *eventlog.Follower
	)
	if *kafka != "" {
		brokers := strings.Split(*kafka, ",")
		store := &eventlog.SnapshotStore{Dir: *dataDir, Symbol: *symbol}

		producer, err := eventlog.NewProducerClient(brokers, *linger, 1<<16)
		if err != nil {
			return err
		}
		setupCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		if err := eventlog.EnsureTopic(setupCtx, producer, *symbol, int16(*replicas)); err != nil {
			cancel()
			return err
		}
		book, st, err := eventlog.Recover(setupCtx, brokers, *symbol, store, bookCfg, log)
		cancel()
		if err != nil {
			return fmt.Errorf("recovery: %w", err)
		}
		log.Info("recovered",
			"from_snapshot", st.FromSnapshot, "snapshot_seq", st.SnapshotSeq,
			"replayed_batches", st.Batches, "replayed_events", st.Events,
			"last_seq", st.LastSeq, "book_hash", fmt.Sprintf("%016x", st.BookHash),
			"resting_orders", book.Len(), "log_end_offset", st.LogEnd,
			"snapshot_load_ms", st.SnapshotLoad.Milliseconds(), "replay_ms", st.Replay.Milliseconds(), "total_ms", st.Total.Milliseconds())

		follower = eventlog.NewFollower(*symbol, book.Clone(bookCfg), st.LastOffset, store, *snapEvery, log)
		pub = eventlog.NewPublisher(producer, eventlog.PublisherConfig{
			Symbol: *symbol, HighWatermark: *highWater, QueueCapacity: *highWater + *inFlight + 1024,
			OnDurable: follower.OnDurable, Registerer: reg, Logger: log,
		})
		pcfg.Restore = &pipeline.Restored{Book: book, NextOrderID: book.MaxOrderID() + 1}
		pcfg.Log = pub
	} else {
		log.Warn("no -kafka given: running WITHOUT an event log, all state is lost on exit")
	}

	var checker *risk.Checker
	if *redisAddr != "" {
		rdb := redis.NewClient(&redis.Options{Addr: *redisAddr, DialTimeout: time.Second, MaxRetries: 1})
		defer rdb.Close()
		setupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		var err error
		checker, err = risk.New(setupCtx, rdb, risk.Config{
			Symbol:  *symbol,
			Limits:  risk.Limits{MaxOrderQty: *maxQty, MaxPrice: *maxPrice, MaxOpenOrders: *maxOpen, RatePerSec: *rateLimit, Burst: *rateBurst},
			Timeout: *riskTO, Registerer: reg,
		})
		cancel()
		if err != nil {
			return fmt.Errorf("risk: %w", err)
		}
		if *kafka == "" {
			log.Warn("-redis without -kafka: the engine sequence restarts at 1 on every start but Redis remembers the last one it applied, so settlement would be skipped after a restart; clear the risk keys first (loadtest.sh does)")
		}
		pcfg.Risk = checker // only when enabled: a typed nil would defeat the nil check
		log.Info("pre-trade risk enabled", "redis", *redisAddr, "max_qty", *maxQty, "max_price", *maxPrice,
			"max_open", *maxOpen, "rate_per_sec", *rateLimit, "burst", *rateBurst)
	} else {
		log.Warn("no -redis given: running WITHOUT pre-trade risk, every order is admitted")
	}

	p := pipeline.New(pcfg)
	gw := gateway.New(*symbol, p, reg)
	if checker != nil {
		gw.WithRisk(checker)
	}

	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		return err
	}
	grpcSrv := grpc.NewServer(grpc.UnaryInterceptor(gw.UnaryInterceptor()))
	gw.Register(grpcSrv)
	httpSrv := httpserver.NewWithReady(*httpAddr, reg, func() error {
		if pub != nil && pub.Err() != nil {
			return pub.Err()
		}
		if follower != nil && follower.Err() != nil {
			return follower.Err()
		}
		return nil
	})

	errc := make(chan error, 2)
	go func() {
		log.Info("grpc listening", "addr", lis.Addr().String(), "symbol", *symbol)
		errc <- grpcSrv.Serve(lis)
	}()
	go func() {
		log.Info("http listening", "addr", *httpAddr)
		errc <- httpSrv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, grpc.ErrServerStopped) {
			return err
		}
		return nil
	case <-ctx.Done():
	}

	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Stop accepting RPCs and let in-flight ones finish, then drain the engine,
	// flush the event log, and write a final snapshot. Long-lived report
	// streams would block a graceful stop, so force-stop after a short grace.
	stopped := make(chan struct{})
	go func() { grpcSrv.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		grpcSrv.Stop()
	}
	if err := p.Stop(shutdownCtx); err != nil {
		return err
	}
	if checker != nil {
		if err := checker.Close(shutdownCtx); err != nil { // apply the queued settlements
			log.Error("risk settlement not fully applied", "err", err)
		}
	}
	if pub != nil {
		if err := pub.Close(shutdownCtx); err != nil {
			return fmt.Errorf("flush event log: %w", err)
		}
		follower.Close()
	}
	return httpSrv.Shutdown(shutdownCtx)
}
