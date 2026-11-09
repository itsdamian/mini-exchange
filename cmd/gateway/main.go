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
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"google.golang.org/grpc"

	"github.com/itsdamian/mini-exchange/internal/engine"
	"github.com/itsdamian/mini-exchange/internal/eventlog"
	"github.com/itsdamian/mini-exchange/internal/gateway"
	"github.com/itsdamian/mini-exchange/internal/httpserver"
	"github.com/itsdamian/mini-exchange/internal/pipeline"
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
	)
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "gateway")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	bookCfg := engine.Config{MaxOrders: *maxOrders, MaxLevels: 4096}
	pcfg := pipeline.Config{Book: bookCfg, InputSize: *inputSize, MaxInFlight: *inFlight, Registerer: reg}

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

	p := pipeline.New(pcfg)
	gw := gateway.New(*symbol, p, reg)

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
	if pub != nil {
		if err := pub.Close(shutdownCtx); err != nil {
			return fmt.Errorf("flush event log: %w", err)
		}
		follower.Close()
	}
	return httpSrv.Shutdown(shutdownCtx)
}
