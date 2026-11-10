// Command marketdata serves depth and recent trades of one symbol over
// WebSocket (/ws) from the Kafka event log, with conflation and slow-client
// disconnection. It never talks to the engine.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/itsdamian/mini-exchange/internal/engine"
	"github.com/itsdamian/mini-exchange/internal/marketdata"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "marketdata:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		kafka      = flag.String("kafka", "localhost:19092", "Kafka bootstrap brokers, comma separated")
		symbol     = flag.String("symbol", "BTC-USD", "symbol whose event log to follow")
		httpAddr   = flag.String("http", ":8083", "listen address: /ws, /metrics, /healthz, /readyz")
		depth      = flag.Int("depth", 10, "price levels per side")
		trades     = flag.Int("trades", 50, "recent trades per frame")
		interval   = flag.Duration("interval", 100*time.Millisecond, "conflation interval: at most one frame per client per interval")
		writeTO    = flag.Duration("write-timeout", time.Second, "disconnect a client that does not accept a frame within this time")
		maxClients = flag.Int("max-clients", 5000, "connections beyond this are refused with 503")
		maxOrders  = flag.Int("max-orders", 1<<20, "sizing of the rebuilt book: resting orders")
	)
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "marketdata")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	setup, cancel := context.WithTimeout(ctx, 5*time.Minute)
	c, err := marketdata.NewConsumer(setup, marketdata.ConsumerConfig{
		Brokers: strings.Split(*kafka, ","), Symbol: *symbol,
		Book: engine.Config{MaxOrders: *maxOrders, MaxLevels: 4096},
		Feed: marketdata.FeedConfig{Depth: *depth, KeepTrades: *trades}, Registerer: reg, Log: log,
	})
	cancel()
	if err != nil {
		return err
	}
	srv := marketdata.NewServer(c.Feed(), marketdata.ServerConfig{Interval: *interval, WriteTimeout: *writeTO, MaxClients: *maxClients, Registerer: reg})

	mux := http.NewServeMux()
	mux.Handle("/ws", srv.Handler())
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	ok := func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) }
	mux.HandleFunc("GET /healthz", ok)
	mux.HandleFunc("GET /readyz", ok)
	httpSrv := &http.Server{Addr: *httpAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	stopFeed := make(chan struct{})
	go c.Feed().Run(stopFeed, *interval)
	errc := make(chan error, 2)
	go func() { errc <- httpSrv.ListenAndServe() }()
	go func() { errc <- c.Run(ctx) }()
	log.Info("market data running", "symbol", *symbol, "http", *httpAddr, "depth", *depth, "interval", interval.String())

	select {
	case err := <-errc:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			close(stopFeed)
			return err
		}
	case <-ctx.Done():
	}
	close(stopFeed)
	sc, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return httpSrv.Shutdown(sc)
}
