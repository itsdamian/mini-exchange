// Command gateway is the order gateway: a gRPC API in front of the matching
// engine pipeline, plus health probes and Prometheus metrics over HTTP.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"google.golang.org/grpc"

	"github.com/itsdamian/mini-exchange/internal/engine"
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
	)
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "gateway")

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	p := pipeline.New(pipeline.Config{
		Book:       engine.Config{MaxOrders: *maxOrders, MaxLevels: 4096},
		InputSize:  *inputSize,
		Registerer: reg,
	})
	gw := gateway.New(*symbol, p, reg)

	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		return err
	}
	grpcSrv := grpc.NewServer(grpc.UnaryInterceptor(gw.UnaryInterceptor()))
	gw.Register(grpcSrv)
	httpSrv := httpserver.New(*httpAddr, reg)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Stop accepting RPCs and let in-flight ones finish, then drain the engine.
	// Long-lived report streams would block a graceful stop, so force-stop after the deadline.
	stopped := make(chan struct{})
	go func() { grpcSrv.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-shutdownCtx.Done():
		grpcSrv.Stop()
	}
	if err := p.Stop(shutdownCtx); err != nil {
		return err
	}
	return httpSrv.Shutdown(shutdownCtx)
}
