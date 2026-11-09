// Command ledger is the double-entry ledger service and its tools.
//
//	ledger run       -pg DSN -kafka ADDR -symbol S   settle trades from the event log into PostgreSQL
//	ledger deposit   -pg DSN -symbol S -account N [-to M] -base N -quote N [-ref ID]
//	ledger reconcile -pg DSN -kafka ADDR -symbol S [-redis ADDR] [-wait 30s]
//
// reconcile prints a JSON report and exits non-zero if it found a discrepancy.
// The fee flags of run and reconcile must match, they are used to recompute.
package main

import (
	"context"
	"encoding/json"
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
	"github.com/redis/go-redis/v9"

	"github.com/itsdamian/mini-exchange/internal/httpserver"
	"github.com/itsdamian/mini-exchange/internal/ledger"
	"github.com/itsdamian/mini-exchange/internal/risk"
)

const defaultDSN = "postgres://exchange:exchange-local-dev@localhost:15432/exchange?sslmode=disable" // local development only

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: ledger run|deposit|reconcile [flags]")
		os.Exit(2)
	}
	if err := run(os.Args[1], os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, "ledger:", err)
		os.Exit(1)
	}
}

func run(cmd string, args []string) error {
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	var (
		dsn      = fs.String("pg", defaultDSN, "PostgreSQL connection string")
		symbol   = fs.String("symbol", "BTC-USD", "symbol, BASE-QUOTE")
		kafka    = fs.String("kafka", "localhost:19092", "run, reconcile: Kafka bootstrap brokers, comma separated")
		group    = fs.String("group", "mx-ledger", "run: consumer group (a new group replays the log from the start)")
		httpAddr = fs.String("http", ":8082", "run: metrics and health listen address")
		replicas = fs.Int("replicas", 1, "run: replication factor of the dead-letter topic")
		takerBps = fs.Int64("taker-bps", 0, "run, reconcile: taker fee in basis points of the notional, charged in the quote asset")
		makerBps = fs.Int64("maker-bps", 0, "run, reconcile: maker fee in basis points")
		redisAdr = fs.String("redis", "", "reconcile: Redis address to compare balances with (optional)")
		wait     = fs.Duration("wait", 0, "reconcile: keep re-checking this long while discrepancies remain (the ledger and Redis settle asynchronously)")
		account  = fs.Uint64("account", 0, "deposit: account id (first one with -to)")
		toAcct   = fs.Uint64("to", 0, "deposit: last account id, to fund a range (each account gets the amounts)")
		base     = fs.Int64("base", 0, "deposit: base units")
		quote    = fs.Int64("quote", 0, "deposit: quote units")
		ref      = fs.String("ref", "", "deposit: idempotency reference (repeating it does nothing); default is unique")
	)
	_ = fs.Parse(args)
	fees := ledger.Fees{TakerBps: *takerBps, MakerBps: *makerBps}
	brokers := strings.Split(*kafka, ",")

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "ledger")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	openCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	store, err := ledger.Open(openCtx, *dsn)
	cancel()
	if err != nil {
		return err
	}
	defer store.Close()

	switch cmd {
	case "run":
		return serve(ctx, log, store, ledger.ConsumerConfig{
			Brokers: brokers, Symbol: *symbol, Group: *group, Fees: fees, Replication: int16(*replicas), Log: log,
		}, *httpAddr)

	case "deposit":
		a, err := ledger.SplitSymbol(*symbol)
		if err != nil {
			return err
		}
		last := *toAcct
		if last < *account {
			last = *account
		}
		for acct := *account; acct <= last; acct++ {
			id := *ref
			if id == "" {
				id = fmt.Sprintf("cli-%d-%d", acct, time.Now().UnixNano())
			} else if last > *account {
				id = fmt.Sprintf("%s-%d", id, acct)
			}
			p, err := ledger.DepositPosting(id, *symbol, a, acct, *base, *quote)
			if err != nil {
				return err
			}
			n, err := store.Apply(ctx, []ledger.Posting{p})
			if err != nil {
				return err
			}
			fmt.Printf("deposit %s: applied=%t account=%d base=%d quote=%d\n", id, n == 1, acct, *base, *quote)
		}
		return nil

	case "reconcile":
		cfg := ledger.ReconcileConfig{Brokers: brokers, Symbol: *symbol, Fees: fees, Wait: *wait}
		if *redisAdr != "" {
			rdb := redis.NewClient(&redis.Options{Addr: *redisAdr})
			defer rdb.Close()
			c, err := risk.New(ctx, rdb, risk.Config{Symbol: *symbol, Limits: risk.Limits{MaxOrderQty: 1, MaxPrice: 1}})
			if err != nil {
				return err
			}
			defer func() { _ = c.Close(ctx) }()
			cfg.Redis = c
		}
		rep, err := store.Reconcile(ctx, cfg)
		if err != nil {
			return err
		}
		out, _ := json.MarshalIndent(rep, "", "  ")
		fmt.Println(string(out))
		if !rep.OK() {
			return fmt.Errorf("%d discrepancies", len(rep.Discrepancies))
		}
		return nil
	}
	return fmt.Errorf("unknown command %q", cmd)
}

func serve(ctx context.Context, log *slog.Logger, store *ledger.Store, cfg ledger.ConsumerConfig, httpAddr string) error {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	cfg.Registerer = reg

	setupCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	c, err := ledger.NewConsumer(setupCtx, store, cfg)
	cancel()
	if err != nil {
		return err
	}
	defer c.Close()

	srv := httpserver.NewWithReady(httpAddr, reg, func() error {
		pc, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		return store.Pool().Ping(pc)
	})
	errc := make(chan error, 2)
	go func() { errc <- srv.ListenAndServe() }()
	go func() { errc <- c.Run(ctx) }()
	log.Info("ledger running", "symbol", cfg.Symbol, "group", cfg.Group, "http", httpAddr, "taker_bps", cfg.Fees.TakerBps, "maker_bps", cfg.Fees.MakerBps)

	select {
	case err := <-errc:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
	}
	log.Info("shutting down")
	sc, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(sc)
}
