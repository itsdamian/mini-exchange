// Command bot sends synthetic order flow to the gateway: a market maker that
// re-quotes around a reference price every step, plus random taker orders.
//
// The reference price comes from a recorded file (default; with a fixed -seed
// two runs send the same orders) or, with -feed coinbase, from the live public
// Coinbase WebSocket ticker. The default price file is synthetic, see data/README.md.
//
// Accounts need funds when the gateway runs with risk: fund the maker and
// takers with risktool (and ledger deposit) first.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/itsdamian/mini-exchange/internal/bot"

	exchangev1 "github.com/itsdamian/mini-exchange/gen/exchange/v1"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "bot:", err)
		os.Exit(1)
	}
}

// loop replays a file source forever.
type loop struct {
	prices []float64
	i      int
}

func (l *loop) Next(context.Context) (float64, error) {
	p := l.prices[l.i%len(l.prices)]
	l.i++
	return p, nil
}
func (l *loop) Close() error { return nil }

func run() error {
	var (
		addr     = flag.String("addr", "localhost:50051", "gateway gRPC address")
		symbol   = flag.String("symbol", "BTC-USD", "symbol to trade")
		feed     = flag.String("feed", "file", "reference price: file or coinbase")
		prices   = flag.String("prices", "data/sample-prices.csv", "file feed: CSV or JSON price file")
		repeat   = flag.Bool("loop", false, "file feed: start over when the file ends instead of stopping")
		product  = flag.String("product", "BTC-USD", "coinbase feed: product id")
		seed     = flag.Uint64("seed", 1, "random seed: with a file feed, the same seed sends the same orders")
		interval = flag.Duration("interval", 100*time.Millisecond, "pause between steps (file feed)")
		steps    = flag.Int("steps", 0, "stop after this many steps (0: until the feed ends)")
		tick     = flag.Float64("tick", 0.01, "price units per tick")
		maker    = flag.Uint64("maker", 1, "market maker account id")
		takerLo  = flag.Uint64("taker-from", 2, "first taker account id")
		takerHi  = flag.Uint64("taker-to", 9, "last taker account id")
		levels   = flag.Int("levels", 3, "quote levels per side")
		half     = flag.Int64("half-spread", 5, "ticks from mid to the first level")
		maxQty   = flag.Int64("max-qty", 50, "orders are 1..max-qty units")
		prefix   = flag.String("prefix", "bot", "client order id prefix; change it when restarting against a live gateway so ids do not collide")
	)
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "bot")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var src bot.PriceSource
	switch *feed {
	case "file":
		fs, err := bot.LoadFile(*prices)
		if err != nil {
			return err
		}
		src = fs
		if *repeat {
			var all []float64
			for {
				p, err := fs.Next(ctx)
				if err == io.EOF {
					break
				}
				all = append(all, p)
			}
			src = &loop{prices: all}
		}
		log.Info("replaying recorded prices", "file", *prices, "prices", fs.Len(), "seed", *seed)
	case "coinbase":
		cs, err := bot.NewCoinbaseSource(ctx, bot.CoinbaseURL, *product, nil)
		if err != nil {
			return fmt.Errorf("coinbase feed: %w", err)
		}
		src = cs
		*interval = 0 // paced by the feed
		log.Info("following the live Coinbase ticker", "product", *product)
	default:
		return fmt.Errorf("unknown -feed %q (file or coinbase)", *feed)
	}
	defer src.Close()

	var takers []uint64
	for a := *takerLo; a <= *takerHi; a++ {
		takers = append(takers, a)
	}
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	r := &bot.Runner{
		Client: exchangev1.NewOrderGatewayServiceClient(conn), Symbol: *symbol, Interval: *interval, MaxSteps: *steps,
		Planner: bot.NewPlanner(bot.Config{Seed: *seed, Prefix: *prefix, MakerAccount: *maker, TakerAccounts: takers, TickSize: *tick,
			Levels: *levels, HalfSpread: *half, MaxQty: *maxQty}),
		OnError: func(err error) { log.Warn("request failed", "err", err) },
	}
	st, err := r.Run(ctx, src)
	log.Info("bot finished", "steps", st.Steps, "placed", st.Placed, "cancelled", st.Cancelled, "rejected", st.Rejected, "errors", st.Errors)
	if err != nil && ctx.Err() == nil {
		return err
	}
	return nil
}
