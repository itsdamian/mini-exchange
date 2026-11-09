// Command risktool seeds and audits the pre-trade risk balances in Redis.
//
//	risktool fund  -redis ADDR -from 1 -to 16 -base N -quote N   add funds to accounts
//	risktool audit -redis ADDR [-expect-base N -expect-quote N]  scan every account
//
// audit exits non-zero if any balance is negative or, when expectations are
// given, if available plus held does not add up to exactly the funded total
// (funds are conserved by settlement, so it must).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/itsdamian/mini-exchange/internal/risk"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: risktool fund|audit [flags]")
		os.Exit(2)
	}
	if err := run(os.Args[1], os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, "risktool:", err)
		os.Exit(1)
	}
}

func run(cmd string, args []string) error {
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	addr := fs.String("redis", "localhost:16379", "Redis address")
	from := fs.Uint64("from", 1, "fund: first account id")
	to := fs.Uint64("to", 16, "fund: last account id")
	base := fs.Int64("base", 0, "fund: base units to add per account")
	quote := fs.Int64("quote", 0, "fund: quote units to add per account")
	expBase := fs.Int64("expect-base", -1, "audit: expected total base (available + held)")
	expQuote := fs.Int64("expect-quote", -1, "audit: expected total quote (available + held)")
	_ = fs.Parse(args)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	rdb := redis.NewClient(&redis.Options{Addr: *addr})
	defer rdb.Close()
	c, err := risk.New(ctx, rdb, risk.Config{Symbol: "-", Limits: risk.Limits{MaxOrderQty: 1, MaxPrice: 1}})
	if err != nil {
		return err
	}
	defer func() { _ = c.Close(ctx) }()

	switch cmd {
	case "fund":
		for a := *from; a <= *to; a++ {
			if err := c.Deposit(ctx, a, *base, *quote); err != nil {
				return fmt.Errorf("account %d: %w", a, err)
			}
		}
		fmt.Printf("funded accounts %d..%d with base=%d quote=%d each\n", *from, *to, *base, *quote)
		return nil
	case "audit":
		a, err := c.Audit(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("accounts=%d base: avail=%d held=%d total=%d | quote: avail=%d held=%d total=%d | open=%d negative_accounts=%d\n",
			a.Accounts, a.BaseAvail, a.BaseHeld, a.BaseAvail+a.BaseHeld, a.QuoteAvail, a.QuoteHeld, a.QuoteAvail+a.QuoteHeld, a.Open, a.Negative)
		if a.Negative > 0 {
			return fmt.Errorf("%d accounts have a negative balance", a.Negative)
		}
		if *expBase >= 0 && a.BaseAvail+a.BaseHeld != *expBase {
			return fmt.Errorf("base not conserved: have %d, funded %d", a.BaseAvail+a.BaseHeld, *expBase)
		}
		if *expQuote >= 0 && a.QuoteAvail+a.QuoteHeld != *expQuote {
			return fmt.Errorf("quote not conserved: have %d, funded %d", a.QuoteAvail+a.QuoteHeld, *expQuote)
		}
		fmt.Println("audit ok")
		return nil
	}
	return fmt.Errorf("unknown command %q", cmd)
}
