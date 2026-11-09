// Package risk implements pre-trade risk in Redis: balances, atomic fund
// reservation, order limits and a per-account token bucket, all in Lua so each
// decision is one atomic round trip.
//
// Flow of one order:
//
//  1. The gateway calls Reserve before the engine. It either holds the funds
//     (buy: qty*price quote; sell: qty base) or returns a typed reject reason.
//  2. The engine runs. The pipeline's dispatcher reports every outcome to the
//     Settler as an Op: bind the reservation to the order id on accept, consume
//     it on fills, release the rest on cancel, release it all on reject.
//  3. The Settler applies the ops to Redis asynchronously and in event order,
//     retrying on failure. Each op carries the engine event sequence, and the
//     scripts skip a sequence they have already applied, so retries are safe.
//
// Nothing here runs on the matcher path.
package risk

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"

	"github.com/itsdamian/mini-exchange/internal/engine"
)

// MaxBalance bounds any single account balance (available plus held) per
// asset. Redis Lua numbers are doubles, exact below 2^53; this leaves room for
// sums.
const MaxBalance = int64(1) << 52

// Limits are the pre-trade limits. Zero RatePerSec disables the rate limit and
// zero MaxOpenOrders disables the open-order limit.
type Limits struct {
	MaxOrderQty   int64 // largest quantity of one order (units)
	MaxPrice      int64 // largest accepted limit price (ticks); also the reserve price of a market buy
	MaxOpenOrders int64 // open orders per account
	RatePerSec    int64 // token bucket refill, orders per second per account
	Burst         int64 // token bucket capacity
}

func (l Limits) validate() error {
	switch {
	case l.MaxOrderQty <= 0 || l.MaxPrice <= 0:
		return errors.New("risk: MaxOrderQty and MaxPrice must be positive")
	case l.MaxOrderQty > MaxBalance/l.MaxPrice:
		return fmt.Errorf("risk: MaxOrderQty*MaxPrice must not exceed %d", MaxBalance)
	case l.MaxOpenOrders < 0 || l.RatePerSec < 0 || l.Burst < 0:
		return errors.New("risk: limits must not be negative")
	case l.RatePerSec > 0 && l.Burst == 0:
		return errors.New("risk: Burst must be positive when RatePerSec is set")
	}
	return nil
}

// Reason is the outcome of a reservation.
type Reason uint8

// Reservation outcomes.
const (
	OK Reason = iota
	RateLimited
	OrderTooLarge
	TooManyOpenOrders
	InsufficientFunds
	InvalidPrice
	InvalidQuantity
)

func (r Reason) String() string {
	return [...]string{"ok", "rate_limited", "order_too_large", "too_many_open_orders",
		"insufficient_funds", "invalid_price", "invalid_quantity"}[r]
}

// Request describes an order to reserve funds for.
type Request struct {
	Account uint64
	Client  string // client order id, the reservation key until the engine accepts the order
	Side    engine.Side
	Type    engine.OrderType
	Price   int64
	Qty     int64
}

// Balance is one account's balances in base and quote units.
type Balance struct {
	BaseAvail, BaseHeld, QuoteAvail, QuoteHeld int64
	Open                                       int64 // orders holding a reservation
}

// Config configures a Checker.
type Config struct {
	Symbol     string // the symbol whose engine sequence the settlement dedup follows (required)
	Limits     Limits
	Timeout    time.Duration // per Redis call on the order path (default 100 ms)
	Clock      func() time.Time
	Registerer prometheus.Registerer
	// Settler tuning; zero values get defaults.
	QueueSize     int // buffered ops (default 1<<20)
	HighWatermark int // queued ops at which Backlogged reports true (default QueueSize/2)
	MaxBatch      int // ops per Redis round trip (default 256)
}

// Checker is the Redis-backed risk engine. Create with New, stop with Close.
type Checker struct {
	rdb     redis.UniversalClient
	lim     Limits
	symbol  string
	timeout time.Duration
	clock   func() time.Time

	reserveTime *prometheus.HistogramVec
	rejects     *prometheus.CounterVec

	st *settler
}

// New validates cfg, loads the scripts and starts the settler.
func New(ctx context.Context, rdb redis.UniversalClient, cfg Config) (*Checker, error) {
	if err := cfg.Limits.validate(); err != nil {
		return nil, err
	}
	if cfg.Symbol == "" {
		return nil, errors.New("risk: Symbol is required")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 100 * time.Millisecond
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	reg := cfg.Registerer
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	c := &Checker{
		rdb: rdb, lim: cfg.Limits, symbol: cfg.Symbol, timeout: cfg.Timeout, clock: cfg.Clock,
		reserveTime: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "mx_risk_reserve_seconds",
			Help:    "Latency of the pre-trade reservation call to Redis, by result (error = Redis unavailable).",
			Buckets: prometheus.ExponentialBuckets(2e-5, 2, 16), // 20 us .. ~0.65 s
		}, []string{"result"}),
		rejects: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "mx_risk_rejects_total", Help: "Orders rejected by pre-trade risk, by reason.",
		}, []string{"reason"}),
	}
	if err := c.loadScripts(ctx); err != nil {
		return nil, fmt.Errorf("risk: load scripts: %w", err)
	}
	c.st = newSettler(c, cfg, reg)
	return c, nil
}

func (c *Checker) loadScripts(ctx context.Context) error {
	for _, s := range allScripts {
		if err := s.Load(ctx, c.rdb).Err(); err != nil {
			return err
		}
	}
	return nil
}

// Close stops the settler after applying what is queued, until ctx expires.
func (c *Checker) Close(ctx context.Context) error { return c.st.close(ctx) }

// Backlogged reports whether settlement is far enough behind that new orders
// should be refused.
func (c *Checker) Backlogged() bool { return c.st.backlogged() }

// Flush waits until every op enqueued so far has been applied (or ctx expires).
func (c *Checker) Flush(ctx context.Context) error { return c.st.flush(ctx) }

// Enqueue hands one engine outcome to the settler. It never blocks.
func (c *Checker) Enqueue(op Op) { c.st.enqueue(op) }

// seqKey holds the last engine event sequence applied for the account. It is
// per symbol because every symbol has its own sequence.
func (c *Checker) seqKey(a uint64) string { return tag(a) + ":seq:" + c.symbol }

func tag(account uint64) string { return "{" + strconv.FormatUint(account, 10) + "}" }

func balKey(a uint64) string { return tag(a) + ":bal" }
func rcKey(a uint64, cid string) string {
	return tag(a) + ":rc:" + cid
}

// Reserve decides whether the order may enter the engine and, if so, holds
// its funds. It returns a non-nil error only when Redis could not be asked;
// callers must then refuse the order (fail closed).
func (c *Checker) Reserve(ctx context.Context, r Request) (Reason, error) {
	switch {
	case r.Qty <= 0:
		return c.reject(InvalidQuantity), nil
	case r.Qty > c.lim.MaxOrderQty:
		return c.reject(OrderTooLarge), nil
	case r.Type == engine.Limit && (r.Price <= 0 || r.Price > c.lim.MaxPrice):
		return c.reject(InvalidPrice), nil
	}
	side, cost, px := "S", r.Qty, int64(1)
	if r.Side == engine.Buy {
		// A market buy has no price, so it reserves at MaxPrice, which is a true
		// upper bound on every execution price (no resting limit order can be
		// above it). The excess is released as it fills.
		px = r.Price
		if r.Type == engine.Market {
			px = c.lim.MaxPrice
		}
		side, cost = "B", r.Qty*px // bounded by Limits.validate
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	start := time.Now()
	code, err := reserveScript.Run(ctx, c.rdb,
		[]string{balKey(r.Account), rcKey(r.Account, r.Client)},
		c.clock().UnixMicro(), c.lim.RatePerSec, c.lim.Burst, c.lim.MaxOpenOrders, side, cost, px).Int()
	if err != nil {
		c.reserveTime.WithLabelValues("error").Observe(time.Since(start).Seconds())
		return 0, err
	}
	var res Reason
	switch code {
	case 0:
		res = OK
	case 1:
		res = RateLimited
	case 2:
		res = TooManyOpenOrders
	default:
		res = InsufficientFunds
	}
	c.reserveTime.WithLabelValues(res.String()).Observe(time.Since(start).Seconds())
	if res != OK {
		c.reject(res)
	}
	return res, nil
}

func (c *Checker) reject(r Reason) Reason {
	c.rejects.WithLabelValues(r.String()).Inc()
	return r
}

// Release gives back a reservation that never reached the engine (the order
// was refused before being enqueued). It is idempotent.
func (c *Checker) Release(ctx context.Context, account uint64, client string) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return settleScript.Run(ctx, c.rdb, []string{balKey(account), c.seqKey(account)},
		tag(account)+":", int(OpRelease), 0, client, 0, 0).Err()
}

// Deposit adds funds to an account. It is an administrative operation used to
// seed balances (there is no deposit flow in the spec) and is not atomic with
// respect to the MaxBalance check.
func (c *Checker) Deposit(ctx context.Context, account uint64, base, quote int64) error {
	if base < 0 || quote < 0 {
		return errors.New("risk: negative deposit")
	}
	b, err := c.Balance(ctx, account)
	if err != nil {
		return err
	}
	if b.BaseAvail+b.BaseHeld+base > MaxBalance || b.QuoteAvail+b.QuoteHeld+quote > MaxBalance {
		return fmt.Errorf("risk: balance would exceed %d", MaxBalance)
	}
	pipe := c.rdb.Pipeline()
	pipe.HIncrBy(ctx, balKey(account), "b_avail", base)
	pipe.HIncrBy(ctx, balKey(account), "q_avail", quote)
	_, err = pipe.Exec(ctx)
	return err
}

// Balance returns one account's balances.
func (c *Checker) Balance(ctx context.Context, account uint64) (Balance, error) {
	m, err := c.rdb.HGetAll(ctx, balKey(account)).Result()
	if err != nil {
		return Balance{}, err
	}
	return parseBalance(m), nil
}

func parseBalance(m map[string]string) Balance {
	n := func(k string) int64 { v, _ := strconv.ParseInt(m[k], 10, 64); return v }
	return Balance{BaseAvail: n("b_avail"), BaseHeld: n("b_held"), QuoteAvail: n("q_avail"), QuoteHeld: n("q_held"), Open: n("open")}
}

// Audit is a scan of every account's balance.
type Audit struct {
	Accounts                                   int
	BaseAvail, BaseHeld, QuoteAvail, QuoteHeld int64
	Open                                       int64
	Negative                                   int // accounts with any negative balance field (must be 0)
}

// Audit scans all accounts. It is meant for tests, the risktool and
// reconciliation, not for the order path, and is only consistent when the
// system is quiescent.
func (c *Checker) Audit(ctx context.Context) (Audit, error) {
	var a Audit
	iter := c.rdb.Scan(ctx, 0, "{*}:bal", 500).Iterator()
	for iter.Next(ctx) {
		k := iter.Val()
		m, err := c.rdb.HGetAll(ctx, k).Result()
		if err != nil {
			return a, err
		}
		b := parseBalance(m)
		a.Accounts++
		a.BaseAvail += b.BaseAvail
		a.BaseHeld += b.BaseHeld
		a.QuoteAvail += b.QuoteAvail
		a.QuoteHeld += b.QuoteHeld
		a.Open += b.Open
		if b.BaseAvail < 0 || b.BaseHeld < 0 || b.QuoteAvail < 0 || b.QuoteHeld < 0 || b.Open < 0 {
			a.Negative++
		}
	}
	return a, iter.Err()
}
