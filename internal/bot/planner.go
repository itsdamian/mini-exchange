package bot

import (
	"fmt"
	"math"
	"math/rand/v2"
)

// Config shapes the synthetic flow. Everything the planner does is a function
// of this config and the sequence of reference prices: no wall clock, no
// map iteration, no goroutines. That is what makes two runs with the same
// price file and seed produce the same orders.
type Config struct {
	Seed          uint64
	Prefix        string   // client order id prefix (default "bot")
	MakerAccount  uint64   // quotes both sides (default 1)
	TakerAccounts []uint64 // random takers (default 2..9)
	TickSize      float64  // price units per tick (default 0.01)
	Levels        int      // quote levels per side (default 3)
	HalfSpread    int64    // ticks between mid and the first level (default 5)
	LevelStep     int64    // ticks between levels (default 3)
	MaxQty        int64    // quantities are 1..MaxQty (default 50)
	TakerAttempts int      // taker attempts per step (default 2)
	TakerProb     float64  // chance that an attempt trades (default 0.6)
	MarketProb    float64  // share of taker orders that are MARKET (default 0.3)
}

func (c *Config) defaults() {
	if c.Prefix == "" {
		c.Prefix = "bot"
	}
	if c.MakerAccount == 0 {
		c.MakerAccount = 1
	}
	if len(c.TakerAccounts) == 0 {
		for a := uint64(2); a <= 9; a++ {
			c.TakerAccounts = append(c.TakerAccounts, a)
		}
	}
	if c.TickSize <= 0 {
		c.TickSize = 0.01
	}
	if c.Levels <= 0 {
		c.Levels = 3
	}
	if c.HalfSpread <= 0 {
		c.HalfSpread = 5
	}
	if c.LevelStep <= 0 {
		c.LevelStep = 3
	}
	if c.MaxQty <= 0 {
		c.MaxQty = 50
	}
	if c.TakerAttempts <= 0 {
		c.TakerAttempts = 2
	}
	if c.TakerProb == 0 {
		c.TakerProb = 0.6
	}
	if c.MarketProb == 0 {
		c.MarketProb = 0.3
	}
}

// Action is one request for the gateway.
type Action struct {
	Cancel   bool
	ClientID string
	Account  uint64
	Buy      bool
	Market   bool
	IOC      bool
	Price    int64 // ticks, 0 for MARKET
	Qty      int64
}

// Planner turns reference prices into actions. It is deterministic.
type Planner struct {
	cfg    Config
	rng    *rand.Rand
	step   uint64
	quotes []string // client ids of the maker's resting quotes, to cancel next step
}

// NewPlanner returns a Planner for the config.
func NewPlanner(cfg Config) *Planner {
	cfg.defaults()
	return &Planner{cfg: cfg, rng: rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9e3779b97f4a7c15))}
}

// Step returns the actions for one reference price: cancel the previous
// quotes, quote Levels levels each side around the new mid, then try a few
// random taker orders that cross the spread.
func (p *Planner) Step(price float64) []Action {
	c := &p.cfg
	p.step++
	mid := int64(math.Round(price / c.TickSize))
	if min := c.HalfSpread + int64(c.Levels)*c.LevelStep + 1; mid < min {
		mid = min
	}
	qty := func() int64 { return 1 + p.rng.Int64N(c.MaxQty) }

	acts := make([]Action, 0, len(p.quotes)+2*c.Levels+c.TakerAttempts)
	for _, id := range p.quotes {
		acts = append(acts, Action{Cancel: true, ClientID: id, Account: c.MakerAccount})
	}
	p.quotes = p.quotes[:0]
	for i := 0; i < c.Levels; i++ {
		off := c.HalfSpread + int64(i)*c.LevelStep
		for _, buy := range []bool{true, false} {
			price := mid + off
			side := "a"
			if buy {
				price, side = mid-off, "b"
			}
			id := fmt.Sprintf("%sm%d%s%d", c.Prefix, p.step, side, i)
			acts = append(acts, Action{ClientID: id, Account: c.MakerAccount, Buy: buy, Price: price, Qty: qty()})
			p.quotes = append(p.quotes, id)
		}
	}
	reach := c.HalfSpread + int64(c.Levels)*c.LevelStep
	for j := 0; j < c.TakerAttempts; j++ {
		if p.rng.Float64() >= c.TakerProb {
			continue
		}
		a := Action{
			ClientID: fmt.Sprintf("%st%d.%d", c.Prefix, p.step, j),
			Account:  c.TakerAccounts[p.rng.IntN(len(c.TakerAccounts))],
			Buy:      p.rng.IntN(2) == 0,
			Qty:      qty(),
		}
		if p.rng.Float64() < c.MarketProb {
			a.Market = true
		} else {
			a.IOC = true
			a.Price = mid + reach // crosses every level the maker quotes
			if !a.Buy {
				a.Price = mid - reach
			}
		}
		acts = append(acts, a)
	}
	return acts
}
