// Command loadgen is an open-loop gRPC load generator for the gateway.
//
// Requests are issued on a fixed schedule (rate per second) regardless of how
// fast responses come back, and each latency is measured from the request's
// INTENDED send time, not from when it was actually sent. A slow response
// therefore cannot hide the requests that should have been sent meanwhile,
// which is how a closed-loop tool suffers coordinated omission. Latencies go
// into an HdrHistogram and are exported as p50/p90/p99/p99.9/max plus a full
// percentile distribution file.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/itsdamian/mini-exchange/internal/benchenv"
	"github.com/itsdamian/mini-exchange/internal/engine"
	"github.com/itsdamian/mini-exchange/internal/enginetest"

	exchangev1 "github.com/itsdamian/mini-exchange/gen/exchange/v1"
)

type percentiles struct {
	P50, P90, P99, P999, Max int64 // microseconds
}

type result struct {
	Timestamp string `json:"timestamp"`
	benchenv.Env
	Note           string         `json:"note,omitempty"`
	Target         string         `json:"target"`
	RatePerSec     int            `json:"rate_per_sec"`
	DurationSec    float64        `json:"measured_seconds"`
	WarmupSec      float64        `json:"warmup_seconds"`
	Conns          int            `json:"grpc_connections"`
	Seed           uint64         `json:"seed"`
	Sent           int            `json:"measured_requests"`
	Completed      int64          `json:"completed"`
	Errors         map[string]int `json:"errors_by_grpc_code"`
	Outcomes       map[string]int `json:"outcomes"`
	AchievedPerSec float64        `json:"achieved_completions_per_sec"`
	LatencyUs      percentiles    `json:"latency_us_from_intended_send_time"`
	SchedulerLagUs percentiles    `json:"scheduler_lag_us"`
	LatencyHdrFile string         `json:"latency_distribution_file,omitempty"`
}

func main() {
	var (
		addr     = flag.String("addr", "localhost:50051", "gateway gRPC address")
		symbol   = flag.String("symbol", "BTC-USD", "symbol")
		rate     = flag.Int("rate", 10_000, "requests per second (fixed arrival rate)")
		duration = flag.Duration("duration", 30*time.Second, "measured duration")
		warmup   = flag.Duration("warmup", 5*time.Second, "unmeasured warm-up before the measured window")
		conns    = flag.Int("conns", 4, "gRPC connections (round robin)")
		seed     = flag.Uint64("seed", 7, "workload seed")
		note     = flag.String("note", "", "free text recorded in the result (e.g. gateway settings)")
		out      = flag.String("out", "", "write the JSON result here (and a .hgrm distribution next to it)")
	)
	flag.Parse()
	if err := run(*addr, *symbol, *rate, *duration, *warmup, *conns, *seed, *note, *out); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

func run(addr, symbol string, rate int, duration, warmup time.Duration, nconns int, seed uint64, note, out string) error {
	warmN := int(float64(rate) * warmup.Seconds())
	measN := int(float64(rate) * duration.Seconds())
	total := warmN + measN

	profile := enginetest.DefaultProfile()
	cmds := enginetest.Generate(total, seed, profile)
	runID := strconv.FormatInt(time.Now().Unix(), 36)
	cid := func(orderID uint64) string { return "lg" + runID + "-" + strconv.FormatUint(orderID, 10) }

	conns := make([]exchangev1.OrderGatewayServiceClient, nconns)
	for i := range conns {
		c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer c.Close()
		conns[i] = exchangev1.NewOrderGatewayServiceClient(c)
	}

	latency := hdrhistogram.New(1, int64(60*time.Second/time.Microsecond), 3)
	lag := hdrhistogram.New(1, int64(60*time.Second/time.Microsecond), 3)
	var mu sync.Mutex
	errs := map[string]int{}
	outcomes := map[string]int{}
	var wg sync.WaitGroup

	one := func(i int, intended time.Time, measured bool) {
		defer wg.Done()
		c := cmds[i]
		client := conns[i%len(conns)]
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		var outcome string
		var err error
		if c.Kind == engine.CmdPlace {
			var r *exchangev1.PlaceOrderResponse
			r, err = client.PlaceOrder(ctx, toPlace(&c, symbol, cid(c.OrderID)))
			if err == nil {
				outcome = "place_" + r.GetStatus().String()
				if r.GetRejectReason() != exchangev1.RejectReason_REJECT_REASON_UNSPECIFIED {
					outcome += "_" + r.GetRejectReason().String()
				}
			}
		} else {
			var r *exchangev1.CancelOrderResponse
			r, err = client.CancelOrder(ctx, &exchangev1.CancelOrderRequest{
				ClientOrderId: cid(c.OrderID), AccountId: c.AccountID, Symbol: symbol,
			})
			if err == nil {
				outcome = "cancel_" + r.GetStatus().String()
				if r.GetRejectReason() != exchangev1.RejectReason_REJECT_REASON_UNSPECIFIED {
					outcome += "_" + r.GetRejectReason().String()
				}
			}
		}
		elapsed := time.Since(intended) // from the INTENDED send time
		if !measured {
			return
		}
		mu.Lock()
		if err != nil {
			errs[status.Code(err).String()]++
		} else {
			outcomes[outcome]++
		}
		_ = latency.RecordValue(max(1, elapsed.Microseconds()))
		mu.Unlock()
	}

	interval := float64(time.Second) / float64(rate)
	start := time.Now().Add(200 * time.Millisecond)
	var measuredStart time.Time
	for i := 0; i < total; i++ {
		intended := start.Add(time.Duration(float64(i) * interval))
		if i == warmN {
			measuredStart = intended
		}
		for {
			d := time.Until(intended)
			if d <= 0 {
				break
			}
			if d > 300*time.Microsecond {
				time.Sleep(d - 200*time.Microsecond)
			} else {
				runtime.Gosched() // short spin for the last stretch
			}
		}
		if i >= warmN {
			mu.Lock()
			_ = lag.RecordValue(max(1, time.Since(intended).Microseconds()))
			mu.Unlock()
		}
		wg.Add(1)
		go one(i, intended, i >= warmN)
	}
	wg.Wait()
	measuredEnd := time.Now()

	res := result{
		Timestamp: time.Now().UTC().Format(time.RFC3339), Env: benchenv.Capture(), Note: note,
		Target: addr, RatePerSec: rate, DurationSec: duration.Seconds(), WarmupSec: warmup.Seconds(),
		Conns: nconns, Seed: seed, Sent: measN, Completed: latency.TotalCount(),
		Errors: errs, Outcomes: outcomes,
		AchievedPerSec: float64(latency.TotalCount()) / measuredEnd.Sub(measuredStart).Seconds(),
		LatencyUs:      pct(latency), SchedulerLagUs: pct(lag),
	}
	if out != "" {
		hgrm := out + ".hgrm"
		f, err := os.Create(hgrm)
		if err != nil {
			return err
		}
		latency.PercentilesPrint(f, 5, 1.0) // microseconds
		_ = f.Close()
		res.LatencyHdrFile = hgrm
		b, _ := json.MarshalIndent(res, "", "  ")
		if err := os.WriteFile(out, append(b, '\n'), 0o644); err != nil {
			return err
		}
	}
	report(&res)
	return nil
}

func pct(h *hdrhistogram.Histogram) percentiles {
	return percentiles{h.ValueAtQuantile(50), h.ValueAtQuantile(90), h.ValueAtQuantile(99), h.ValueAtQuantile(99.9), h.Max()}
}

func toPlace(c *engine.Command, symbol, clientID string) *exchangev1.PlaceOrderRequest {
	r := &exchangev1.PlaceOrderRequest{
		ClientOrderId: clientID, AccountId: c.AccountID, Symbol: symbol,
		Price: c.Price, Quantity: c.Qty,
	}
	if c.Side == engine.Buy {
		r.Side = exchangev1.Side_SIDE_BUY
	} else {
		r.Side = exchangev1.Side_SIDE_SELL
	}
	if c.Type == engine.Market {
		r.Type = exchangev1.OrderType_ORDER_TYPE_MARKET
	} else {
		r.Type = exchangev1.OrderType_ORDER_TYPE_LIMIT
		if c.TIF == engine.IOC {
			r.TimeInForce = exchangev1.TimeInForce_TIME_IN_FORCE_IOC
		} else {
			r.TimeInForce = exchangev1.TimeInForce_TIME_IN_FORCE_GTC
		}
	}
	return r
}

func report(r *result) {
	fmt.Printf("target=%s rate=%d/s measured=%.0fs warmup=%.0fs conns=%d commit=%s hw=%q %s GOMAXPROCS=%d\n",
		r.Target, r.RatePerSec, r.DurationSec, r.WarmupSec, r.Conns, r.Commit, r.Hardware, r.GoVersion, r.GOMAXPROCS)
	l := r.LatencyUs
	fmt.Printf("completed=%d achieved=%.0f/s errors=%v\n", r.Completed, r.AchievedPerSec, r.Errors)
	fmt.Printf("latency (us, from intended send time): p50=%d p90=%d p99=%d p99.9=%d max=%d\n", l.P50, l.P90, l.P99, l.P999, l.Max)
	g := r.SchedulerLagUs
	fmt.Printf("loadgen scheduler lag (us): p50=%d p99=%d p99.9=%d max=%d\n", g.P50, g.P99, g.P999, g.Max)
	keys := make([]string, 0, len(r.Outcomes))
	for k := range r.Outcomes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("  %-55s %d\n", k, r.Outcomes[k])
	}
}
