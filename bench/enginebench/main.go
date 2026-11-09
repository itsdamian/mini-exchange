// Command enginebench measures the in-process latency distribution of
// engine.Book.Apply (no network, no queue) and writes a JSON result with the
// environment needed to reproduce it.
//
// Method: a deterministic command stream is generated up front, the book is
// warmed with one full pass, then every Apply call is timed individually and
// recorded in an HdrHistogram. The run is repeated and the spread reported.
// Because the engine is driven back-to-back with no arrival process, this is a
// service-time measurement of the matcher alone; coordinated omission applies
// to the open-loop gateway benchmark (phase 2), not here. The cost of the
// timer itself is measured separately and reported, since at tens of
// nanoseconds per order it is not negligible.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"

	"github.com/itsdamian/mini-exchange/internal/engine"
	"github.com/itsdamian/mini-exchange/internal/enginetest"
)

type percentiles struct {
	P50, P90, P99, P999, Max int64 // nanoseconds
}

type runResult struct {
	Orders       int         `json:"orders"`
	OrdersPerSec float64     `json:"orders_per_sec"`
	LatencyNs    percentiles `json:"latency_ns"`
	Mallocs      uint64      `json:"mallocs_during_run"`
	Events       int         `json:"events_emitted"`
}

type result struct {
	Scenario    string      `json:"scenario"`
	Timestamp   string      `json:"timestamp"`
	Commit      string      `json:"commit"`
	Hardware    string      `json:"hardware"`
	OS          string      `json:"os"`
	GoVersion   string      `json:"go_version"`
	GOMAXPROCS  int         `json:"gomaxprocs"`
	GOGC        string      `json:"gogc"`
	GOMEMLIMIT  string      `json:"gomemlimit"`
	Seed        uint64      `json:"seed"`
	TimerCostNs percentiles `json:"timer_overhead_ns"`
	Runs        []runResult `json:"runs"`
}

func main() {
	var (
		scenario = flag.String("scenario", "mixed", "mixed | deep")
		orders   = flag.Int("orders", 5_000_000, "orders measured per run")
		runs     = flag.Int("runs", 5, "repeated runs")
		seed     = flag.Uint64("seed", 7, "generator seed")
		out      = flag.String("out", "", "write JSON result to this file")
	)
	flag.Parse()

	p := enginetest.DefaultProfile()
	if *scenario == "deep" {
		p.Depth, p.TargetBook = 500, 20_000
	}

	// A generated stream of 1<<20 commands is replayed; ids are shifted per lap.
	const lap = 1 << 20
	cmds := enginetest.Generate(lap, *seed, p)

	res := result{
		Scenario: *scenario, Timestamp: time.Now().UTC().Format(time.RFC3339),
		Commit: gitCommit(), Hardware: hardware(), OS: runtime.GOOS + "/" + runtime.GOARCH,
		GoVersion: runtime.Version(), GOMAXPROCS: runtime.GOMAXPROCS(0),
		GOGC: envOr("GOGC", "100 (default)"), GOMEMLIMIT: envOr("GOMEMLIMIT", "unset"),
		Seed: *seed, TimerCostNs: timerCost(),
	}

	for r := 0; r < *runs; r++ {
		res.Runs = append(res.Runs, runOnce(cmds, p, *orders))
	}

	printReport(&res)
	if *out != "" {
		b, _ := json.MarshalIndent(res, "", "  ")
		if err := os.WriteFile(*out, append(b, '\n'), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println("wrote", *out)
	}
}

func runOnce(cmds []engine.Command, p enginetest.Profile, orders int) runResult {
	const lap = 1 << 20
	book := engine.NewBook(engine.Config{MaxOrders: p.TargetBook * 2, MaxLevels: int(p.Depth) * 4})
	evs := make([]engine.Event, 0, 4096)
	var ts int64
	step := func(i int, h *hdrhistogram.Histogram) {
		c := cmds[i&(lap-1)]
		c.OrderID += uint64(i/lap) * (1 << 41)
		ts++
		c.Ts = ts
		if h == nil {
			evs = book.Apply(&c, evs[:0])
			return
		}
		t0 := time.Now()
		evs = book.Apply(&c, evs[:0])
		_ = h.RecordValue(int64(time.Since(t0)))
	}
	for i := 0; i < lap; i++ { // warm-up lap, unmeasured
		step(i, nil)
	}
	h := hdrhistogram.New(1, int64(time.Second), 3)
	runtime.GC()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	events := 0
	start := time.Now()
	for i := lap; i < lap+orders; i++ {
		step(i, h)
		events += len(evs)
	}
	elapsed := time.Since(start)
	runtime.ReadMemStats(&m1)
	return runResult{
		Orders:       orders,
		OrdersPerSec: float64(orders) / elapsed.Seconds(),
		LatencyNs: percentiles{
			P50: h.ValueAtQuantile(50), P90: h.ValueAtQuantile(90), P99: h.ValueAtQuantile(99),
			P999: h.ValueAtQuantile(99.9), Max: h.Max(),
		},
		Mallocs: m1.Mallocs - m0.Mallocs,
		Events:  events,
	}
}

// timerCost measures back-to-back time.Now/time.Since pairs with nothing in
// between: the floor under every recorded latency.
func timerCost() percentiles {
	h := hdrhistogram.New(1, int64(time.Second), 3)
	for i := 0; i < 2_000_000; i++ {
		t0 := time.Now()
		_ = h.RecordValue(int64(time.Since(t0)))
	}
	return percentiles{h.ValueAtQuantile(50), h.ValueAtQuantile(90), h.ValueAtQuantile(99), h.ValueAtQuantile(99.9), h.Max()}
}

func printReport(r *result) {
	fmt.Printf("scenario=%s commit=%s hw=%q os=%s %s GOMAXPROCS=%d GOGC=%s GOMEMLIMIT=%s\n",
		r.Scenario, r.Commit, r.Hardware, r.OS, r.GoVersion, r.GOMAXPROCS, r.GOGC, r.GOMEMLIMIT)
	t := r.TimerCostNs
	fmt.Printf("timer overhead (ns): p50=%d p90=%d p99=%d p99.9=%d max=%d\n", t.P50, t.P90, t.P99, t.P999, t.Max)
	fmt.Printf("%-4s %12s %8s %8s %8s %8s %10s %8s\n", "run", "orders/s", "p50ns", "p90ns", "p99ns", "p99.9ns", "maxns", "mallocs")
	for i, x := range r.Runs {
		l := x.LatencyNs
		fmt.Printf("%-4d %12.0f %8d %8d %8d %8d %10d %8d\n", i+1, x.OrdersPerSec, l.P50, l.P90, l.P99, l.P999, l.Max, x.Mallocs)
	}
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func gitCommit() string {
	rev, err := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	if err != nil {
		if bi, ok := debug.ReadBuildInfo(); ok {
			for _, s := range bi.Settings {
				if s.Key == "vcs.revision" {
					return s.Value
				}
			}
		}
		return "unknown"
	}
	c := strings.TrimSpace(string(rev))
	if st, err := exec.Command("git", "status", "--porcelain", "--untracked-files=no").Output(); err == nil && len(strings.TrimSpace(string(st))) > 0 {
		c += "-dirty"
	}
	return c
}

func hardware() string {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("sysctl", "-n", "machdep.cpu.brand_string")
	case "linux":
		cmd = exec.Command("sh", "-c", "grep -m1 'model name' /proc/cpuinfo | cut -d: -f2")
	default:
		return runtime.GOARCH
	}
	b, err := cmd.Output()
	if err != nil {
		return runtime.GOARCH
	}
	return fmt.Sprintf("%s (%d logical CPUs)", strings.TrimSpace(string(b)), runtime.NumCPU())
}
