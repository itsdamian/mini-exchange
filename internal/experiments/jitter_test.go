package experiments

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

// The tail-latency question left open since phase 2: is the p99.9 / max of the gateway
// caused by the gateway (GC, locks, the rings) or by the machine? This test runs code
// that has none of those: goroutines that do nothing but read the clock in a tight loop
// (no allocation, no locks, no I/O, nothing to garbage collect) and record the longest
// gap between two reads. A gap of milliseconds there can only be the goroutine not
// being scheduled: the OS or the hypervisor took the core away.
//
// Run it with: MX_JITTER_SECONDS=20 go test ./internal/experiments -run HostJitter -v
// (skipped otherwise). Run it once on an idle machine and once while a load test runs.
func TestHostJitter(t *testing.T) {
	secs, _ := strconv.Atoi(os.Getenv("MX_JITTER_SECONDS"))
	if secs <= 0 {
		t.Skip("set MX_JITTER_SECONDS to run")
	}
	workers := runtime.GOMAXPROCS(0)
	if w, _ := strconv.Atoi(os.Getenv("MX_JITTER_WORKERS")); w > 0 {
		workers = w
	}
	thresholds := []time.Duration{200 * time.Microsecond, time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond, 50 * time.Millisecond}
	type result struct {
		max    time.Duration
		counts []int
	}
	results := make([]result, workers)
	deadline := time.Now().Add(time.Duration(secs) * time.Second)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := result{counts: make([]int, len(thresholds))}
			prev := time.Now()
			for {
				now := time.Now()
				gap := now.Sub(prev)
				prev = now
				if gap > r.max {
					r.max = gap
				}
				for i, th := range thresholds {
					if gap >= th {
						r.counts[i]++
					}
				}
				if now.After(deadline) {
					break
				}
			}
			results[w] = r
		}()
	}
	wg.Wait()

	total := make([]int, len(thresholds))
	var max time.Duration
	for _, r := range results {
		if r.max > max {
			max = r.max
		}
		for i := range total {
			total[i] += r.counts[i]
		}
	}
	line := fmt.Sprintf("host jitter: %d spinning goroutines, %d s, GOMAXPROCS=%d: longest gap %v", workers, secs, runtime.GOMAXPROCS(0), max)
	for i, th := range thresholds {
		line += fmt.Sprintf(" | gaps >= %v: %d", th, total[i])
	}
	t.Log(line)
	fmt.Println(line)
}
