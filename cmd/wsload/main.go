// Command wsload opens many WebSocket clients against marketdata and reports
// what they received: frames per client, the age of each frame when it
// arrived (engine event time to client receive, same host clock) and
// disconnections. It is the client side of the FR-7 load test.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/coder/websocket"
)

func main() {
	var (
		url      = flag.String("url", "ws://localhost:8083/ws", "marketdata WebSocket URL")
		clients  = flag.Int("clients", 1000, "simultaneous clients")
		duration = flag.Duration("duration", 30*time.Second, "how long to listen after all clients are connected (SIGINT ends it early)")
		slow     = flag.Int("slow", 0, "of the clients, how many connect and never read (they should be disconnected)")
		out      = flag.String("out", "", "write the JSON result here as well")
	)
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sig, stopSig := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSig()

	var (
		mu       sync.Mutex
		age      = hdrhistogram.New(1, 60_000_000, 3) // microseconds
		gap      = hdrhistogram.New(1, 60_000_000, 3) // time between frames of one client, microseconds
		frames   atomic.Int64
		failed   atomic.Int64
		dropped  atomic.Int64
		wg       sync.WaitGroup
		connect  = make(chan struct{}, 64)
		measured atomic.Bool
	)
	start := func(i int) {
		defer wg.Done()
		dctx, dcancel := context.WithTimeout(ctx, 30*time.Second)
		c, _, err := websocket.Dial(dctx, *url, nil)
		dcancel()
		<-connect
		if err != nil {
			failed.Add(1)
			return
		}
		defer func() { _ = c.CloseNow() }()
		c.SetReadLimit(1 << 20)
		if i < *slow {
			<-ctx.Done() // never reads
			return
		}
		var last time.Time
		for {
			_, data, err := c.Read(ctx)
			if err != nil {
				if ctx.Err() == nil {
					dropped.Add(1)
				}
				return
			}
			now := time.Now()
			if !measured.Load() {
				last = time.Time{}
				continue
			}
			var m struct {
				TsNs int64 `json:"ts_ns"`
			}
			_ = json.Unmarshal(data, &m)
			mu.Lock()
			if m.TsNs > 0 {
				_ = age.RecordValue(max(1, (now.UnixNano()-m.TsNs)/1000))
			}
			if !last.IsZero() {
				_ = gap.RecordValue(max(1, now.Sub(last).Microseconds()))
			}
			mu.Unlock()
			last = now
			frames.Add(1)
		}
	}
	for i := 0; i < *clients; i++ {
		connect <- struct{}{}
		wg.Add(1)
		go start(i)
	}
	for len(connect) > 0 {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(time.Second)
	measured.Store(true)
	t0 := time.Now()
	select { // SIGINT or SIGTERM also ends the measurement (the load test script does that)
	case <-time.After(*duration):
	case <-sig.Done():
	}
	elapsed := time.Since(t0)
	cancel()
	wg.Wait()

	pct := func(h *hdrhistogram.Histogram) map[string]int64 {
		return map[string]int64{"p50": h.ValueAtQuantile(50), "p99": h.ValueAtQuantile(99), "max": h.Max()}
	}
	res := map[string]any{
		"clients": *clients, "slow_clients": *slow, "measured_seconds": elapsed.Seconds(),
		"connect_failures": failed.Load(), "dropped_by_server": dropped.Load(),
		"frames": frames.Load(), "frames_per_client_per_sec": float64(frames.Load()) / float64(max(1, *clients-*slow)) / elapsed.Seconds(),
		"frame_age_us": pct(age), "gap_between_frames_us": pct(gap),
	}
	b, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(b))
	if *out != "" {
		_ = os.WriteFile(*out, b, 0o644)
	}
}
