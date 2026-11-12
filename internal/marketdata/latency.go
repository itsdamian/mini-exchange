package marketdata

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// LatencyTracker follows the gateway's request latency histogram for the UI.
// It scrapes the gateway's /metrics once per interval, keeps the cumulative
// bucket counts of every scrape for a few minutes, and answers "what were the
// percentiles over the last N seconds" from the difference between two scrapes.
// Percentiles come from histogram buckets (power-of-two widths), interpolated
// linearly inside a bucket like Prometheus' histogram_quantile, so they are
// estimates with up to the width of a bucket of error.
//
// What it measures is the gateway's handler time for PlaceOrder
// (mx_gateway_request_seconds), not the latency a client sees.
type LatencyTracker struct {
	url      string
	client   *http.Client
	interval time.Duration
	keep     int

	mu      sync.Mutex
	samples []sample
	lastErr string
}

type bucket struct {
	le    float64 // upper bound in seconds
	count float64 // cumulative
}

type sample struct {
	at      time.Time
	buckets []bucket // sorted by le, the last one is +Inf
	total   float64
}

// NewLatencyTracker scrapes url every interval (default 1 s).
func NewLatencyTracker(url string, interval time.Duration) *LatencyTracker {
	if interval <= 0 {
		interval = time.Second
	}
	return &LatencyTracker{url: url, client: &http.Client{Timeout: 2 * time.Second}, interval: interval, keep: 300}
}

// Run scrapes until ctx ends.
func (l *LatencyTracker) Run(ctx context.Context) {
	l.Scrape(ctx)
	t := time.NewTicker(l.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			l.Scrape(ctx)
		}
	}
}

// Scrape fetches the metrics once and records the PlaceOrder histogram.
func (l *LatencyTracker) Scrape(ctx context.Context) {
	s, err := l.fetch(ctx)
	l.mu.Lock()
	defer l.mu.Unlock()
	if err != nil {
		l.lastErr = err.Error()
		return
	}
	l.lastErr = ""
	l.samples = append(l.samples, s)
	if len(l.samples) > l.keep {
		l.samples = l.samples[len(l.samples)-l.keep:]
	}
}

func (l *LatencyTracker) fetch(ctx context.Context) (sample, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, l.url, nil)
	if err != nil {
		return sample{}, err
	}
	resp, err := l.client.Do(req)
	if err != nil {
		return sample{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return sample{}, fmt.Errorf("gateway metrics: %s", resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return sample{}, err
	}
	return parsePlaceOrder(string(body), time.Now())
}

// parsePlaceOrder sums the cumulative buckets of mx_gateway_request_seconds over
// every status code of the PlaceOrder RPC (the gateway labels it rpc="PlaceOrder").
func parsePlaceOrder(text string, at time.Time) (sample, error) {
	parser := expfmt.NewTextParser(model.UTF8Validation)
	fams, err := parser.TextToMetricFamilies(strings.NewReader(text))
	if err != nil {
		return sample{}, fmt.Errorf("parse gateway metrics: %w", err)
	}
	fam := fams["mx_gateway_request_seconds"]
	if fam == nil {
		return sample{}, fmt.Errorf("mx_gateway_request_seconds not found (is this the gateway's /metrics?)")
	}
	sum := map[float64]float64{}
	var total float64
	found := false
	for _, m := range fam.GetMetric() {
		if !labelEndsWith(m, "rpc", "PlaceOrder") || m.GetHistogram() == nil {
			continue
		}
		found = true
		h := m.GetHistogram()
		total += float64(h.GetSampleCount())
		var hasInf bool
		for _, b := range h.GetBucket() {
			sum[b.GetUpperBound()] += float64(b.GetCumulativeCount())
			hasInf = hasInf || math.IsInf(b.GetUpperBound(), 1)
		}
		if !hasInf { // the text format omits nothing, but protobuf-style families may
			sum[math.Inf(1)] += float64(h.GetSampleCount())
		}
	}
	if !found {
		return sample{at: at}, nil // the gateway has not served a PlaceOrder yet
	}
	s := sample{at: at, total: total}
	for le, c := range sum {
		s.buckets = append(s.buckets, bucket{le, c})
	}
	sort.Slice(s.buckets, func(i, j int) bool { return s.buckets[i].le < s.buckets[j].le })
	return s, nil
}

func labelEndsWith(m *dto.Metric, name, suffix string) bool {
	for _, lp := range m.GetLabel() {
		if lp.GetName() == name {
			return strings.HasSuffix(lp.GetValue(), suffix)
		}
	}
	return false
}

// quantile estimates the q-quantile (seconds) of the requests between two
// samples; ok is false when there were none.
func quantile(q float64, a, b sample) (v float64, ok bool) {
	n := len(b.buckets)
	if n == 0 || b.total-a.total <= 0 {
		return 0, false
	}
	delta := make([]float64, n)
	for i, bk := range b.buckets {
		prev := 0.0
		if len(a.buckets) == n && a.buckets[i].le == bk.le { // same bucket layout (always, in practice)
			prev = a.buckets[i].count
		}
		delta[i] = bk.count - prev
	}
	total := delta[n-1]
	if total <= 0 {
		return 0, false
	}
	rank := q * total
	for i, c := range delta {
		if c < rank {
			continue
		}
		lo, loCount := 0.0, 0.0
		if i > 0 {
			lo, loCount = b.buckets[i-1].le, delta[i-1]
		}
		hi := b.buckets[i].le
		if math.IsInf(hi, 1) { // beyond the last finite bucket: report that bucket's bound
			return lo, true
		}
		if c == loCount {
			return hi, true
		}
		return lo + (hi-lo)*(rank-loCount)/(c-loCount), true
	}
	return b.buckets[n-1].le, true
}

// Point is one value of the series in the response.
type Point struct {
	T     int64   `json:"t"` // unix seconds
	P50Us float64 `json:"p50_us"`
	P99Us float64 `json:"p99_us"`
}

// LatencyResponse is the body of GET /api/latency.
type LatencyResponse struct {
	OK            bool    `json:"ok"`
	Error         string  `json:"error,omitempty"`
	WindowSeconds float64 `json:"window_seconds,omitempty"`
	RequestsPerS  float64 `json:"requests_per_second,omitempty"`
	P50Us         float64 `json:"p50_us,omitempty"`
	P99Us         float64 `json:"p99_us,omitempty"`
	P999Us        float64 `json:"p999_us,omitempty"`
	Series        []Point `json:"series,omitempty"` // 5-second sliding windows, one point per scrape
}

// Snapshot computes the response for the last window (default 10 s).
func (l *LatencyTracker) Snapshot(window time.Duration) LatencyResponse {
	if window <= 0 {
		window = 10 * time.Second
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.samples) < 2 {
		msg := l.lastErr
		if msg == "" {
			msg = "waiting for the first two scrapes of the gateway"
		}
		return LatencyResponse{Error: msg}
	}
	last := l.samples[len(l.samples)-1]
	first := l.samples[0]
	for i := len(l.samples) - 1; i >= 0; i-- { // the newest sample at least `window` older than the last
		if last.at.Sub(l.samples[i].at) >= window {
			first = l.samples[i]
			break
		}
	}
	span := last.at.Sub(first.at).Seconds()
	r := LatencyResponse{WindowSeconds: span, Error: l.lastErr}
	if p50, ok := quantile(0.5, first, last); ok {
		p99, _ := quantile(0.99, first, last)
		p999, _ := quantile(0.999, first, last)
		r.OK, r.P50Us, r.P99Us, r.P999Us = true, p50*1e6, p99*1e6, p999*1e6
		r.RequestsPerS = (last.total - first.total) / span
	} else if l.lastErr == "" {
		r.Error = "no PlaceOrder requests in the window"
	}
	const step = 5
	for i := step; i < len(l.samples); i++ {
		a, b := l.samples[i-step], l.samples[i]
		if p50, ok := quantile(0.5, a, b); ok {
			p99, _ := quantile(0.99, a, b)
			r.Series = append(r.Series, Point{T: b.at.Unix(), P50Us: p50 * 1e6, P99Us: p99 * 1e6})
		}
	}
	if len(r.Series) > 120 {
		r.Series = r.Series[len(r.Series)-120:]
	}
	return r
}

// Handler serves GET /api/latency?window=10 (seconds).
func (l *LatencyTracker) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		window := 10 * time.Second
		if s := r.URL.Query().Get("window"); s != "" {
			if d, err := time.ParseDuration(s + "s"); err == nil && d >= time.Second && d <= 5*time.Minute {
				window = d
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(l.Snapshot(window))
	})
}
