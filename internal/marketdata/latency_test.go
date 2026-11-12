package marketdata

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// exposition builds the gateway's histogram in the Prometheus text format for the
// given cumulative counts of three buckets (le 0.001, 0.002, +Inf), PlaceOrder
// with two status codes, plus another RPC that must be ignored.
func exposition(okLe1, okLe2, okInf, errLe1, errLe2, errInf int) string {
	var b strings.Builder
	b.WriteString("# TYPE mx_gateway_request_seconds histogram\n")
	line := func(rpc, code string, c1, c2, inf int) {
		fmt.Fprintf(&b, "mx_gateway_request_seconds_bucket{rpc=%q,code=%q,le=\"0.001\"} %d\n", rpc, code, c1)
		fmt.Fprintf(&b, "mx_gateway_request_seconds_bucket{rpc=%q,code=%q,le=\"0.002\"} %d\n", rpc, code, c2)
		fmt.Fprintf(&b, "mx_gateway_request_seconds_bucket{rpc=%q,code=%q,le=\"+Inf\"} %d\n", rpc, code, inf)
		fmt.Fprintf(&b, "mx_gateway_request_seconds_sum{rpc=%q,code=%q} 1\n", rpc, code)
		fmt.Fprintf(&b, "mx_gateway_request_seconds_count{rpc=%q,code=%q} %d\n", rpc, code, inf)
	}
	line("PlaceOrder", "OK", okLe1, okLe2, okInf)
	line("PlaceOrder", "Unavailable", errLe1, errLe2, errInf)
	line("CancelOrder", "OK", 999999, 999999, 999999) // must not count
	return b.String()
}

func TestParsePlaceOrderSumsCodesAndIgnoresOtherRPCs(t *testing.T) {
	s, err := parsePlaceOrder(exposition(10, 20, 30, 1, 2, 3), time.Now())
	require.NoError(t, err)
	require.EqualValues(t, 33, s.total)
	require.Len(t, s.buckets, 3)
	require.EqualValues(t, 11, s.buckets[0].count)
	require.EqualValues(t, 22, s.buckets[1].count)
	require.True(t, math.IsInf(s.buckets[2].le, 1))
	require.EqualValues(t, 33, s.buckets[2].count)

	_, err = parsePlaceOrder("go_goroutines 7\n", time.Now())
	require.ErrorContains(t, err, "not found")
	_, err = parsePlaceOrder("this is not metrics {", time.Now())
	require.Error(t, err)
}

// The gateway labels the RPC with its short name; a full gRPC method path must work too.
func TestParsePlaceOrderAcceptsTheShortNameAndTheFullPath(t *testing.T) {
	for _, rpc := range []string{"PlaceOrder", "/exchange.v1.OrderGatewayService/PlaceOrder"} {
		text := fmt.Sprintf("# TYPE mx_gateway_request_seconds histogram\nmx_gateway_request_seconds_bucket{code=\"OK\",rpc=%q,le=\"+Inf\"} 7\nmx_gateway_request_seconds_sum{code=\"OK\",rpc=%q} 1\nmx_gateway_request_seconds_count{code=\"OK\",rpc=%q} 7\n", rpc, rpc, rpc)
		s, err := parsePlaceOrder(text, time.Now())
		require.NoError(t, err)
		require.EqualValues(t, 7, s.total, rpc)
	}
}

func TestQuantileInterpolatesWithinABucket(t *testing.T) {
	// 100 requests since the first sample: 50 under 1 ms, 40 in 1 to 2 ms, 10 above.
	a := sample{buckets: []bucket{{0.001, 0}, {0.002, 0}, {math.Inf(1), 0}}}
	b := sample{total: 100, buckets: []bucket{{0.001, 50}, {0.002, 90}, {math.Inf(1), 100}}}
	p50, ok := quantile(0.5, a, b)
	require.True(t, ok)
	require.InDelta(t, 0.001, p50, 1e-9, "the median is exactly the top of the first bucket")
	p70, _ := quantile(0.7, a, b)
	require.InDelta(t, 0.0015, p70, 1e-9, "half way through the second bucket")
	p99, _ := quantile(0.99, a, b)
	require.InDelta(t, 0.002, p99, 1e-9, "above the last finite bucket the bound of that bucket is reported")

	_, ok = quantile(0.5, b, b)
	require.False(t, ok, "no requests between two identical samples")
}

func TestQuantileUsesTheDifferenceBetweenScrapes(t *testing.T) {
	// Before: 1,000 old slow requests (all in the 1 to 2 ms bucket). After: 100 new fast ones.
	a := sample{total: 1000, buckets: []bucket{{0.001, 0}, {0.002, 1000}, {math.Inf(1), 1000}}}
	b := sample{total: 1100, buckets: []bucket{{0.001, 100}, {0.002, 1100}, {math.Inf(1), 1100}}}
	p99, ok := quantile(0.99, a, b)
	require.True(t, ok)
	require.Less(t, p99, 0.001, "only the new requests count")
}

func TestTrackerEndToEndWithAFakeGateway(t *testing.T) {
	var step atomic.Int64
	gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := int(step.Add(1))
		// every scrape 100 more requests, 80% under 1 ms, 15% in 1 to 2 ms, 5% slower
		fmt.Fprint(w, exposition(80*n, 95*n, 100*n, 0, 0, 0))
	}))
	defer gw.Close()

	l := NewLatencyTracker(gw.URL, time.Second)
	for i := 0; i < 8; i++ {
		l.Scrape(context.Background())
		time.Sleep(5 * time.Millisecond)
	}
	r := l.Snapshot(10 * time.Second)
	require.True(t, r.OK, r.Error)
	require.Greater(t, r.RequestsPerS, 0.0)
	require.Less(t, r.P50Us, 1000.0)
	require.Greater(t, r.P99Us, 1000.0)
	require.LessOrEqual(t, r.P99Us, 2000.0)
	require.NotEmpty(t, r.Series)

	srv := httptest.NewServer(l.Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "?window=5")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))
	require.Equal(t, "no-store", resp.Header.Get("Cache-Control"))
}

func TestTrackerReportsAnUnreachableGateway(t *testing.T) {
	l := NewLatencyTracker("http://127.0.0.1:1/metrics", time.Second)
	l.Scrape(context.Background())
	l.Scrape(context.Background())
	r := l.Snapshot(10 * time.Second)
	require.False(t, r.OK)
	require.NotEmpty(t, r.Error)

	// And before any scrape.
	require.Contains(t, NewLatencyTracker("http://x", time.Second).Snapshot(0).Error, "waiting")
}

func TestTrackerWithNoPlaceOrderYet(t *testing.T) {
	gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "# TYPE mx_gateway_request_seconds histogram\nmx_gateway_request_seconds_bucket{rpc=\"/x/CancelOrder\",code=\"OK\",le=\"+Inf\"} 5\n")
	}))
	defer gw.Close()
	l := NewLatencyTracker(gw.URL, time.Second)
	l.Scrape(context.Background())
	l.Scrape(context.Background())
	r := l.Snapshot(10 * time.Second)
	require.False(t, r.OK)
	require.Contains(t, r.Error, "no PlaceOrder")
}
