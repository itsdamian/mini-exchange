package main

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/itsdamian/mini-exchange/internal/marketdata"
)

// newMux wires the HTTP surface of the process: the WebSocket feed, metrics and probes,
// and optionally the latency endpoint (lt != nil) and the built UI (uiDir != "").
// Kept apart from run() so the routing, which panics at start-up if two patterns
// conflict, is covered by a test.
func newMux(srv *marketdata.Server, reg *prometheus.Registry, lt *marketdata.LatencyTracker, uiDir string) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("GET /ws", srv.Handler())
	if lt != nil {
		mux.Handle("GET /api/latency", lt.Handler())
	}
	if uiDir != "" {
		mux.Handle("GET /", http.FileServer(http.Dir(uiDir)))
	}
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	ok := func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) }
	mux.HandleFunc("GET /healthz", ok)
	mux.HandleFunc("GET /readyz", ok)
	return mux
}
