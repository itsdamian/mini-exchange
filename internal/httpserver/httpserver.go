// Package httpserver is the small operational HTTP surface every service
// exposes: liveness/readiness probes and Prometheus metrics.
package httpserver

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// New returns an http.Server serving /healthz, /readyz and /metrics from reg.
func New(addr string, reg *prometheus.Registry) *http.Server {
	return NewWithReady(addr, reg, nil)
}

// NewWithReady is New with a readiness check: /readyz answers 503 with the
// error text while ready returns an error. /healthz is liveness only.
func NewWithReady(addr string, reg *prometheus.Registry, ready func() error) *http.Server {
	mux := http.NewServeMux()
	ok := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	}
	mux.HandleFunc("GET /healthz", ok)
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if ready != nil {
			if err := ready(); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
		}
		ok(w, r)
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}
