package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/itsdamian/mini-exchange/internal/engine"
	"github.com/itsdamian/mini-exchange/internal/marketdata"
)

func get(t *testing.T, h http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
	return rec
}

// Every combination of options must register without a routing conflict (the mux panics at
// start-up otherwise, which is how "/ws" against "GET /" was found) and serve what it should.
func TestMuxWithEveryOption(t *testing.T) {
	reg := prometheus.NewRegistry()
	feed := marketdata.NewFeed("BTC-USD", engine.NewBook(engine.Config{MaxOrders: 16, MaxLevels: 4}), marketdata.FeedConfig{Registerer: reg})
	srv := marketdata.NewServer(feed, marketdata.ServerConfig{Registerer: reg})
	lt := marketdata.NewLatencyTracker("http://127.0.0.1:1/metrics", time.Second)

	ui := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(ui, "index.html"), []byte("<title>ui</title>"), 0o600))

	t.Run("everything", func(t *testing.T) {
		m := newMux(srv, reg, lt, ui)
		require.Equal(t, 200, get(t, m, "GET", "/healthz").Code)
		require.Equal(t, 200, get(t, m, "GET", "/readyz").Code)
		require.Equal(t, 200, get(t, m, "GET", "/metrics").Code)
		require.Equal(t, 426, get(t, m, "GET", "/ws").Code, "a plain GET to the feed endpoint is told to upgrade")
		require.Equal(t, 200, get(t, m, "GET", "/api/latency").Code)
		root := get(t, m, "GET", "/")
		require.Equal(t, 200, root.Code)
		require.Contains(t, root.Body.String(), "<title>ui</title>")
		require.Equal(t, 405, get(t, m, "POST", "/ws").Code, "only GET reaches the feed")
	})
	t.Run("no UI and no latency", func(t *testing.T) {
		m := newMux(srv, reg, nil, "")
		require.Equal(t, 404, get(t, m, "GET", "/").Code)
		require.Equal(t, 404, get(t, m, "GET", "/api/latency").Code)
		require.Equal(t, 200, get(t, m, "GET", "/healthz").Code)
	})
}
