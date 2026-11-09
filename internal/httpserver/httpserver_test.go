package httpserver_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/stretchr/testify/require"

	"github.com/itsdamian/mini-exchange/internal/httpserver"
)

func TestEndpoints(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	srv := httptest.NewServer(httpserver.New("", reg).Handler)
	defer srv.Close()

	for _, path := range []string{"/healthz", "/readyz"} {
		resp, err := http.Get(srv.URL + path)
		require.NoError(t, err)
		_ = resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode, path)
	}

	resp, err := http.Get(srv.URL + "/metrics")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, string(body), "go_goroutines")
}

func TestUnknownPathAndMethod(t *testing.T) {
	srv := httptest.NewServer(httpserver.New("", prometheus.NewRegistry()).Handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/nope")
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	resp, err = http.Post(srv.URL+"/healthz", "text/plain", nil)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
}
