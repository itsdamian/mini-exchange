// Package pgtest starts a real PostgreSQL 17 in Docker for integration and
// chaos tests, on a FIXED host port so it can be stopped and restarted (a
// database outage) without clients losing its address.
package pgtest

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const image = "postgres:17-alpine"

// Server is a running PostgreSQL container.
type Server struct {
	c    testcontainers.Container
	Addr string // host:port
	n    int
	mu   sync.Mutex
}

var (
	once   sync.Once
	shared *Server
	err    error
)

// Shared returns one server per test binary, started on first use. Tests that
// stop it must restart it before returning. Skips under -short.
func Shared(t *testing.T) *Server {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping PostgreSQL integration test in -short mode")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)
	once.Do(func() { shared, err = start(context.Background()) })
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	return shared
}

// Terminate removes the shared server. Call it from TestMain.
func Terminate() {
	if shared != nil {
		_ = shared.c.Terminate(context.Background())
	}
}

func start(ctx context.Context) (*Server, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	_, port, _ := net.SplitHostPort(l.Addr().String())
	_ = l.Close()

	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{"5432/tcp"},
		Env:          map[string]string{"POSTGRES_USER": "test", "POSTGRES_PASSWORD": "test", "POSTGRES_DB": "postgres"},
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.PortBindings = network.PortMap{network.MustParsePort("5432/tcp"): []network.PortBinding{{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: port}}}
		},
		// The image restarts the server once during init, so the log line
		// appears twice.
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(2 * time.Minute),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		return nil, err
	}
	return &Server{c: c, Addr: "127.0.0.1:" + port}, nil
}

// NewDSN creates a fresh, empty database (isolating one test from another) and
// returns its connection string.
func (s *Server) NewDSN(t *testing.T) string {
	t.Helper()
	s.mu.Lock()
	s.n++
	name := fmt.Sprintf("t%d_%d", time.Now().UnixNano(), s.n)
	s.mu.Unlock()
	code, _, err := s.c.Exec(context.Background(), []string{"psql", "-U", "test", "-d", "postgres", "-c", "CREATE DATABASE " + name})
	if err != nil || code != 0 {
		t.Fatalf("create database: code=%d err=%v", code, err)
	}
	return fmt.Sprintf("postgres://test:test@%s/%s?sslmode=disable", s.Addr, name)
}

// Stop shuts PostgreSQL down.
func (s *Server) Stop(ctx context.Context) error {
	d := 10 * time.Second
	return s.c.Stop(ctx, &d)
}

// Restart starts a stopped server on the same address and waits for it to
// accept connections.
func (s *Server) Restart(ctx context.Context) error {
	if err := s.c.Start(ctx); err != nil {
		return err
	}
	deadline := time.Now().Add(time.Minute)
	for time.Now().Before(deadline) {
		code, _, err := s.c.Exec(ctx, []string{"pg_isready", "-U", "test"})
		if err == nil && code == 0 {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("postgres did not come back on %s", s.Addr)
}
