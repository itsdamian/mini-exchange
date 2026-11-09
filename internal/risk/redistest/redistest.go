// Package redistest starts a real Redis 7 in Docker for integration tests, on
// a FIXED host port so it can be stopped and restarted (a Redis outage)
// without clients losing its address.
package redistest

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
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const image = "redis:7-alpine"

// Server is a running Redis container.
type Server struct {
	c    testcontainers.Container
	Addr string // host:port reachable from the test process
}

var (
	once   sync.Once
	shared *Server
	err    error
)

// Shared returns one Redis per test binary, started on first use. Tests that
// stop it must restart it before returning. Skips under -short.
func Shared(t *testing.T) *Server {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping Redis integration test in -short mode")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)
	once.Do(func() { shared, err = start(context.Background()) })
	if err != nil {
		t.Fatalf("start redis: %v", err)
	}
	return shared
}

// Terminate removes the shared server. Call it from TestMain.
func Terminate() {
	if shared != nil {
		_ = shared.c.Terminate(context.Background())
	}
}

// Client returns a go-redis client for the server and a cleanup function.
func (s *Server) Client(t *testing.T) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: s.Addr, DialTimeout: time.Second})
	t.Cleanup(func() { _ = c.Close() })
	return c
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
		ExposedPorts: []string{"6379/tcp"},
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.PortBindings = network.PortMap{network.MustParsePort("6379/tcp"): []network.PortBinding{{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: port}}}
		},
		WaitingFor: wait.ForLog("Ready to accept connections").WithStartupTimeout(time.Minute),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		return nil, err
	}
	return &Server{c: c, Addr: "127.0.0.1:" + port}, nil
}

// Stop shuts Redis down (the in-memory data is lost: no persistence here).
func (s *Server) Stop(ctx context.Context) error {
	d := 5 * time.Second
	return s.c.Stop(ctx, &d)
}

// Restart starts a stopped Redis on the same address and waits for it.
func (s *Server) Restart(ctx context.Context) error {
	if err := s.c.Start(ctx); err != nil {
		return err
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if conn, err := net.DialTimeout("tcp", s.Addr, time.Second); err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("redis did not come back on %s", s.Addr)
}
