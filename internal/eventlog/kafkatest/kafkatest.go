// Package kafkatest starts a real single-node Kafka (KRaft) in Docker for
// integration and chaos tests, on a FIXED host port so the broker can be
// stopped, killed and restarted without clients losing its address.
package kafkatest

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const image = "apache/kafka:3.9.0"

// Broker is a running Kafka container.
type Broker struct {
	c    testcontainers.Container
	Addr string // host:port reachable from the test process
}

// Brokers returns the bootstrap list.
func (b *Broker) Brokers() []string { return []string{b.Addr} }

var (
	once   sync.Once
	shared *Broker
	err    error
)

// Shared returns one broker per test binary, started on first use. Tests that
// stop or kill it must restart it before returning.
func Shared(t *testing.T) *Broker {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping Kafka integration test in -short mode")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)
	once.Do(func() { shared, err = start(context.Background()) })
	if err != nil {
		t.Fatalf("start kafka: %v", err)
	}
	return shared
}

// Terminate removes the shared broker. Call it from TestMain.
func Terminate() {
	if shared != nil {
		_ = shared.c.Terminate(context.Background())
	}
}

func freePort() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer l.Close()
	_, p, _ := net.SplitHostPort(l.Addr().String())
	return p, nil
}

func start(ctx context.Context) (*Broker, error) {
	port, err := freePort()
	if err != nil {
		return nil, err
	}
	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{"9092/tcp"},
		Env: map[string]string{
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@127.0.0.1:9093",
			"KAFKA_LISTENERS":                                "INTERNAL://:19092,EXTERNAL://:9092,CONTROLLER://:9093",
			"KAFKA_ADVERTISED_LISTENERS":                     "INTERNAL://127.0.0.1:19092,EXTERNAL://127.0.0.1:" + port,
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT",
			"KAFKA_INTER_BROKER_LISTENER_NAME":               "INTERNAL",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE":                "false",
		},
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.PortBindings = network.PortMap{network.MustParsePort("9092/tcp"): []network.PortBinding{{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: port}}}
		},
		WaitingFor: wait.ForLog("Kafka Server started").WithStartupTimeout(2 * time.Minute),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		return nil, err
	}
	return &Broker{c: c, Addr: "127.0.0.1:" + port}, nil
}

// Stop shuts the broker down gracefully (SIGTERM).
func (b *Broker) Stop(ctx context.Context) error {
	d := 20 * time.Second
	return b.c.Stop(ctx, &d)
}

// Kill terminates the broker with SIGKILL, like a crash.
func (b *Broker) Kill(ctx context.Context) error {
	out, err := exec.CommandContext(ctx, "docker", "kill", b.c.GetContainerID()).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker kill: %w: %s", err, out)
	}
	return nil
}

// Restart starts a stopped or killed broker again on the same address and
// waits until it accepts connections.
func (b *Broker) Restart(ctx context.Context) error {
	if err := b.c.Start(ctx); err != nil {
		return err
	}
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", b.Addr, time.Second)
		if err == nil {
			_ = conn.Close()
			time.Sleep(3 * time.Second) // let the controller finish electing itself
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("kafka did not come back on %s", b.Addr)
}
