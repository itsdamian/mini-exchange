package eventlog

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// NewProducerClient returns a client tuned for the event log: idempotent
// (franz-go default), acks from all in-sync replicas, ordered, with a short
// linger so batches fill without adding visible latency. Linger and buffer
// size are the knobs of the phase 8 batching experiment.
func NewProducerClient(brokers []string, linger time.Duration, maxBuffered int, opts ...kgo.Opt) (*kgo.Client, error) {
	base := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(linger),
		kgo.MaxBufferedRecords(maxBuffered),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
		kgo.RecordRetries(1 << 30), // outlast a Kafka outage; ordering is kept by the idempotent producer
		kgo.UnknownTopicRetries(-1),
		kgo.RetryBackoffFn(func(int) time.Duration { return 250 * time.Millisecond }),
	}
	return kgo.NewClient(append(base, opts...)...)
}

// EnsureTopic creates the symbol's event topic (one partition, infinite
// retention because the log is the system of record) if it does not exist.
func EnsureTopic(ctx context.Context, cl *kgo.Client, symbol string, replication int16) error {
	adm := kadm.NewClient(cl)
	cfgs := map[string]*string{"retention.ms": ptr("-1"), "retention.bytes": ptr("-1")}
	resp, err := adm.CreateTopic(ctx, 1, replication, cfgs, Topic(symbol))
	if err != nil {
		if errors.Is(err, kerr.TopicAlreadyExists) || (resp.Err != nil && errors.Is(resp.Err, kerr.TopicAlreadyExists)) {
			return nil
		}
		return fmt.Errorf("create topic %s: %w", Topic(symbol), err)
	}
	return nil
}

func ptr(s string) *string { return &s }
