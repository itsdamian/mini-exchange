package marketdata

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/itsdamian/mini-exchange/internal/engine"
	"github.com/itsdamian/mini-exchange/internal/eventlog"

	exchangev1 "github.com/itsdamian/mini-exchange/gen/exchange/v1"
)

// ConsumerConfig configures a Consumer.
type ConsumerConfig struct {
	Brokers    []string
	Symbol     string
	Book       engine.Config // sizing of the rebuilt book
	Feed       FeedConfig
	Registerer prometheus.Registerer
	Log        *slog.Logger
}

// Consumer rebuilds the book from the event log (the same recovery the
// engine uses, so the view is exact) and then follows the log.
type Consumer struct {
	cfg  ConsumerConfig
	feed *Feed
	next int64 // next log offset to read
	log  *slog.Logger
}

// NewConsumer replays the log up to its current end and returns a Consumer
// whose Feed is ready to serve.
func NewConsumer(ctx context.Context, cfg ConsumerConfig) (*Consumer, error) {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	book, st, err := eventlog.Recover(ctx, cfg.Brokers, cfg.Symbol, nil, cfg.Book, cfg.Log)
	if err != nil {
		return nil, fmt.Errorf("rebuild book from the event log: %w", err)
	}
	cfg.Feed.Registerer = cfg.Registerer
	cfg.Log.Info("book rebuilt", "events", st.Events, "last_seq", st.LastSeq, "resting_orders", book.Len(), "ms", st.Total.Milliseconds())
	return &Consumer{cfg: cfg, feed: NewFeed(cfg.Symbol, book, cfg.Feed), next: st.LastOffset + 1, log: cfg.Log}, nil
}

// Feed returns the feed the consumer updates.
func (c *Consumer) Feed() *Feed { return c.feed }

// Run follows the log until ctx ends. It returns an error if the log cannot
// be applied to the book (a gap or an inconsistent event): the view would be
// wrong, so it stops rather than serve it.
func (c *Consumer) Run(ctx context.Context) error {
	topic := eventlog.Topic(c.cfg.Symbol)
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.cfg.Brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().At(c.next)}}))
	if err != nil {
		return err
	}
	defer cl.Close()
	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return nil
		}
		for _, fe := range fetches.Errors() {
			if errors.Is(fe.Err, kerr.UnknownTopicOrPartition) {
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(200 * time.Millisecond):
				}
				continue
			}
			c.log.Warn("fetch error", "err", fe.Err)
		}
		var applyErr error
		fetches.EachRecord(func(r *kgo.Record) {
			if applyErr != nil {
				return
			}
			var b exchangev1.EventBatch
			if err := proto.Unmarshal(r.Value, &b); err != nil {
				applyErr = fmt.Errorf("offset %d: %w", r.Offset, err)
				return
			}
			evs, err := eventlog.FromProtoBatch(&b, c.cfg.Symbol)
			if err == nil {
				err = c.feed.Apply(evs)
			}
			if err != nil {
				applyErr = fmt.Errorf("offset %d: %w", r.Offset, err)
			}
		})
		if applyErr != nil {
			return applyErr
		}
	}
}
