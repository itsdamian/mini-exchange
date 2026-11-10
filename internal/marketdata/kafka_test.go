package marketdata_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/itsdamian/mini-exchange/internal/engine"
	"github.com/itsdamian/mini-exchange/internal/eventlog"
	"github.com/itsdamian/mini-exchange/internal/eventlog/kafkatest"
	"github.com/itsdamian/mini-exchange/internal/marketdata"
)

func TestMain(m *testing.M) {
	code := m.Run()
	kafkatest.Terminate()
	os.Exit(code)
}

// The consumer rebuilds the view from what is already in the log, then follows
// new records: the final frame equals the reference book's depth.
func TestConsumerRebuildsThenFollowsTheLog(t *testing.T) {
	symbol := fmt.Sprintf("MDK%d-USD", time.Now().UnixNano()) // a fresh log per run
	b := kafkatest.Shared(t)
	prod, err := eventlog.NewProducerClient(b.Brokers(), time.Millisecond, 1<<16)
	require.NoError(t, err)
	defer prod.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	require.NoError(t, eventlog.EnsureTopic(ctx, prod, symbol, 1))

	batches, ref := history(6000, 9)
	publish := func(bs [][]engine.Event) {
		for _, evs := range bs {
			raw, err := proto.Marshal(eventlog.ToProtoBatch(symbol, evs))
			require.NoError(t, err)
			prod.Produce(ctx, &kgo.Record{Topic: eventlog.Topic(symbol), Value: raw}, nil)
		}
		require.NoError(t, prod.Flush(ctx))
	}
	half := len(batches) / 2
	publish(batches[:half])

	c, err := marketdata.NewConsumer(ctx, marketdata.ConsumerConfig{Brokers: b.Brokers(), Symbol: symbol, Book: bookCfg, Feed: marketdata.FeedConfig{Depth: 5}})
	require.NoError(t, err)
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- c.Run(runCtx) }()
	defer func() { stop(); require.NoError(t, <-done) }()

	publish(batches[half:]) // arrives while the consumer follows
	require.Eventually(t, func() bool {
		c.Feed().Publish()
		return decode(t, c.Feed().Frame().Data).Seq == ref.Seq()
	}, 30*time.Second, 50*time.Millisecond)

	s := decode(t, c.Feed().Frame().Data)
	var asks, bids [][2]int64
	ref.Depth(5, func(side engine.Side, p, q int64) {
		if side == engine.Sell {
			asks = append(asks, [2]int64{p, q})
		} else {
			bids = append(bids, [2]int64{p, q})
		}
	})
	require.Equal(t, asks, s.Asks)
	require.Equal(t, bids, s.Bids)
}
