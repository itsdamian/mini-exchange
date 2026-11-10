package engine

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDepthAggregatesAndOrdersLevels(t *testing.T) {
	b := NewBook(Config{MaxOrders: 64, MaxLevels: 16})
	place := func(id uint64, side Side, price, qty int64) {
		c := Command{Kind: CmdPlace, Side: side, Type: Limit, TIF: GTC, OrderID: id, AccountID: id, Price: price, Qty: qty}
		b.Apply(&c, nil)
	}
	place(1, Buy, 99, 5)
	place(2, Buy, 99, 7) // same level: aggregated
	place(3, Buy, 98, 1)
	place(4, Buy, 97, 1)
	place(5, Sell, 101, 2)
	place(6, Sell, 102, 3)
	place(7, Sell, 103, 4)

	type lv struct {
		side       Side
		price, qty int64
	}
	var got []lv
	b.Depth(2, func(s Side, p, q int64) { got = append(got, lv{s, p, q}) })
	require.Equal(t, []lv{{Sell, 101, 2}, {Sell, 102, 3}, {Buy, 99, 12}, {Buy, 98, 1}}, got, "best first, n per side")

	got = nil
	b.Depth(10, func(s Side, p, q int64) { got = append(got, lv{s, p, q}) })
	require.Len(t, got, 6)
}
