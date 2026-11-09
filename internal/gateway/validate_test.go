package gateway

import (
	"testing"

	"github.com/stretchr/testify/require"

	exchangev1 "github.com/itsdamian/mini-exchange/gen/exchange/v1"
	"github.com/itsdamian/mini-exchange/internal/engine"
)

// FuzzBuildPlace checks that validation never panics and that anything it
// accepts is a well-formed engine command with a legal idempotency key.
func FuzzBuildPlace(f *testing.F) {
	f.Add("id-1", uint64(1), "BTC-USD", int32(1), int32(1), int32(1), int64(100), int64(5))
	f.Add("", uint64(0), "", int32(0), int32(0), int32(0), int64(0), int64(0))
	f.Add("a b", uint64(1), "BTC-USD", int32(99), int32(-1), int32(2), int64(-5), int64(-1))
	f.Fuzz(func(t *testing.T, id string, acct uint64, sym string, side, typ, tif int32, price, qty int64) {
		req := &exchangev1.PlaceOrderRequest{
			ClientOrderId: id, AccountId: acct, Symbol: sym,
			Side: exchangev1.Side(side), Type: exchangev1.OrderType(typ), TimeInForce: exchangev1.TimeInForce(tif),
			Price: price, Quantity: qty,
		}
		cmd, err := buildPlace(req, "BTC-USD")
		if err != nil {
			return
		}
		require.NoError(t, validateClientOrderID(id))
		require.NotZero(t, acct)
		require.Equal(t, "BTC-USD", sym)
		require.Equal(t, engine.CmdPlace, cmd.Kind)
		require.Contains(t, []engine.Side{engine.Buy, engine.Sell}, cmd.Side)
		require.Contains(t, []engine.OrderType{engine.Limit, engine.Market}, cmd.Type)
		if cmd.Type == engine.Limit {
			require.Contains(t, []engine.TimeInForce{engine.GTC, engine.IOC}, cmd.TIF)
		}
		require.Equal(t, price, cmd.Price)
		require.Equal(t, qty, cmd.Qty)
	})
}
