package gateway

import (
	"fmt"

	"github.com/itsdamian/mini-exchange/internal/engine"

	exchangev1 "github.com/itsdamian/mini-exchange/gen/exchange/v1"
)

// MaxClientOrderIDLen bounds the idempotency key.
const MaxClientOrderIDLen = 64

// validateClientOrderID checks the mandatory idempotency key: 1 to 64
// printable ASCII characters (no spaces or control characters).
func validateClientOrderID(id string) error {
	if id == "" {
		return fmt.Errorf("client_order_id is required")
	}
	if len(id) > MaxClientOrderIDLen {
		return fmt.Errorf("client_order_id longer than %d characters", MaxClientOrderIDLen)
	}
	for i := 0; i < len(id); i++ {
		if id[i] <= 0x20 || id[i] >= 0x7f {
			return fmt.Errorf("client_order_id must be printable ASCII without spaces")
		}
	}
	return nil
}

// buildPlace validates the protocol-level shape of a PlaceOrderRequest and
// converts it to an engine command. Semantic checks that have typed reject
// reasons (price and quantity ranges) are deliberately left to the engine so
// clients get RejectReason values rather than transport errors.
func buildPlace(req *exchangev1.PlaceOrderRequest, symbol string) (engine.Command, error) {
	if err := validateClientOrderID(req.GetClientOrderId()); err != nil {
		return engine.Command{}, err
	}
	if req.GetAccountId() == 0 {
		return engine.Command{}, fmt.Errorf("account_id is required")
	}
	if req.GetSymbol() != symbol {
		return engine.Command{}, fmt.Errorf("unknown symbol %q", req.GetSymbol())
	}
	side := toEngineSide(req.GetSide())
	if side == 0 {
		return engine.Command{}, fmt.Errorf("side is required")
	}
	typ := toEngineType(req.GetType())
	if typ == 0 {
		return engine.Command{}, fmt.Errorf("type is required")
	}
	tif := toEngineTIF(req.GetTimeInForce())
	if typ == engine.Limit && tif == 0 {
		return engine.Command{}, fmt.Errorf("time_in_force is required for LIMIT orders")
	}
	return engine.Command{
		Kind: engine.CmdPlace, Side: side, Type: typ, TIF: tif,
		AccountID: req.GetAccountId(), Price: req.GetPrice(), Qty: req.GetQuantity(),
	}, nil
}

func validateCancel(req *exchangev1.CancelOrderRequest, symbol string) error {
	if err := validateClientOrderID(req.GetClientOrderId()); err != nil {
		return err
	}
	if req.GetAccountId() == 0 {
		return fmt.Errorf("account_id is required")
	}
	if req.GetSymbol() != symbol {
		return fmt.Errorf("unknown symbol %q", req.GetSymbol())
	}
	return nil
}
