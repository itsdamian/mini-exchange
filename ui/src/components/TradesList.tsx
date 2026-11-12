import { formatPrice, formatQty, formatTime } from "../format";
import type { Trade } from "../types";

/** The most recent trades, newest first, coloured by the side of the taker. */
export function TradesList({ trades, tick, limit = 20 }: { trades: Trade[]; tick: number; limit?: number }) {
  const shown = trades.slice(-limit).reverse();
  return (
    <section aria-labelledby="trades-title" className="panel">
      <h2 id="trades-title">Recent trades</h2>
      {shown.length === 0 ? (
        <p className="dim">No trades yet.</p>
      ) : (
        <table className="trades">
          <thead>
            <tr>
              <th>Time</th>
              <th className="num">Price</th>
              <th className="num">Size</th>
            </tr>
          </thead>
          <tbody>
            {shown.map((t) => (
              <tr key={t.seq} className={t.side === "buy" ? "buy" : "sell"}>
                <td className="dim">{formatTime(t.ts_ns)}</td>
                <td className="num price">
                  <span className="arrow" aria-label={`taker ${t.side}`}>
                    {t.side === "buy" ? "▲" : "▼"}
                  </span>{" "}
                  {formatPrice(t.price, tick)}
                </td>
                <td className="num">{formatQty(t.qty)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}
