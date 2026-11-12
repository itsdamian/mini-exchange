import { buildLadder, type Row } from "../depth";
import { formatPrice, formatQty } from "../format";
import type { Snapshot } from "../types";

interface Props {
  snapshot: Snapshot;
  tick: number;
  depth?: number;
}

function Side({ rows, side, tick }: { rows: Row[]; side: "ask" | "bid"; tick: number }) {
  return (
    <>
      {rows.map((r) => (
        <tr key={`${side}-${r.price}`} className={`level ${side}`}>
          <td className="price">{formatPrice(r.price, tick)}</td>
          <td className="num">{formatQty(r.qty)}</td>
          <td className="num dim">{formatQty(r.cumulative)}</td>
          <td className="bar" aria-hidden="true">
            <span style={{ width: `${Math.round(r.width * 100)}%` }} />
          </td>
        </tr>
      ))}
    </>
  );
}

/** The order book: asks above the spread (worst first), bids below (best first), with cumulative size bars. */
export function DepthLadder({ snapshot, tick, depth = 10 }: Props) {
  const l = buildLadder(snapshot, depth);
  return (
    <section aria-labelledby="depth-title" className="panel">
      <h2 id="depth-title">Depth</h2>
      <table className="ladder">
        <thead>
          <tr>
            <th>Price</th>
            <th className="num">Size</th>
            <th className="num">Total</th>
            <th aria-hidden="true" />
          </tr>
        </thead>
        <tbody>
          <Side rows={l.asks} side="ask" tick={tick} />
          <tr className="spread">
            <td colSpan={4}>
              {l.spread === undefined ? (
                "no two-sided market"
              ) : (
                <>
                  spread <strong>{formatPrice(l.spread, tick)}</strong> · mid <strong>{formatPrice(l.mid ?? 0, tick)}</strong>
                  {l.crossed && <span className="warn"> · book is crossed</span>}
                </>
              )}
            </td>
          </tr>
          <Side rows={l.bids} side="bid" tick={tick} />
        </tbody>
      </table>
    </section>
  );
}
