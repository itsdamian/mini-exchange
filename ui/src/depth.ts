import type { Level, Snapshot } from "./types";

export interface Row {
  price: number;
  qty: number;
  /** quantity from the best price down to this row */
  cumulative: number;
  /** bar width, 0 to 1, relative to the largest cumulative quantity shown */
  width: number;
}

export interface Ladder {
  /** worst ask first, best ask last: the order they are drawn above the spread */
  asks: Row[];
  /** best bid first */
  bids: Row[];
  bestBid?: number;
  bestAsk?: number;
  spread?: number;
  mid?: number;
  /** true when the book is crossed (a bid at or above an ask), which a consistent feed never shows */
  crossed: boolean;
}

function rows(levels: Level[], max: number): Row[] {
  let cum = 0;
  return levels.map(([price, qty]) => {
    cum += qty;
    return { price, qty, cumulative: cum, width: max > 0 ? cum / max : 0 };
  });
}

/** buildLadder turns a snapshot into the rows the depth view draws, limited to `depth` levels per side. */
export function buildLadder(s: Snapshot, depth: number): Ladder {
  const asks = s.asks.slice(0, depth);
  const bids = s.bids.slice(0, depth);
  const total = (ls: Level[]) => ls.reduce((a, [, q]) => a + q, 0);
  const max = Math.max(total(asks), total(bids));
  const bestAsk = asks[0]?.[0];
  const bestBid = bids[0]?.[0];
  const both = bestAsk !== undefined && bestBid !== undefined;
  return {
    asks: rows(asks, max).reverse(),
    bids: rows(bids, max),
    bestBid,
    bestAsk,
    spread: both ? bestAsk - bestBid : undefined,
    mid: both ? (bestAsk + bestBid) / 2 : undefined,
    crossed: both && bestBid >= bestAsk,
  };
}
