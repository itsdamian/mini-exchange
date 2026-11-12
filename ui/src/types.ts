/** [price in ticks, quantity in units] */
export type Level = [number, number];

export interface Trade {
  seq: number;
  price: number;
  qty: number;
  /** side of the taker */
  side: "buy" | "sell";
  /** engine timestamp, nanoseconds since the epoch (JSON numbers keep it to about 0.25 microseconds) */
  ts_ns: number;
}

/** One message of the market data feed: the whole current state, not a delta. */
export interface Snapshot {
  type: "snapshot";
  symbol: string;
  seq: number;
  ts_ns: number;
  /** best (lowest) ask first */
  asks: Level[];
  /** best (highest) bid first */
  bids: Level[];
  /** oldest first, newest last */
  trades: Trade[];
}

export interface LatencyPoint {
  t: number;
  p50_us: number;
  p99_us: number;
}

/** Response of GET /api/latency: gateway handler time for PlaceOrder. */
export interface Latency {
  ok: boolean;
  error?: string;
  window_seconds?: number;
  requests_per_second?: number;
  p50_us?: number;
  p99_us?: number;
  p999_us?: number;
  series?: LatencyPoint[];
}

export type Connection = "connecting" | "live" | "stale" | "reconnecting";
