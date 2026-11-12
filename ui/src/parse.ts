import type { Latency, Level, Snapshot, Trade } from "./types";

const isNum = (v: unknown): v is number => typeof v === "number" && Number.isFinite(v);

function levels(v: unknown): Level[] | null {
  if (!Array.isArray(v)) return null;
  const out: Level[] = [];
  for (const l of v) {
    if (!Array.isArray(l) || l.length !== 2 || !isNum(l[0]) || !isNum(l[1])) return null;
    out.push([l[0], l[1]]);
  }
  return out;
}

function trades(v: unknown): Trade[] | null {
  if (!Array.isArray(v)) return null;
  const out: Trade[] = [];
  for (const t of v) {
    if (typeof t !== "object" || t === null) return null;
    const o = t as Record<string, unknown>;
    if (!isNum(o.seq) || !isNum(o.price) || !isNum(o.qty) || !isNum(o.ts_ns)) return null;
    if (o.side !== "buy" && o.side !== "sell") return null;
    out.push({ seq: o.seq, price: o.price, qty: o.qty, side: o.side, ts_ns: o.ts_ns });
  }
  return out;
}

/** parseSnapshot validates a feed message; it returns null for anything that is not a well-formed snapshot. */
export function parseSnapshot(text: string): Snapshot | null {
  let raw: unknown;
  try {
    raw = JSON.parse(text);
  } catch {
    return null;
  }
  if (typeof raw !== "object" || raw === null) return null;
  const o = raw as Record<string, unknown>;
  if (o.type !== "snapshot" || typeof o.symbol !== "string" || !isNum(o.seq) || !isNum(o.ts_ns)) return null;
  const asks = levels(o.asks);
  const bids = levels(o.bids);
  const tr = trades(o.trades);
  if (!asks || !bids || !tr) return null;
  return { type: "snapshot", symbol: o.symbol, seq: o.seq, ts_ns: o.ts_ns, asks, bids, trades: tr };
}

/** parseLatency is lenient on purpose: the endpoint may be missing or return an error object. */
export function parseLatency(raw: unknown): Latency {
  if (typeof raw !== "object" || raw === null) return { ok: false, error: "unexpected response" };
  const o = raw as Record<string, unknown>;
  const out: Latency = { ok: o.ok === true };
  if (typeof o.error === "string") out.error = o.error;
  for (const k of ["window_seconds", "requests_per_second", "p50_us", "p99_us", "p999_us"] as const) {
    const v = o[k];
    if (isNum(v)) out[k] = v;
  }
  if (Array.isArray(o.series)) {
    out.series = o.series.flatMap((p) => {
      const q = p as Record<string, unknown>;
      return isNum(q?.t) && isNum(q?.p50_us) && isNum(q?.p99_us) ? [{ t: q.t, p50_us: q.p50_us, p99_us: q.p99_us }] : [];
    });
  }
  return out;
}
