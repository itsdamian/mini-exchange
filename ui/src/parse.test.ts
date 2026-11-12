import { parseLatency, parseSnapshot } from "./parse";

const good = {
  type: "snapshot", symbol: "BTC-USD", seq: 42, ts_ns: 1_794_000_000_000_000_000,
  asks: [[101, 2]], bids: [[100, 3]],
  trades: [{ seq: 41, price: 100, qty: 1, side: "buy", ts_ns: 1_794_000_000_000_000_000 }],
};

describe("parseSnapshot", () => {
  it("accepts what the server sends", () => {
    const s = parseSnapshot(JSON.stringify(good));
    expect(s?.symbol).toBe("BTC-USD");
    expect(s?.asks).toEqual([[101, 2]]);
    expect(s?.trades[0]?.side).toBe("buy");
  });
  it("accepts an empty book", () => {
    expect(parseSnapshot(JSON.stringify({ ...good, asks: [], bids: [], trades: [] }))).not.toBeNull();
  });
  it.each([
    ["not JSON", "{oops"],
    ["not an object", "42"],
    ["another message type", JSON.stringify({ ...good, type: "hello" })],
    ["a level that is not a pair", JSON.stringify({ ...good, asks: [[101]] })],
    ["a non-numeric price", JSON.stringify({ ...good, bids: [["x", 1]] })],
    ["a trade with a bad side", JSON.stringify({ ...good, trades: [{ ...good.trades[0], side: "up" }] })],
    ["a missing symbol", JSON.stringify({ ...good, symbol: undefined })],
    ["null", "null"],
  ])("rejects %s", (_name, text) => {
    expect(parseSnapshot(text)).toBeNull();
  });
});

describe("parseLatency", () => {
  it("reads a good answer", () => {
    const l = parseLatency({ ok: true, p50_us: 80, p99_us: 300, p999_us: 900, requests_per_second: 9876, window_seconds: 10, series: [{ t: 1, p50_us: 80, p99_us: 300 }] });
    expect(l.ok).toBe(true);
    expect(l.p99_us).toBe(300);
    expect(l.series).toHaveLength(1);
  });
  it("keeps the error of an unavailable answer", () => {
    expect(parseLatency({ ok: false, error: "waiting" })).toEqual({ ok: false, error: "waiting" });
  });
  it("survives garbage", () => {
    expect(parseLatency(null).ok).toBe(false);
    expect(parseLatency("x").ok).toBe(false);
    expect(parseLatency({ ok: true, series: [1, { t: "a" }, null] }).series).toEqual([]);
  });
});
