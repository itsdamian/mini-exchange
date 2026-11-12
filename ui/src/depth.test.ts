import { buildLadder } from "./depth";
import type { Snapshot } from "./types";

const snap = (asks: [number, number][], bids: [number, number][]): Snapshot => ({
  type: "snapshot", symbol: "BTC-USD", seq: 1, ts_ns: 0, asks, bids, trades: [],
});

describe("buildLadder", () => {
  const l = buildLadder(snap([[101, 2], [102, 3], [103, 5]], [[100, 4], [99, 1]]), 10);

  it("draws asks worst first and bids best first", () => {
    expect(l.asks.map((r) => r.price)).toEqual([103, 102, 101]);
    expect(l.bids.map((r) => r.price)).toEqual([100, 99]);
  });
  it("accumulates from the best price outwards", () => {
    expect(l.asks.map((r) => r.cumulative)).toEqual([10, 5, 2]);
    expect(l.bids.map((r) => r.cumulative)).toEqual([4, 5]);
  });
  it("scales bars to the largest cumulative size on screen", () => {
    expect(l.asks[0]?.width).toBe(1);
    expect(l.bids[1]?.width).toBe(0.5);
  });
  it("computes spread and mid", () => {
    expect(l.bestBid).toBe(100);
    expect(l.bestAsk).toBe(101);
    expect(l.spread).toBe(1);
    expect(l.mid).toBe(100.5);
    expect(l.crossed).toBe(false);
  });
  it("limits the depth per side", () => {
    const d = buildLadder(snap([[101, 1], [102, 1], [103, 1]], [[100, 1], [99, 1], [98, 1]]), 2);
    expect(d.asks).toHaveLength(2);
    expect(d.bids).toHaveLength(2);
  });
  it("copes with one side empty", () => {
    const d = buildLadder(snap([], [[100, 1]]), 10);
    expect(d.spread).toBeUndefined();
    expect(d.mid).toBeUndefined();
    expect(d.crossed).toBe(false);
  });
  it("copes with an empty book", () => {
    const d = buildLadder(snap([], []), 10);
    expect(d.asks).toEqual([]);
    expect(d.bids).toEqual([]);
  });
  it("flags a crossed book", () => {
    expect(buildLadder(snap([[100, 1]], [[101, 1]]), 10).crossed).toBe(true);
  });
});
