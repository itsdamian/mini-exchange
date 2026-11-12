import { act, render, screen, within } from "@testing-library/react";
import { App } from "./App";

class FakeSocket {
  static instances: FakeSocket[] = [];
  onopen: (() => void) | null = null;
  onmessage: ((e: { data: unknown }) => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: (() => void) | null = null;
  closed = false;
  constructor(public url: string) {
    FakeSocket.instances.push(this);
  }
  close() {
    this.closed = true;
    this.onclose?.();
  }
  open() {
    this.onopen?.();
  }
  send(data: unknown) {
    this.onmessage?.({ data });
  }
}

const frame = (seq: number, extra: Record<string, unknown> = {}) =>
  JSON.stringify({
    type: "snapshot", symbol: "BTC-USD", seq, ts_ns: Date.now() * 1e6,
    asks: [[6_700_105, 20], [6_700_110, 5]], bids: [[6_700_095, 30], [6_700_090, 10]],
    trades: [
      { seq: seq - 2, price: 6_700_100, qty: 3, side: "buy", ts_ns: Date.now() * 1e6 },
      { seq: seq - 1, price: 6_700_098, qty: 7, side: "sell", ts_ns: Date.now() * 1e6 },
    ],
    ...extra,
  });

let latencyBody: unknown;
let latencyStatus = 200;

beforeEach(() => {
  vi.useFakeTimers();
  FakeSocket.instances = [];
  latencyBody = { ok: true, p50_us: 82, p99_us: 4_427, p999_us: 12_300, requests_per_second: 9_876, window_seconds: 10, series: [{ t: 1, p50_us: 80, p99_us: 300 }, { t: 2, p50_us: 90, p99_us: 500 }] };
  latencyStatus = 200;
  vi.stubGlobal("WebSocket", FakeSocket);
  vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify(latencyBody), { status: latencyStatus })));
});
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

const sock = () => FakeSocket.instances[FakeSocket.instances.length - 1]!;
const settle = () => act(async () => { await vi.advanceTimersByTimeAsync(0); });

describe("App", () => {
  it("connects to /ws on the same host and says it is connecting", () => {
    render(<App />);
    expect(sock().url).toMatch(/^wss?:\/\/.+\/ws$/);
    expect(screen.getByText(/connecting/)).toBeInTheDocument();
    expect(screen.getByText(/Waiting for the first frame/)).toBeInTheDocument();
  });

  it("shows depth, spread, trades and a live status after the first frame", async () => {
    render(<App />);
    act(() => { sock().open(); sock().send(frame(100)); });
    await settle();

    expect(screen.getByText("BTC-USD")).toBeInTheDocument();
    expect(screen.getByText(/live/)).toBeInTheDocument();

    const depth = screen.getByRole("region", { name: "Depth" });
    expect(within(depth).getByText("67,001.05")).toBeInTheDocument(); // best ask
    expect(within(depth).getByText("67,000.95")).toBeInTheDocument(); // best bid
    expect(within(depth).getByText("0.10")).toBeInTheDocument(); // spread of 10 ticks

    const trades = screen.getByRole("region", { name: "Recent trades" });
    const rows = within(trades).getAllByRole("row").slice(1);
    expect(rows).toHaveLength(2);
    expect(within(rows[0]!).getByText("67,000.98")).toBeInTheDocument(); // newest first
    expect(within(rows[1]!).getByText("67,001.00")).toBeInTheDocument();
  });

  it("shows the latency percentiles once /api/latency answers", async () => {
    render(<App />);
    await settle();
    const panel = screen.getByRole("region", { name: "Gateway latency" });
    expect(within(panel).getByText("82 µs")).toBeInTheDocument();
    expect(within(panel).getByText("4.43 ms")).toBeInTheDocument();
    expect(within(panel).getByText("12.30 ms")).toBeInTheDocument();
    expect(within(panel).getByText("9,876")).toBeInTheDocument();
    expect(within(panel).getByRole("img")).toBeInTheDocument(); // the sparkline
  });

  it("says why latency is unavailable instead of showing zeros", async () => {
    latencyStatus = 404;
    render(<App />);
    await settle();
    expect(screen.getByRole("region", { name: "Gateway latency" })).toHaveTextContent(/not enabled on the server/);
  });

  it("goes stale when the feed goes quiet, and live again on the next frame", async () => {
    render(<App />);
    act(() => { sock().open(); sock().send(frame(1)); });
    await settle();
    await act(async () => { await vi.advanceTimersByTimeAsync(4000); });
    expect(screen.getByText(/stale/)).toBeInTheDocument();
    act(() => sock().send(frame(2)));
    await settle();
    expect(screen.getByText(/live/)).toBeInTheDocument();
  });

  it("reconnects with backoff after the connection drops and keeps the last book on screen", async () => {
    render(<App />);
    act(() => { sock().open(); sock().send(frame(5)); });
    await settle();
    const first = sock();
    act(() => first.onclose?.());
    expect(screen.getByText(/reconnecting/)).toBeInTheDocument();
    expect(screen.getByRole("region", { name: "Depth" })).toBeInTheDocument();
    await act(async () => { await vi.advanceTimersByTimeAsync(600); });
    expect(FakeSocket.instances.length).toBe(2);
    act(() => { sock().open(); sock().send(frame(9)); });
    await settle();
    expect(screen.getByText(/live/)).toBeInTheDocument();
  });

  it("ignores frames it does not understand", async () => {
    render(<App />);
    act(() => { sock().open(); sock().send("{not json"); sock().send(JSON.stringify({ type: "hello" })); sock().send(frame(7)); });
    await settle();
    expect(screen.getByRole("region", { name: "Depth" })).toBeInTheDocument();
  });

  it("closes the socket when the page goes away", () => {
    const { unmount } = render(<App />);
    unmount();
    expect(sock().closed).toBe(true);
  });
});
