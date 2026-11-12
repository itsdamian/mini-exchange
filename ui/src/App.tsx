import { DepthLadder } from "./components/DepthLadder";
import { LatencyPanel } from "./components/LatencyPanel";
import { TradesList } from "./components/TradesList";
import { formatTime } from "./format";
import type { Connection } from "./types";
import { useLatency } from "./useLatency";
import { useMarketData } from "./useMarketData";

const LABEL: Record<Connection, string> = {
  connecting: "connecting…",
  live: "live",
  stale: "stale: no data",
  reconnecting: "disconnected, reconnecting…",
};

function wsUrl(): string {
  const scheme = window.location.protocol === "https:" ? "wss" : "ws";
  return `${scheme}://${window.location.host}/ws`;
}

/** Price per tick. The default matches the bot (0.01); ?tick=1 shows raw ticks. */
function tickFromQuery(): number {
  const t = Number(new URLSearchParams(window.location.search).get("tick"));
  return Number.isFinite(t) && t > 0 ? t : 0.01;
}

export function App() {
  const { snapshot, connection, silentMs, frames } = useMarketData(wsUrl());
  const latency = useLatency("/api/latency");
  const tick = tickFromQuery();

  return (
    <main>
      <header>
        <h1>Mini Exchange{snapshot ? <span className="symbol"> {snapshot.symbol}</span> : null}</h1>
        <p className={`status ${connection}`} role="status" aria-live="polite">
          <span className="dot" aria-hidden="true" /> {LABEL[connection]}
          {snapshot && connection !== "reconnecting" ? (
            <span className="dim">
              {" "}
              · seq {snapshot.seq.toLocaleString("en-US")} · {formatTime(snapshot.ts_ns)} · {frames.toLocaleString("en-US")} frames
              {silentMs !== null && silentMs > 1000 ? ` · silent ${Math.round(silentMs / 1000)} s` : ""}
            </span>
          ) : null}
        </p>
      </header>
      <div className="grid">
        {snapshot ? <DepthLadder snapshot={snapshot} tick={tick} /> : <section className="panel">Waiting for the first frame…</section>}
        <div className="stack">
          <LatencyPanel latency={latency} />
          <TradesList trades={snapshot?.trades ?? []} tick={tick} />
        </div>
      </div>
      <footer className="dim">
        A frame is the whole book state and arrives at most every 100 ms. Prices are ticks × {tick}.
      </footer>
    </main>
  );
}
