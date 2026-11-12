import { formatMicros } from "../format";
import type { Latency, LatencyPoint } from "../types";

function Sparkline({ points }: { points: LatencyPoint[] }) {
  if (points.length < 2) return null;
  const w = 240;
  const h = 48;
  const max = Math.max(...points.map((p) => p.p99_us), 1);
  const x = (i: number) => (i / (points.length - 1)) * w;
  const y = (v: number) => h - (v / max) * (h - 4) - 2;
  const path = (key: "p50_us" | "p99_us") => points.map((p, i) => `${i === 0 ? "M" : "L"}${x(i).toFixed(1)},${y(p[key]).toFixed(1)}`).join(" ");
  return (
    <svg className="spark" viewBox={`0 0 ${w} ${h}`} role="img" aria-label={`p50 and p99 over the last ${points.length} seconds, p99 peak ${formatMicros(max)}`}>
      <path d={path("p99_us")} className="p99" fill="none" />
      <path d={path("p50_us")} className="p50" fill="none" />
    </svg>
  );
}

/**
 * Live latency percentiles of the gateway's PlaceOrder handler. They are estimates from
 * histogram buckets and measure the time inside the gateway, not what a client sees.
 */
export function LatencyPanel({ latency }: { latency: Latency }) {
  return (
    <section aria-labelledby="latency-title" className="panel">
      <h2 id="latency-title">Gateway latency</h2>
      {latency.ok ? (
        <>
          <dl className="tiles">
            <div>
              <dt>p50</dt>
              <dd>{formatMicros(latency.p50_us)}</dd>
            </div>
            <div>
              <dt>p99</dt>
              <dd>{formatMicros(latency.p99_us)}</dd>
            </div>
            <div>
              <dt>p99.9</dt>
              <dd>{formatMicros(latency.p999_us)}</dd>
            </div>
            <div>
              <dt>orders/s</dt>
              <dd>{Math.round(latency.requests_per_second ?? 0).toLocaleString("en-US")}</dd>
            </div>
          </dl>
          <Sparkline points={latency.series ?? []} />
          <p className="note dim">
            PlaceOrder handler time in the gateway over the last {Math.round(latency.window_seconds ?? 0)} s, estimated from histogram buckets. Not
            the latency a client sees.
          </p>
        </>
      ) : (
        <p className="dim" role="status">
          Latency unavailable: {latency.error ?? "no data"}
        </p>
      )}
    </section>
  );
}
