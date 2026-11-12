import { useEffect, useState } from "react";
import { parseLatency } from "./parse";
import type { Latency } from "./types";

/** usePolledLatency polls GET /api/latency once per second. */
export function useLatency(url: string, windowSeconds = 10): Latency {
  const [latency, setLatency] = useState<Latency>({ ok: false, error: "waiting for the first answer" });

  useEffect(() => {
    let stop = false;
    const tick = async () => {
      try {
        const r = await fetch(`${url}?window=${windowSeconds}`, { cache: "no-store" });
        if (!r.ok) throw new Error(r.status === 404 ? "latency is not enabled on the server (-gateway-metrics)" : `HTTP ${r.status}`);
        if (!stop) setLatency(parseLatency(await r.json()));
      } catch (e) {
        if (!stop) setLatency({ ok: false, error: e instanceof Error ? e.message : "request failed" });
      }
    };
    void tick();
    const id = setInterval(() => void tick(), 1000);
    return () => {
      stop = true;
      clearInterval(id);
    };
  }, [url, windowSeconds]);

  return latency;
}
