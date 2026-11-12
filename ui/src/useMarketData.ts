import { useEffect, useRef, useState } from "react";
import { parseSnapshot } from "./parse";
import type { Connection, Snapshot } from "./types";

export interface MarketData {
  snapshot: Snapshot | null;
  connection: Connection;
  /** milliseconds since the last frame arrived (null before the first) */
  silentMs: number | null;
  /** frames received in this page's lifetime */
  frames: number;
}

/** A feed that sends nothing for this long is shown as stale (the server sends at least a frame per change, at most one per 100 ms). */
export const STALE_AFTER_MS = 3000;

const MIN_BACKOFF = 500;
const MAX_BACKOFF = 10_000;

/**
 * useMarketData keeps one WebSocket to the market data feed, reconnecting with
 * exponential backoff. Every frame is the whole state, so after a reconnect there
 * is nothing to resynchronise: the next frame replaces the screen.
 */
export function useMarketData(url: string): MarketData {
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [connected, setConnected] = useState(false);
  const [frames, setFrames] = useState(0);
  const [now, setNow] = useState(() => Date.now());
  const lastFrame = useRef<number | null>(null);

  useEffect(() => {
    let ws: WebSocket | null = null;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let backoff = MIN_BACKOFF;
    let closed = false;

    const connect = () => {
      ws = new WebSocket(url);
      ws.onopen = () => {
        backoff = MIN_BACKOFF;
        setConnected(true);
      };
      ws.onmessage = (ev: MessageEvent) => {
        if (typeof ev.data !== "string") return;
        const s = parseSnapshot(ev.data);
        if (!s) return; // not a snapshot we understand: ignore, never crash the page
        lastFrame.current = Date.now();
        setSnapshot(s);
        setFrames((n) => n + 1);
      };
      ws.onclose = () => {
        setConnected(false);
        if (closed) return;
        timer = setTimeout(connect, backoff);
        backoff = Math.min(backoff * 2, MAX_BACKOFF);
      };
      ws.onerror = () => ws?.close();
    };
    connect();
    return () => {
      closed = true;
      clearTimeout(timer);
      ws?.close();
    };
  }, [url]);

  // A clock for the "silent for" figure; it also re-evaluates staleness.
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 500);
    return () => clearInterval(id);
  }, []);

  const silentMs = lastFrame.current === null ? null : Math.max(0, now - lastFrame.current);
  let connection: Connection;
  if (!connected) connection = snapshot ? "reconnecting" : "connecting";
  else if (silentMs !== null && silentMs > STALE_AFTER_MS) connection = "stale";
  else connection = snapshot ? "live" : "connecting";
  return { snapshot, connection, silentMs, frames };
}
