/** Ticks to a price string. `tick` is the price per tick (0.01 for the default bot and prices in dollars). */
export function formatPrice(ticks: number, tick: number): string {
  const decimals = Math.max(0, Math.round(-Math.log10(tick)));
  return (ticks * tick).toLocaleString("en-US", { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
}

export function formatQty(qty: number): string {
  return qty.toLocaleString("en-US");
}

/** Engine timestamp (ns since the epoch) as HH:MM:SS.mmm in local time. */
export function formatTime(tsNs: number): string {
  const d = new Date(tsNs / 1e6);
  const p = (n: number, w = 2) => String(n).padStart(w, "0");
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${p(d.getMilliseconds(), 3)}`;
}

/** Microseconds as "123 µs" below a millisecond and "4.56 ms" above. */
export function formatMicros(us: number | undefined): string {
  if (us === undefined || !Number.isFinite(us)) return "–";
  if (us < 1000) return `${Math.round(us)} µs`;
  if (us < 100_000) return `${(us / 1000).toFixed(2)} ms`;
  return `${Math.round(us / 1000)} ms`;
}
