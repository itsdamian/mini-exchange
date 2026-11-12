import { formatMicros, formatPrice, formatQty, formatTime } from "./format";

describe("formatPrice", () => {
  it("shows ticks as a price with the decimals of the tick size", () => {
    expect(formatPrice(6_700_005, 0.01)).toBe("67,000.05");
    expect(formatPrice(6_700_000, 0.01)).toBe("67,000.00");
    expect(formatPrice(100_000, 1)).toBe("100,000");
    expect(formatPrice(5, 0.001)).toBe("0.005");
  });
});

describe("formatQty", () => {
  it("groups thousands", () => expect(formatQty(1_234_567)).toBe("1,234,567"));
});

describe("formatMicros", () => {
  it("picks the unit", () => {
    expect(formatMicros(82)).toBe("82 µs");
    expect(formatMicros(999.4)).toBe("999 µs");
    expect(formatMicros(1500)).toBe("1.50 ms");
    expect(formatMicros(4_427)).toBe("4.43 ms");
    expect(formatMicros(250_000)).toBe("250 ms");
  });
  it("shows a dash for no data", () => {
    expect(formatMicros(undefined)).toBe("–");
    expect(formatMicros(Number.NaN)).toBe("–");
  });
});

describe("formatTime", () => {
  it("shows hours to milliseconds in local time", () => {
    const d = new Date(2026, 10, 12, 9, 8, 7, 45);
    expect(formatTime(d.getTime() * 1e6)).toBe("09:08:07.045");
  });
});
