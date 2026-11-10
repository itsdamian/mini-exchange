#!/usr/bin/env python3
"""Summarise gateway load-test runs by tag: one row per run with the client latency
percentiles (from the loadgen JSON) and, from the gateway's metrics file, the p50 / p99
of the event-log publish lag and the number of GC cycles. Usage:

    scripts/bench-summary.py bench/results TAG [TAG ...]

Quantiles taken from Prometheus histogram buckets are the UPPER BOUND of the bucket that
holds the quantile (the buckets are powers of two), so they overstate by up to 2x.
"""
import glob, json, os, re, sys

def hist_quantile(lines, name, q):
    buckets = []
    for ln in lines:
        m = re.match(r'%s_bucket\{le="([^"]+)"\} (\S+)' % re.escape(name), ln)
        if m and m.group(1) != "+Inf":
            buckets.append((float(m.group(1)), float(m.group(2))))
    total = None
    for ln in lines:
        m = re.match(r'%s_count (\S+)' % re.escape(name), ln)
        if m:
            total = float(m.group(1))
    if not buckets or not total:
        return None
    buckets.sort()
    for le, c in buckets:
        if c >= q * total:
            return le
    return buckets[-1][0]

def gc_cycles(lines):
    for ln in lines:
        m = re.match(r'go_gc_duration_seconds_count (\S+)', ln)
        if m:
            return int(float(m.group(1)))
    return None

def main():
    root, tags = sys.argv[1], sys.argv[2:]
    print("| tag | run | p50 us | p90 us | p99 us | p99.9 us | max us | GC cycles | publish lag p50 / p99 (ms, bucket bound) |")
    print("|---|---|---|---|---|---|---|---|---|")
    for tag in tags:
        for f in sorted(glob.glob(os.path.join(root, "gateway-%s-*rps-*-run*.json" % tag))):
            m = re.search(r"-run(\d+)\.json$", f)
            run = m.group(1) if m else "?"
            d = json.load(open(f))
            l = d["latency_us_from_intended_send_time"]
            mf = f.replace("gateway-%s-" % tag, "gateway-metrics-%s-" % tag).replace(".json", ".txt")
            lag = gc = "-"
            if os.path.exists(mf):
                lines = open(mf).read().splitlines()
                p50 = hist_quantile(lines, "mx_eventlog_publish_lag_seconds", 0.5)
                p99 = hist_quantile(lines, "mx_eventlog_publish_lag_seconds", 0.99)
                if p50 is not None:
                    lag = "%.2f / %.2f" % (p50 * 1000, p99 * 1000)
                g = gc_cycles(lines)
                gc = str(g) if g is not None else "-"
            print("| %s | %s | %d | %d | %d | %d | %d | %s | %s |" % (tag, run, l["P50"], l["P90"], l["P99"], l["P999"], l["Max"], gc, lag))

main()
