# Plan: phase 8 (optimization report)

- [x] Experiment 4 (and the phase 1 gap): open-addressing id table in the engine, before/after (22 to 27% faster, allocations gone)
- [x] Experiments 1, 2, 3, 5, 8 and the costs of experiment 4 as isolated micro-benchmarks (`internal/experiments`)
- [x] Experiment 3 at the level of the whole gateway: `-queue-channels`, interleaved A/B with CPU time
- [x] Experiment 6: GC settings on the gateway; experiment 7: Kafka linger against publish lag; experiment 9: engine thread pinning (`-lock-engine-thread`)
- [x] Experiment 8: reused protobuf messages in the publisher (adopted)
- [x] CPU profile of the gateway under load (`-pprof-addr`, `PPROF=1`)
- [x] The tail question of phases 2 to 4: host jitter control (`TestHostJitter`) together with the GC and pinning experiments
- [x] `scripts/bench-all.sh` (one script, `QUICK=1` for a short run, `STAGES` to select), `scripts/bench-summary.py`; the QUICK run was verified end to end
- [x] ADR 0013, BENCHMARKS section 8, README, runbook
- [ ] FR-9 hot standby engine: not started (optional)
- Acceptance: BENCHMARKS.md complete and reproducible with one script.
