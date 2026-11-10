// Package experiments holds the phase 8 micro-experiments (requirements
// section 5.3): small, isolated benchmarks that compare a design choice of the
// engine against its alternatives. Nothing here is imported by the system;
// scripts/bench-all.sh runs them and BENCHMARKS.md reports them.
//
// They are deliberately narrow: each measures one mechanism in isolation (the
// price-level index, the per-level order queue, the hand-off between
// goroutines, ...), not the whole matcher, and says so. The whole-system
// effect of a change is measured separately with the load test.
package experiments
