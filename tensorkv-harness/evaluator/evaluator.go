// Package evaluator provides the top-level Evaluate function that an automated
// evaluation framework (e.g. OpenEvolve/ShinkaEvolve/GEPA) calls to score a
// Cluster implementation against distributed systems invariants and performance
// benchmarks.
package evaluator

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/tensorkv/harness/benchmark"
	"github.com/tensorkv/harness/checkers"
	"github.com/tensorkv/harness/faultinjector"
	"github.com/tensorkv/harness/interfaces"
	"github.com/tensorkv/harness/recorder"
	"github.com/tensorkv/harness/workload"
)

// invariantDesc maps checker names to a one-sentence definition of the invariant
// being tested. Prepended to every [FAIL] line so an LLM reader sees both the
// rule and the evidence in a single line.
var invariantDesc = map[string]string{
	"phantom-reads":      "Get must only return values that were previously Put for this key.",
	"monotonic-reads":    "A client must never observe an older version of a key after reading a newer one.",
	"causal-consistency": "Causal consistency: read your writes, monotonic reads, monotonic writes, writes follow reads (session guarantees).",
	"durability":         "Values successfully Put before a node crash must survive on all remaining and restarted nodes.",
	"workload":           "The workload must run without infrastructure errors.",
	"performance":        "The cluster must sustain measurable throughput under the benchmark workload.",
}

// result accumulates pass/fail information for a single phase of the evaluation.
type result struct {
	phase string
	lines []string
	fail  string // non-empty if any invariant failed
}

func (r *result) pass(invariant string) {
	r.lines = append(r.lines, fmt.Sprintf("  [PASS] %s/%s", r.phase, invariant))
}

// failWith records a failure, prepending the invariant definition so an LLM
// reader understands the rule that was violated before seeing the evidence.
func (r *result) failWith(invariant, msg string) {
	desc := invariantDesc[invariant]
	var line string
	if desc != "" {
		line = fmt.Sprintf("  [FAIL] %s/%s: Invariant: %s Violation: %s", r.phase, invariant, desc, msg)
	} else {
		line = fmt.Sprintf("  [FAIL] %s/%s: %s", r.phase, invariant, msg)
	}
	r.lines = append(r.lines, line)
	if r.fail == "" {
		r.fail = msg
	}
}

// Config optionally shortens the evaluation for quick local runs.
type Config struct {
	Short bool // if true, phases use 5s workload and 10s perf (~30–40s total)
}

// Evaluate runs the full evaluation suite against the given Cluster.
func Evaluate(cluster interfaces.Cluster, nodeCount int) (float64, string) {
	return EvaluateWithConfig(cluster, nodeCount, Config{})
}

// EvaluateWithConfig runs the evaluation with optional short durations.
func EvaluateWithConfig(cluster interfaces.Cluster, nodeCount int, cfg Config) (float64, string) {
	phaseDuration := 30 * time.Second
	phaseRampUp := 3 * time.Second
	perfDuration := 60 * time.Second
	perfRampUp := 5 * time.Second
	if cfg.Short {
		phaseDuration = 5 * time.Second
		phaseRampUp = 1 * time.Second
		perfDuration = 10 * time.Second
		perfRampUp = 1 * time.Second
	}

	var sb strings.Builder
	sb.WriteString("=== TensorKV Harness Evaluation Report ===\n\n")
	sb.WriteString("Scoring: Phase1=0.25 Phase2=0.25 Phase3=0.25 Phase4=min(0.25, throughput/1000*0.25)\n\n")

	if err := cluster.Start(nodeCount); err != nil {
		msg := fmt.Sprintf("cluster failed to start: %v", err)
		sb.WriteString("[FATAL] " + msg + "\n")
		sb.WriteString("\n=== Score: 0.0000 — cluster failed to start ===\n")
		return 0.0, sb.String()
	}
	defer cluster.Shutdown() //nolint:errcheck

	sb.WriteString(fmt.Sprintf("Cluster started with %d nodes.\n\n", nodeCount))
	runStart := time.Now()
	if cfg.Short {
		fmt.Fprintf(os.Stderr, "Cluster started. Short mode: ~30–40s total.\n")
	} else {
		fmt.Fprintf(os.Stderr, "Cluster started. Full run: ~3–4 min. Use --short for quick check.\n")
	}
	fmt.Fprintf(os.Stderr, "[timing] start %v\n", runStart.Format("15:04:05.000"))

	baseConfig := workload.WorkloadConfig{
		NumClients:      8,
		NumKeys:         1000,
		ReadRatio:       0.9,
		KeyDistribution: "zipfian",
		ZipfianConstant: 0.99,
		ValueSize:       1024,              // 1 KB average
		ValueSizeMax:    interfaces.MaxValueSize, // 1 MB max
		Duration:        phaseDuration,
		RampUp:          phaseRampUp,
	}

	var totalScore float64

	// ------------------------------------------------------------------
	// Phase 1: HappyPath — no faults
	// ------------------------------------------------------------------
	phase1Start := time.Now()
	fmt.Fprintf(os.Stderr, "Phase 1: HappyPath (%s workload)...\n", phaseDuration)
	sb.WriteString("--- Phase 1: HappyPath (no faults) ---\n")
	{
		r := &result{phase: "HappyPath"}
		history, _, err := faultinjector.RunWithFaults(cluster, baseConfig, faultinjector.HappyPath())
		fmt.Fprintf(os.Stderr, "[timing] Phase 1 workload done in %v\n", time.Since(phase1Start))
		if err != nil {
			r.failWith("workload", err.Error())
		} else {
			rec := historyToRecorder(history)
			checkAll(r, rec, nil, nil, time.Time{}, cluster)
		}
		for _, l := range r.lines {
			sb.WriteString(l + "\n")
		}
		if r.fail == "" {
			totalScore += 0.25
			sb.WriteString(fmt.Sprintf("  Phase 1 score: +0.25  (running total: %.2f)\n", totalScore))
		} else {
			sb.WriteString(fmt.Sprintf("  Phase 1 score: +0.00  (running total: %.2f)\n", totalScore))
		}
	}
	fmt.Fprintf(os.Stderr, "[timing] Phase 1 total %v\n", time.Since(phase1Start))
	sb.WriteString("\n")

	// ------------------------------------------------------------------
	// Phase 2: CrashDuringLoad
	// ------------------------------------------------------------------
	phase2Start := time.Now()
	fmt.Fprintf(os.Stderr, "Phase 2: CrashDuringLoad (%s, node 1 killed/restarted)...\n", phaseDuration)
	sb.WriteString("--- Phase 2: CrashDuringLoad (node 1 killed at 30%, restarted at 60%) ---\n")
	{
		r := &result{phase: "CrashDuringLoad"}
		faults := faultinjector.CrashDuringLoad(baseConfig.Duration)
		crashTime := time.Now().Add(faults[0].At)

		history, _, err := faultinjector.RunWithFaults(cluster, baseConfig, faults)
		if err != nil {
			r.failWith("workload", err.Error())
		} else {
			rec := historyToRecorder(history)

			// Standard consistency checkers.
			checkConsistency(r, rec)

			// Durability: keys written before crash must survive.
			nodes := cluster.NodeIDs()
			surviving := make([]interfaces.NodeID, 0, len(nodes)-1)
			for _, id := range nodes {
				if id != interfaces.NodeID(1) {
					surviving = append(surviving, id)
				}
			}
			durCfg := checkers.DurabilityCheckConfig{
				CrashedNode:   interfaces.NodeID(1),
				CrashTime:     crashTime,
				ReadFromNodes: surviving,
				Restarted:     true,
			}
			if err := checkers.CheckDurability(cluster, rec, durCfg); err != nil {
				r.failWith("durability", err.Error())
			} else {
				r.pass("durability")
			}
		}
		for _, l := range r.lines {
			sb.WriteString(l + "\n")
		}
		if r.fail == "" {
			totalScore += 0.25
			sb.WriteString(fmt.Sprintf("  Phase 2 score: +0.25  (running total: %.2f)\n", totalScore))
		} else {
			sb.WriteString(fmt.Sprintf("  Phase 2 score: +0.00  (running total: %.2f)\n", totalScore))
		}
	}
	fmt.Fprintf(os.Stderr, "[timing] Phase 2 total %v\n", time.Since(phase2Start))
	sb.WriteString("\n")

	// ------------------------------------------------------------------
	// Phase 3: NetworkPartition
	// ------------------------------------------------------------------
	phase3Start := time.Now()
	fmt.Fprintf(os.Stderr, "Phase 3: NetworkPartition (%s)...\n", phaseDuration)
	sb.WriteString("--- Phase 3: NetworkPartition (node 0 ↔ node 1 partitioned at 30%, healed at 60%) ---\n")
	{
		r := &result{phase: "NetworkPartition"}
		faults := faultinjector.NetworkPartition(baseConfig.Duration)
		history, _, err := faultinjector.RunWithFaults(cluster, baseConfig, faults)
		if err != nil {
			r.failWith("workload", err.Error())
		} else {
			rec := historyToRecorder(history)
			checkConsistency(r, rec)
		}
		for _, l := range r.lines {
			sb.WriteString(l + "\n")
		}
		if r.fail == "" {
			totalScore += 0.25
			sb.WriteString(fmt.Sprintf("  Phase 3 score: +0.25  (running total: %.2f)\n", totalScore))
		} else {
			sb.WriteString(fmt.Sprintf("  Phase 3 score: +0.00  (running total: %.2f)\n", totalScore))
		}
	}
	fmt.Fprintf(os.Stderr, "[timing] Phase 3 total %v\n", time.Since(phase3Start))
	sb.WriteString("\n")

	// ------------------------------------------------------------------
	// Phase 4: Performance benchmark
	// ------------------------------------------------------------------
	phase4Start := time.Now()
	fmt.Fprintf(os.Stderr, "Phase 4: Performance benchmark (%s)...\n", perfDuration)
	sb.WriteString("--- Phase 4: Performance Benchmark (16 clients, 10k keys, 90/10 R/W) ---\n")
	perfCfg := benchmark.BenchmarkConfig{
		NumClients: 16,
		NumKeys:    10000,
		ReadRatio:  0.9,
		ValueSize:  1024,
		ValueSizeMax: interfaces.MaxValueSize,
		Duration:   perfDuration,
		RampUp:     perfRampUp,
	}
	perf, perfReport, err := benchmark.Run(cluster, perfCfg)
	if err != nil {
		r4 := &result{phase: "Performance"}
		r4.failWith("performance", err.Error())
		for _, l := range r4.lines {
			sb.WriteString(l + "\n")
		}
		sb.WriteString(fmt.Sprintf("  Phase 4 score: +0.00  (running total: %.4f)\n", totalScore))
	} else {
		sb.WriteString(perfReport)
		// Phase 4 contributes up to 0.25; full credit at throughput >= 1000 ops/s.
		phase4 := perf.Throughput / 1000.0 * 0.25
		if phase4 > 0.25 {
			phase4 = 0.25
		}
		totalScore += phase4
		sb.WriteString(fmt.Sprintf("  Phase 4 score: +%.4f  throughput=%.1f ops/s  (running total: %.4f)\n",
			phase4, perf.Throughput, totalScore))
	}

	fmt.Fprintf(os.Stderr, "[timing] Phase 4 total %v\n", time.Since(phase4Start))
	fmt.Fprintf(os.Stderr, "[timing] total run %v\n", time.Since(runStart))
	sb.WriteString(fmt.Sprintf("\n=== Score: %.4f ===\n", totalScore))
	sb.WriteString(scoreBreakdown(totalScore))

	return totalScore, sb.String()
}

// scoreBreakdown returns a plain-text interpretation of the score that
// directly tells an LLM evolver where to focus next.
func scoreBreakdown(score float64) string {
	switch {
	case score >= 1.0:
		return "All invariants pass and throughput >=1000 ops/s. Full marks.\n"
	case score >= 0.75:
		return "All correctness phases pass; throughput below 1000 ops/s. Optimise for performance.\n"
	case score >= 0.50:
		return "Two correctness phases pass; at least one is failing. Check the [FAIL] lines above.\n"
	case score >= 0.25:
		return "Only Phase 1 (HappyPath) passes. Crash recovery or partition handling is broken.\n"
	default:
		return "No phases pass. Fix the [FAIL] lines in Phase 1 first — basic correctness is not met.\n"
	}
}

// historyToRecorder re-injects a flat history slice into a fresh Recorder so
// checkers can query it.
func historyToRecorder(history []recorder.HistoryEvent) *recorder.Recorder {
	r := recorder.NewRecorder()
	for _, e := range history {
		r.InjectEvent(e)
	}
	return r
}

// checkAll runs all invariant checkers and records results.
func checkAll(
	r *result,
	rec *recorder.Recorder,
	_ []recorder.HistoryEvent, // reserved for future callers
	_ interface{},
	_ time.Time,
	_ interfaces.Cluster,
) {
	checkConsistency(r, rec)
}

// checkConsistency runs phantom, monotonic, and causal checkers.
func checkConsistency(r *result, rec *recorder.Recorder) {
	if err := checkers.CheckNoPhantomReads(rec); err != nil {
		r.failWith("phantom-reads", err.Error())
	} else {
		r.pass("phantom-reads")
	}

	if err := checkers.CheckMonotonicReads(rec); err != nil {
		r.failWith("monotonic-reads", err.Error())
	} else {
		r.pass("monotonic-reads")
	}

	if err := checkers.CheckCausalConsistencySession(rec); err != nil {
		r.failWith("causal-consistency", err.Error())
	} else {
		r.pass("causal-consistency")
	}
}
