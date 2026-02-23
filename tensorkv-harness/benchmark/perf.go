// Package benchmark provides standalone performance benchmarking utilities
// for a distributed KV store cluster, built on top of the workload generator.
package benchmark

import (
	"fmt"
	"strings"
	"time"

	"github.com/tensorkv/harness/interfaces"
	"github.com/tensorkv/harness/workload"
)

// BenchmarkConfig parameterises a standalone performance run.
type BenchmarkConfig struct {
	// NumClients is the number of concurrent client goroutines.
	NumClients int
	// NumKeys is the size of the key space.
	NumKeys int
	// ReadRatio is the fraction of operations that are Gets (0.0–1.0).
	ReadRatio float64
	// ValueSize is the fixed value size in bytes.
	ValueSize int
	// Duration is how long to run.
	Duration time.Duration
	// RampUp is the warm-up period (not measured).
	RampUp time.Duration
}

// DefaultBenchmarkConfig returns a sensible default for a 60-second benchmark.
func DefaultBenchmarkConfig() BenchmarkConfig {
	return BenchmarkConfig{
		NumClients: 16,
		NumKeys:    10000,
		ReadRatio:  0.9,
		ValueSize:  interfaces.MinValueSize, // 1 MB
		Duration:   60 * time.Second,
		RampUp:     5 * time.Second,
	}
}

// Run executes a performance benchmark against cluster using cfg and returns
// the result and a human-readable report string.
func Run(cluster interfaces.Cluster, cfg BenchmarkConfig) (workload.PerfResult, string, error) {
	wCfg := workload.WorkloadConfig{
		NumClients:      cfg.NumClients,
		NumKeys:         cfg.NumKeys,
		ReadRatio:       cfg.ReadRatio,
		KeyDistribution: "zipfian",
		ZipfianConstant: 0.99,
		ValueSize:       cfg.ValueSize,
		Duration:        cfg.Duration,
		RampUp:          cfg.RampUp,
	}

	_, perf, err := workload.RunWorkload(cluster, wCfg)
	if err != nil {
		return workload.PerfResult{}, "", err
	}

	report := FormatPerfResult(perf)
	return perf, report, nil
}

// FormatPerfResult returns a multi-line human-readable summary of a PerfResult.
func FormatPerfResult(p workload.PerfResult) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "  Total ops    : %d\n", p.TotalOps)
	fmt.Fprintf(&sb, "  Duration     : %v\n", p.Duration.Round(time.Millisecond))
	fmt.Fprintf(&sb, "  Throughput   : %.1f ops/s\n", p.Throughput)
	fmt.Fprintf(&sb, "  Read  p50    : %v\n", p.ReadLatencyP50.Round(time.Microsecond))
	fmt.Fprintf(&sb, "  Read  p99    : %v\n", p.ReadLatencyP99.Round(time.Microsecond))
	fmt.Fprintf(&sb, "  Read  p99.9  : %v\n", p.ReadLatencyP999.Round(time.Microsecond))
	fmt.Fprintf(&sb, "  Write p50    : %v\n", p.WriteLatencyP50.Round(time.Microsecond))
	fmt.Fprintf(&sb, "  Write p99    : %v\n", p.WriteLatencyP99.Round(time.Microsecond))
	fmt.Fprintf(&sb, "  Write p99.9  : %v\n", p.WriteLatencyP999.Round(time.Microsecond))
	return sb.String()
}
