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
	NumClients int
	NumKeys    int
	ReadRatio  float64
	// ValueSize is the average value size in bytes; ValueSizeMax caps variable size.
	ValueSize    int
	ValueSizeMax int
	Duration     time.Duration
	RampUp       time.Duration
}

// DefaultBenchmarkConfig returns a sensible default for a 60-second benchmark.
func DefaultBenchmarkConfig() BenchmarkConfig {
	return BenchmarkConfig{
		NumClients:   16,
		NumKeys:      10000,
		ReadRatio:    0.9,
		ValueSize:    1024,
		ValueSizeMax: interfaces.MaxValueSize,
		Duration:     60 * time.Second,
		RampUp:       5 * time.Second,
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
		ValueSizeMax:    cfg.ValueSizeMax,
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
