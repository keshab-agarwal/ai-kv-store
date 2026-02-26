// Command harness is the CLI entry point for the TensorKV evaluation harness.
//
// Usage:
//
//	harness --mock                          # evaluate against the in-memory mock
//	harness --kv-store                      # evaluate the kv-store implementation (build bin/node first)
//	harness --kv-store --workload-only      # performance benchmark only
//	harness --nodes 3                       # override node count (default 3)
//
// Exit codes:
//
//	0  all invariants pass (or --workload-only was specified)
//	1  one or more invariants failed, or a fatal error occurred
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/tensorkv/harness/benchmark"
	"github.com/tensorkv/harness/evaluator"
	"github.com/tensorkv/harness/interfaces"
	"github.com/tensorkv/harness/internal/mock"
	"github.com/tensorkv/harness/skeleton"
	"github.com/tensorkv/harness/workload"
)

func main() {
	useMock := flag.Bool("mock", false, "Use the in-memory mock cluster for testing the harness itself.")
	useKVStore := flag.Bool("kv-store", false, "Use the kv-store implementation (requires bin/node).")
	short := flag.Bool("short", false, "Short run: 5s per phase, 10s perf (~30–40s total).")
	workloadOnly := flag.Bool("workload-only", false,
		"Skip invariant checking and run only the performance benchmark. Useful for quick iteration.")
	nodes := flag.Int("nodes", 3, "Number of cluster nodes to start.")
	flag.Parse()

	var cluster interfaces.Cluster
	switch {
	case *useKVStore:
		cluster = skeleton.NewCluster()
	case *useMock:
		cluster = mock.NewCluster()
	default:
		fmt.Fprintln(os.Stderr, "error: specify --mock or --kv-store")
		os.Exit(1)
	}

	if *workloadOnly {
		runWorkloadOnly(cluster, *nodes)
		return
	}

	score, report := evaluator.EvaluateWithConfig(cluster, *nodes, evaluator.Config{Short: *short})
	fmt.Print(report)

	if score == 0.0 {
		os.Exit(1)
	}
	os.Exit(0)
}

// runWorkloadOnly runs a standalone benchmark without invariant checking and
// prints the results. Exits with code 0 always (even if the store misbehaves).
func runWorkloadOnly(cluster interfaces.Cluster, nodeCount int) {
	if err := cluster.Start(nodeCount); err != nil {
		fmt.Fprintf(os.Stderr, "failed to start cluster: %v\n", err)
		os.Exit(1)
	}
	defer cluster.Shutdown() //nolint:errcheck

	fmt.Printf("Running workload-only benchmark against %d nodes...\n", nodeCount)

	cfg := workload.WorkloadConfig{
		NumClients:      8,
		NumKeys:         1000,
		ReadRatio:       0.9,
		KeyDistribution: "zipfian",
		ZipfianConstant: 0.99,
		ValueSize:       1024,
		ValueSizeMax:    interfaces.MaxValueSize,
		Duration:        30 * time.Second,
		RampUp:          3 * time.Second,
	}

	_, perf, err := workload.RunWorkload(cluster, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "workload error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(benchmark.FormatPerfResult(perf))
}
