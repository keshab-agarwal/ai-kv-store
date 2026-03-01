// Command harness runs the KV store correctness test harness.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"ai-kv-store/task3_harness/checker"
	"ai-kv-store/task3_harness/history"
	"ai-kv-store/task3_harness/orchestrator"
	"ai-kv-store/task3_harness/selftest"
	"ai-kv-store/task3_harness/workload"
)

func main() {
	// Parse flags.
	nodes := flag.Int("nodes", 3, "number of nodes in the cluster")
	clients := flag.Int("clients", 10, "number of concurrent clients")
	ops := flag.Int("ops", 1000, "number of operations to run")
	seed := flag.Int64("seed", time.Now().UnixNano(), "random seed for reproducibility")
	crash := flag.Bool("crash", false, "enable fault injection (crash a node)")
	crashNode := flag.Int("crash-node", -1, "which node to crash (-1 = random based on seed)")
	crashAfterOps := flag.Int("crash-after-ops", -1, "crash after N ops (-1 = ops/3)")
	recoverAfterMs := flag.Int("recover-after-ms", 5000, "recover crashed node after N milliseconds")
	outputDir := flag.String("output-dir", "", "output directory (default: ./test-run-TIMESTAMP)")
	binaryPath := flag.String("binary", "../bin/kvnode", "path to kvnode binary")
	maxValueSize := flag.Int("max-value-size", 1024, "maximum value size in bytes")
	keyPoolSize := flag.Int("key-pool-size", 100, "number of distinct keys in the pool")
	selfTest := flag.Bool("self-test", false, "run self-test only (no cluster needed)")
	basePort := flag.Int("base-port", 9100, "starting port for nodes")
	flag.Parse()

	// Self-test mode.
	if *selfTest {
		ok, err := selftest.RunSelfTest()
		if err != nil {
			log.Fatalf("Self-test error: %v", err)
		}
		if !ok {
			os.Exit(1)
		}
		os.Exit(0)
	}

	// Set defaults for crash parameters.
	if *crashAfterOps < 0 {
		*crashAfterOps = *ops / 3
	}
	if *crashNode < 0 {
		rng := rand.New(rand.NewSource(*seed))
		*crashNode = rng.Intn(*nodes)
	}

	// Set output directory.
	if *outputDir == "" {
		*outputDir = fmt.Sprintf("./test-run-%d", time.Now().Unix())
	}
	if err := os.MkdirAll(*outputDir, 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	// Resolve binary path to absolute.
	absBinary, err := filepath.Abs(*binaryPath)
	if err != nil {
		log.Fatalf("Failed to resolve binary path: %v", err)
	}

	// Check that the binary exists.
	if _, err := os.Stat(absBinary); os.IsNotExist(err) {
		log.Fatalf("kvnode binary not found at %s", absBinary)
	}

	log.Printf("=== KV Store Test Harness ===")
	log.Printf("Nodes: %d, Clients: %d, Ops: %d, Seed: %d", *nodes, *clients, *ops, *seed)
	log.Printf("Crash: %v, Output: %s", *crash, *outputDir)

	startTime := time.Now()

	// Create data and log directories.
	dataDir := filepath.Join(*outputDir, "data")
	logDir := filepath.Join(*outputDir, "logs")

	// Start the cluster.
	orch := orchestrator.NewOrchestrator(orchestrator.OrchestratorConfig{
		BasePort:   *basePort,
		NodeCount:  *nodes,
		BinaryPath: absBinary,
		DataDir:    dataDir,
		LogDir:     logDir,
	})

	log.Printf("Starting %d-node cluster...", *nodes)
	if err := orch.StartAll(); err != nil {
		log.Fatalf("Failed to start cluster: %v", err)
	}
	defer orch.StopAll()

	// Wait for the cluster to stabilize (coordinator elected).
	log.Printf("Waiting for cluster to stabilize...")
	coordID, err := orch.WaitForCluster(10 * time.Second)
	if err != nil {
		log.Fatalf("Cluster failed to stabilize: %v", err)
	}
	log.Printf("Coordinator elected: node %d", coordID)

	// Create the recorder.
	rec := history.NewRecorder()

	// Set up workload context.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Configure the workload generator.
	genConfig := workload.GeneratorConfig{
		NumClients:   *clients,
		NumOps:       *ops,
		ReadPct:      0.95,
		WritePct:     0.04,
		DeletePct:    0.01,
		MaxValueSize: *maxValueSize,
		KeyPoolSize:  *keyPoolSize,
		Seed:         *seed,
		Timeout:      10 * time.Second,
	}
	gen := workload.NewGenerator(genConfig)

	// If crash is enabled, schedule fault injection.
	if *crash {
		go func() {
			// Wait until the specified number of ops have been recorded.
			log.Printf("Fault injection: will crash node %d after ~%d ops", *crashNode, *crashAfterOps)
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					entries := rec.Entries()
					if len(entries) >= *crashAfterOps {
						log.Printf("Fault injection: crashing node %d (after %d ops recorded)", *crashNode, len(entries))
						if err := orch.CrashNode(*crashNode); err != nil {
							log.Printf("Fault injection: crash failed: %v", err)
							return
						}
						// Schedule recovery.
						time.Sleep(time.Duration(*recoverAfterMs) * time.Millisecond)
						log.Printf("Fault injection: recovering node %d", *crashNode)
						if err := orch.RecoverNode(*crashNode); err != nil {
							log.Printf("Fault injection: recovery failed: %v", err)
						} else {
							log.Printf("Fault injection: node %d recovered", *crashNode)
						}
						return
					}
				}
			}
		}()
	}

	// Run the workload.
	log.Printf("Running workload: %d ops across %d clients...", *ops, *clients)
	workloadStart := time.Now()
	gen.Run(ctx, orch, rec)
	workloadDuration := time.Since(workloadStart)
	log.Printf("Workload completed in %s", workloadDuration)

	// Stop the cluster.
	log.Printf("Stopping cluster...")
	orch.StopAll()

	// Copy node logs to output directory.
	if err := orch.CopyLogs(*outputDir); err != nil {
		log.Printf("Warning: failed to copy node logs: %v", err)
	}

	// Save history.
	historyPath := filepath.Join(*outputDir, "history.jsonl")
	if err := rec.Save(historyPath); err != nil {
		log.Fatalf("Failed to save history: %v", err)
	}
	log.Printf("History saved to %s", historyPath)

	// Load and check history.
	entries, err := history.Load(historyPath)
	if err != nil {
		log.Fatalf("Failed to load history: %v", err)
	}

	log.Printf("Running linearizability check on %d operations...", len(entries))
	result := checker.CheckHistory(entries)

	// Write visualization.
	vizPath := filepath.Join(*outputDir, "visualization.html")
	if err := checker.WriteVisualization(result, vizPath); err != nil {
		log.Printf("Warning: failed to write visualization: %v", err)
	} else {
		log.Printf("Visualization written to %s", vizPath)
	}

	// Compute summary statistics.
	totalDuration := time.Since(startTime)
	summary := buildSummary(entries, result, *seed, workloadDuration, totalDuration, *crash, *crashNode)

	// Write summary.
	summaryPath := filepath.Join(*outputDir, "summary.json")
	summaryJSON, _ := json.MarshalIndent(summary, "", "  ")
	if err := os.WriteFile(summaryPath, summaryJSON, 0644); err != nil {
		log.Printf("Warning: failed to write summary: %v", err)
	}
	log.Printf("Summary written to %s", summaryPath)

	// Print results.
	fmt.Println()
	fmt.Println("========================================")
	if result.Ok {
		fmt.Println("  RESULT: PASS (linearizable)")
	} else {
		fmt.Printf("  RESULT: FAIL (non-linearizable, porcupine: %s)\n", result.PorcupineResult)
	}
	fmt.Println("========================================")
	fmt.Printf("  Total ops:     %d\n", summary.TotalOps)
	fmt.Printf("  Skipped:       %d (errors)\n", result.SkippedOps)
	fmt.Printf("  By type:       get=%d put=%d delete=%d\n",
		summary.ByType["get"], summary.ByType["put"], summary.ByType["delete"])
	fmt.Printf("  By status:     ok=%d found=%d not_found=%d error=%d timeout=%d\n",
		summary.ByStatus["ok"], summary.ByStatus["found"], summary.ByStatus["not_found"],
		summary.ByStatus["error"], summary.ByStatus["timeout"])
	fmt.Printf("  Duration:      %s\n", workloadDuration.Round(time.Millisecond))
	fmt.Printf("  Throughput:    %.1f ops/sec\n", summary.Throughput)
	fmt.Printf("  Check time:    %s\n", result.Duration.Round(time.Millisecond))
	fmt.Printf("  Seed:          %d\n", *seed)
	fmt.Printf("  Output:        %s\n", *outputDir)
	fmt.Println("========================================")

	if !result.Ok {
		os.Exit(1)
	}
}

// Summary holds the test run summary report.
type Summary struct {
	TotalOps      int               `json:"total_ops"`
	ByType        map[string]int    `json:"by_type"`
	ByStatus      map[string]int    `json:"by_status"`
	DurationMs    int64             `json:"duration_ms"`
	Throughput    float64           `json:"throughput"`
	Linearizable  bool              `json:"linearizable"`
	CheckResult   string            `json:"check_result"`
	CheckDurationMs int64           `json:"check_duration_ms"`
	Seed          int64             `json:"seed"`
	CrashEnabled  bool              `json:"crash_enabled"`
	CrashedNode   int               `json:"crashed_node"`
	TotalDurationMs int64           `json:"total_duration_ms"`
}

func buildSummary(entries []history.HistoryEntry, result checker.CheckResult, seed int64,
	workloadDuration, totalDuration time.Duration, crashEnabled bool, crashedNode int) Summary {

	byType := map[string]int{"get": 0, "put": 0, "delete": 0}
	byStatus := map[string]int{"ok": 0, "found": 0, "not_found": 0, "error": 0, "timeout": 0}

	for _, e := range entries {
		byType[e.OpType]++
		byStatus[e.Status]++
	}

	throughput := 0.0
	if workloadDuration.Seconds() > 0 {
		throughput = float64(len(entries)) / workloadDuration.Seconds()
	}

	return Summary{
		TotalOps:        len(entries),
		ByType:          byType,
		ByStatus:        byStatus,
		DurationMs:      workloadDuration.Milliseconds(),
		Throughput:      throughput,
		Linearizable:    result.Ok,
		CheckResult:     string(result.PorcupineResult),
		CheckDurationMs: result.Duration.Milliseconds(),
		Seed:            seed,
		CrashEnabled:    crashEnabled,
		CrashedNode:     crashedNode,
		TotalDurationMs: totalDuration.Milliseconds(),
	}
}
