package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"ai-kv-store/correctness"
	"ai-kv-store/correctness/checker"
)

func main() {
	nodeCount := flag.Int("nodes", 3, "Number of nodes (3-5)")
	duration := flag.Duration("duration", 30*time.Second, "Test duration")
	workers := flag.Int("workers", 10, "Number of concurrent client workers")
	maxValueSize := flag.Int("maxvalue", 256, "Maximum value size in bytes")
	faults := flag.Bool("faults", false, "Enable fault injection (crash/recover 1 node)")
	faultSeed := flag.Int64("seed", 0, "Fault injection seed (0 = time-based)")
	binPath := flag.String("bin", "./bin/kvnode", "Path to kvnode binary")
	outDir := flag.String("outdir", "", "Output directory (default: auto-generated)")
	skipCheck := flag.Bool("skipcheck", false, "Skip Porcupine linearizability check")
	selftest := flag.Bool("selftest", false, "Run self-test only (no cluster needed)")
	checkTimeout := flag.Duration("checktimeout", 60*time.Second, "Porcupine checker timeout")
	flag.Parse()

	if *selftest {
		if err := checker.RunSelfTest(); err != nil {
			fmt.Fprintf(os.Stderr, "SELF-TEST FAILED: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	if *outDir == "" {
		*outDir = fmt.Sprintf("test-run-%s", time.Now().Format("20060102-150405"))
	}
	os.MkdirAll(*outDir, 0755)
	logDir := filepath.Join(*outDir, "logs")
	os.MkdirAll(logDir, 0755)

	logger := log.New(os.Stderr, "[harness] ", log.LstdFlags|log.Lmicroseconds)
	logger.Printf("output dir: %s", *outDir)

	seed := *faultSeed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	logger.Printf("seed: %d", seed)

	orch := correctness.NewOrchestrator(*nodeCount, *binPath, logDir, logger)

	logger.Println("starting cluster...")
	if err := orch.StartAll(); err != nil {
		logger.Fatalf("failed to start cluster: %v", err)
	}
	defer orch.StopAll()

	historyPath := filepath.Join(*outDir, "history.jsonl")
	recorder, err := correctness.NewHistoryRecorder(historyPath)
	if err != nil {
		logger.Fatalf("create history recorder: %v", err)
	}

	cfg := correctness.WorkloadConfig{
		Duration:     *duration,
		Workers:      *workers,
		MaxValueSize: *maxValueSize,
		ReadRatio:    0.95,
		PutRatio:     0.04,
		DeleteRatio:  0.01,
	}
	runner := correctness.NewWorkloadRunner(cfg, orch.ClientAddrs(), recorder, logger)

	var injector *correctness.FaultInjector
	if *faults {
		schedule := correctness.GenerateFaultSchedule(seed, *duration, *nodeCount)
		injector = correctness.NewFaultInjector(orch, schedule, logger)
	}

	logger.Println("starting workload...")
	startTime := time.Now()

	if injector != nil {
		go injector.Run(startTime)
	}

	gets, puts, deletes, errors, timeouts := runner.Run()
	elapsed := time.Since(startTime)
	recorder.Close()

	logger.Printf("workload done: gets=%d puts=%d deletes=%d errors=%d timeouts=%d elapsed=%.1fs",
		gets, puts, deletes, errors, timeouts, elapsed.Seconds())

	faultEvents := 0
	if injector != nil {
		faultEvents = injector.FaultCount()
	}

	summary := correctness.Summary{
		Pass:        true,
		TotalOps:    gets + puts + deletes + errors + timeouts,
		Gets:        gets,
		Puts:        puts,
		Deletes:     deletes,
		Errors:      errors,
		Timeouts:    timeouts,
		ElapsedSec:  elapsed.Seconds(),
		FaultEvents: faultEvents,
	}

	if !*skipCheck {
		logger.Println("running Porcupine linearizability check...")
		result, err := checker.RunCheckerOnFile(historyPath, *checkTimeout)
		if err != nil {
			logger.Fatalf("checker error: %v", err)
		}

		logger.Printf("checker: %s (checked=%d skipped=%d timeouts=%d elapsed=%dms)",
			result.Info, result.CheckedOps, result.SkippedOps, result.TimeoutOps, result.ElapsedMs)

		summary.Pass = result.OK
		summary.CheckerResult = result.Info

		resultPath := filepath.Join(*outDir, "checker_result.txt")
		checker.WriteCheckResult(resultPath, result)
	}

	summaryPath := filepath.Join(*outDir, "summary.json")
	correctness.WriteSummary(summaryPath, summary)
	logger.Printf("summary written to %s", summaryPath)

	if !summary.Pass {
		fmt.Fprintf(os.Stderr, "\n*** LINEARIZABILITY CHECK FAILED ***\n")
		fmt.Fprintf(os.Stderr, "Details: %s\n", summary.CheckerResult)
		fmt.Fprintf(os.Stderr, "History: %s\n", historyPath)
		fmt.Fprintf(os.Stderr, "Seed: %d (use -seed=%d to reproduce)\n", seed, seed)
		os.Exit(1)
	}

	fmt.Println("\n*** ALL CHECKS PASSED ***")
}
