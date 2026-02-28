package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"ai-kv-store/correctness/checker"
)

func main() {
	historyPath := flag.String("history", "", "Path to history.jsonl file (required)")
	timeout := flag.Duration("timeout", 60*time.Second, "Checker timeout")
	outputPath := flag.String("output", "", "Path to write result file (optional)")
	selftest := flag.Bool("selftest", false, "Run self-test instead of checking a history")
	flag.Parse()

	if *selftest {
		if err := checker.RunSelfTest(); err != nil {
			fmt.Fprintf(os.Stderr, "SELF-TEST FAILED: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	if *historyPath == "" {
		fmt.Fprintf(os.Stderr, "Usage: checker -history=path/to/history.jsonl\n")
		fmt.Fprintf(os.Stderr, "       checker -selftest\n")
		os.Exit(1)
	}

	result, err := checker.RunCheckerOnFile(*historyPath, *timeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Result:      %s\n", boolToPassFail(result.OK))
	fmt.Printf("Info:        %s\n", result.Info)
	fmt.Printf("Total ops:   %d\n", result.TotalOps)
	fmt.Printf("Checked:     %d\n", result.CheckedOps)
	fmt.Printf("Skipped:     %d (errors)\n", result.SkippedOps)
	fmt.Printf("Timeouts:    %d (treated as pending)\n", result.TimeoutOps)
	fmt.Printf("Check time:  %dms\n", result.ElapsedMs)

	if *outputPath != "" {
		checker.WriteCheckResult(*outputPath, result)
		fmt.Printf("Result written to %s\n", *outputPath)
	}

	if !result.OK {
		os.Exit(1)
	}
}

func boolToPassFail(b bool) string {
	if b {
		return "PASS"
	}
	return "FAIL"
}
