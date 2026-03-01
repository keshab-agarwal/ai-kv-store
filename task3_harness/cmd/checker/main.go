// Command checker runs the Porcupine linearizability checker on a saved history file.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"ai-kv-store/task3_harness/checker"
	"ai-kv-store/task3_harness/history"
)

func main() {
	historyPath := flag.String("history", "", "path to JSONL history file (required)")
	outputDir := flag.String("output-dir", "", "output directory for visualization (default: same directory as history file)")
	flag.Parse()

	if *historyPath == "" {
		fmt.Fprintln(os.Stderr, "Error: --history flag is required")
		flag.Usage()
		os.Exit(2)
	}

	// Resolve output directory.
	if *outputDir == "" {
		*outputDir = filepath.Dir(*historyPath)
	}
	if err := os.MkdirAll(*outputDir, 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	// Load history.
	entries, err := history.Load(*historyPath)
	if err != nil {
		log.Fatalf("Failed to load history: %v", err)
	}
	log.Printf("Loaded %d history entries from %s", len(entries), *historyPath)

	// Run linearizability check.
	result := checker.CheckHistory(entries)

	// Write visualization.
	vizPath := filepath.Join(*outputDir, "visualization.html")
	if err := checker.WriteVisualization(result, vizPath); err != nil {
		log.Printf("Warning: failed to write visualization: %v", err)
	} else {
		log.Printf("Visualization written to %s", vizPath)
	}

	// Print results.
	fmt.Println()
	fmt.Println("========================================")
	if result.Ok {
		fmt.Println("  RESULT: PASS (linearizable)")
	} else {
		fmt.Printf("  RESULT: FAIL (non-linearizable, porcupine: %s)\n", result.PorcupineResult)
	}
	fmt.Println("========================================")
	fmt.Printf("  Total ops:     %d\n", result.TotalOps)
	fmt.Printf("  Skipped:       %d (errors)\n", result.SkippedOps)
	fmt.Printf("  Checked:       %d\n", result.TotalOps-result.SkippedOps)
	fmt.Printf("  Check time:    %s\n", result.Duration)
	fmt.Println("========================================")

	if !result.Ok {
		os.Exit(1)
	}
}
