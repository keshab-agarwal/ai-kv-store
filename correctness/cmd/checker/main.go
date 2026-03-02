// Command checker is a standalone Porcupine linearizability checker.
// It can load a JSONL history file produced by the harness and check it,
// or run the built-in self-test suite without a running cluster.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"ai-kv-store/correctness/checker"
)

func main() {
	historyPath := flag.String("history", "", "path to JSONL history file")
	timeout     := flag.Duration("timeout", 120*time.Second, "Porcupine check timeout")
	selfTest    := flag.Bool("selftest", false, "run built-in self-test (no cluster needed)")
	flag.Parse()

	// --- Self-test mode ---
	if *selfTest {
		if checker.SelfTest() {
			fmt.Println("SELF-TEST PASS")
			os.Exit(0)
		}
		fmt.Println("SELF-TEST FAIL")
		os.Exit(1)
	}

	// --- File check mode ---
	if *historyPath == "" {
		fmt.Fprintln(os.Stderr, "usage: checker -history <path> [-timeout <duration>]")
		fmt.Fprintln(os.Stderr, "       checker -selftest")
		os.Exit(2)
	}

	result, err := checker.CheckFile(*historyPath, *timeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(2)
	}

	fmt.Printf("ops_checked:   %d\n", result.OpCount)
	fmt.Printf("check_time:    %s\n", result.Duration)
	fmt.Printf("linearizable:  %v\n", result.Linearizable)
	fmt.Printf("raw_result:    %v\n", result.RawResult)

	// Pass if linearizable, or if checker timed out without finding a violation.
	if result.Linearizable || result.IsUnknown() {
		fmt.Println("PASS")
		os.Exit(0)
	}
	fmt.Println("FAIL")
	os.Exit(1)
}
