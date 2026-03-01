// checker verifies linearizability of a KV store operation history using
// the Porcupine checker.
//
// Usage:
//
//	checker -selftest
//	checker -history=path/to/history.jsonl [-timeout=120s]
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"ai-kv-store/correctness"

	"github.com/anishathalye/porcupine"
)

func main() {
	selftest := flag.Bool("selftest", false, "Run built-in self-tests (no cluster needed)")
	historyPath := flag.String("history", "", "Path to history.jsonl file to check")
	timeout := flag.Duration("timeout", 120*time.Second, "Timeout for linearizability check")
	flag.Parse()

	if *selftest {
		runSelfTest()
		return
	}

	if *historyPath == "" {
		fmt.Fprintf(os.Stderr, "Usage: checker -selftest | checker -history=<path>\n")
		os.Exit(1)
	}

	events, err := correctness.ReadHistoryJSONL(*historyPath)
	if err != nil {
		log.Fatalf("Failed to read history: %v", err)
	}

	ops := correctness.HistoryToOperations(events)
	fmt.Printf("Loaded %d events, %d deterministic operations (skipped %d timeout/error)\n",
		len(events), len(ops), len(events)-len(ops))

	if len(ops) == 0 {
		fmt.Println("PASS (no deterministic operations to check)")
		os.Exit(0)
	}

	model := correctness.KVModel()
	result := porcupine.CheckOperationsTimeout(model, ops, *timeout)

	switch result {
	case porcupine.Ok:
		fmt.Println("PASS — history is linearizable")
		os.Exit(0)
	case porcupine.Illegal:
		fmt.Println("FAIL — history is NOT linearizable")
		os.Exit(1)
	case porcupine.Unknown:
		fmt.Println("UNKNOWN — checker timed out, could not determine linearizability")
		os.Exit(1)
	default:
		fmt.Printf("UNEXPECTED result: %v\n", result)
		os.Exit(1)
	}
}

// runSelfTest generates known-good and known-bad histories and verifies the
// Porcupine checker produces the expected results.
func runSelfTest() {
	fmt.Println("=== Self-Test: Known-Good History ===")
	passOk := testKnownGood()

	fmt.Println()
	fmt.Println("=== Self-Test: Known-Bad History ===")
	failOk := testKnownBad()

	fmt.Println()
	if passOk && failOk {
		fmt.Println("SELFTEST PASS")
		os.Exit(0)
	}
	fmt.Println("SELFTEST FAIL")
	os.Exit(1)
}

// testKnownGood generates a sequential history that is trivially linearizable:
//
//	put(k, v1) -> ok
//	get(k) -> found(v1)
//	delete(k) -> ok
//	get(k) -> not_found
//
// All operations are sequential (non-overlapping timestamps).
func testKnownGood() bool {
	key := "00112233445566778899aabbccddeeff"
	val := "deadbeef"

	ops := []porcupine.Operation{
		{
			ClientId: 0,
			Input:    correctness.KVInput{OpType: "put", Key: key, Value: val},
			Output:   correctness.KVOutput{Status: "ok"},
			Call:     1,
			Return:   2,
		},
		{
			ClientId: 0,
			Input:    correctness.KVInput{OpType: "get", Key: key},
			Output:   correctness.KVOutput{Status: "found", Value: val},
			Call:     3,
			Return:   4,
		},
		{
			ClientId: 0,
			Input:    correctness.KVInput{OpType: "delete", Key: key},
			Output:   correctness.KVOutput{Status: "ok"},
			Call:     5,
			Return:   6,
		},
		{
			ClientId: 0,
			Input:    correctness.KVInput{OpType: "get", Key: key},
			Output:   correctness.KVOutput{Status: "not_found"},
			Call:     7,
			Return:   8,
		},
	}

	model := correctness.KVModel()
	result := porcupine.CheckOperationsTimeout(model, ops, 5*time.Second)

	if result == porcupine.Ok {
		fmt.Println("  PASS: known-good history accepted (Ok=true)")
		return true
	}
	fmt.Printf("  FAIL: known-good history rejected (result=%v, expected Ok)\n", result)
	return false
}

// testKnownBad generates a history that is NOT linearizable:
// Two concurrent gets of the same key return different values with no
// intervening write. Specifically:
//
//	put(k, v1) -> ok           [time 1..2]
//	get(k) -> found(v1)        [time 3..5]  (concurrent with next)
//	get(k) -> found(v2)        [time 4..6]  (concurrent, but v2 was never written)
//
// There is no linearization that explains get returning v2.
func testKnownBad() bool {
	key := "00112233445566778899aabbccddeeff"
	val1 := "deadbeef"
	val2 := "cafebabe" // never written

	ops := []porcupine.Operation{
		{
			ClientId: 0,
			Input:    correctness.KVInput{OpType: "put", Key: key, Value: val1},
			Output:   correctness.KVOutput{Status: "ok"},
			Call:     1,
			Return:   2,
		},
		{
			ClientId: 1,
			Input:    correctness.KVInput{OpType: "get", Key: key},
			Output:   correctness.KVOutput{Status: "found", Value: val1},
			Call:     3,
			Return:   5,
		},
		{
			ClientId: 2,
			Input:    correctness.KVInput{OpType: "get", Key: key},
			Output:   correctness.KVOutput{Status: "found", Value: val2},
			Call:     4,
			Return:   6,
		},
	}

	model := correctness.KVModel()
	result := porcupine.CheckOperationsTimeout(model, ops, 5*time.Second)

	if result == porcupine.Illegal {
		fmt.Println("  PASS: known-bad history rejected (Ok=false)")
		return true
	}
	fmt.Printf("  FAIL: known-bad history accepted (result=%v, expected Illegal)\n", result)
	return false
}
