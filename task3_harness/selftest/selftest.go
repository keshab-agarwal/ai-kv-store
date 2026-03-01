// Package selftest provides self-test / negative control for the Porcupine checker integration.
package selftest

import (
	"fmt"

	"ai-kv-store/task3_harness/checker"
	"ai-kv-store/task3_harness/history"
)

// RunSelfTest runs both a positive and negative self-test of the linearizability checker.
// Returns true if both tests produce the expected result (good history passes, bad history fails).
func RunSelfTest() (bool, error) {
	passGood, err := testGoodHistory()
	if err != nil {
		return false, fmt.Errorf("good history test error: %w", err)
	}

	passBad, err := testBadHistory()
	if err != nil {
		return false, fmt.Errorf("bad history test error: %w", err)
	}

	allPassed := passGood && passBad

	if allPassed {
		fmt.Println("SELF-TEST: ALL PASSED")
	} else {
		fmt.Println("SELF-TEST: FAILED")
	}

	return allPassed, nil
}

// testGoodHistory generates a known-good (linearizable) history and verifies the checker accepts it.
func testGoodHistory() (bool, error) {
	fmt.Println("--- Self-test: Good history (should PASS) ---")

	k1 := "00000000000000000000000000000001"
	k2 := "00000000000000000000000000000002"

	// All operations are sequential (non-overlapping times).
	// Client 1: put(k1, "a") -> ok, get(k1) -> found("a"), delete(k1) -> ok, get(k1) -> not_found
	// Client 2: put(k2, "b") -> ok, get(k2) -> found("b")
	entries := []history.HistoryEntry{
		{
			OpID: 1, ClientID: "client-1", CallTime: 1000, ReturnTime: 2000,
			OpType: "put", Key: k1, InputValue: "YQ==", Status: "ok", // "a" in base64
		},
		{
			OpID: 2, ClientID: "client-1", CallTime: 3000, ReturnTime: 4000,
			OpType: "get", Key: k1, OutputValue: "YQ==", Status: "found",
		},
		{
			OpID: 3, ClientID: "client-2", CallTime: 5000, ReturnTime: 6000,
			OpType: "put", Key: k2, InputValue: "Yg==", Status: "ok", // "b" in base64
		},
		{
			OpID: 4, ClientID: "client-1", CallTime: 7000, ReturnTime: 8000,
			OpType: "delete", Key: k1, Status: "ok",
		},
		{
			OpID: 5, ClientID: "client-1", CallTime: 9000, ReturnTime: 10000,
			OpType: "get", Key: k1, Status: "not_found",
		},
		{
			OpID: 6, ClientID: "client-2", CallTime: 11000, ReturnTime: 12000,
			OpType: "get", Key: k2, OutputValue: "Yg==", Status: "found",
		},
	}

	result := checker.CheckHistory(entries)

	if result.Ok {
		fmt.Println("  PASS: Good history correctly identified as linearizable")
		return true, nil
	}

	fmt.Printf("  FAIL: Good history incorrectly identified as non-linearizable (result: %s)\n", result.PorcupineResult)
	return false, nil
}

// testBadHistory generates a known-bad (non-linearizable) history and verifies the checker rejects it.
func testBadHistory() (bool, error) {
	fmt.Println("--- Self-test: Bad history (should FAIL) ---")

	k1 := "00000000000000000000000000000001"

	// Client 1: put(k1, "a") at t=0..1 -> ok
	// Client 2: put(k1, "b") at t=2..3 -> ok
	// Client 1: get(k1) at t=4..5 -> found("a")  <- STALE READ: should see "b"
	//
	// This is not linearizable because:
	// - put(k1,"a") must linearize in [0,1]
	// - put(k1,"b") must linearize in [2,3] (after put "a")
	// - get(k1) must linearize in [4,5] (after put "b")
	// - So get must return "b", but it returns "a"
	entries := []history.HistoryEntry{
		{
			OpID: 1, ClientID: "client-1", CallTime: 0, ReturnTime: 1,
			OpType: "put", Key: k1, InputValue: "YQ==", Status: "ok", // "a"
		},
		{
			OpID: 2, ClientID: "client-2", CallTime: 2, ReturnTime: 3,
			OpType: "put", Key: k1, InputValue: "Yg==", Status: "ok", // "b"
		},
		{
			OpID: 3, ClientID: "client-1", CallTime: 4, ReturnTime: 5,
			OpType: "get", Key: k1, OutputValue: "YQ==", Status: "found", // returns "a" - stale!
		},
	}

	result := checker.CheckHistory(entries)

	if !result.Ok {
		fmt.Println("  PASS: Bad history correctly identified as non-linearizable")
		return true, nil
	}

	fmt.Println("  FAIL: Bad history incorrectly identified as linearizable")
	return false, nil
}
