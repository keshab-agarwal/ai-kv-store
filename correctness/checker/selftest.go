package checker

import (
	"fmt"
	"time"

	"ai-kv-store/correctness"
)

// RunSelfTest validates the Porcupine checker pipeline without a running cluster.
// It generates a known-good history (should PASS) and a known-bad history (should FAIL).
func RunSelfTest() error {
	fmt.Println("=== Self-Test: Porcupine Checker Validation ===")

	// Test 1: Known-good history
	fmt.Print("Test 1: Known-good (linearizable) history... ")
	goodHistory := generateGoodHistory()
	result := CheckHistory(goodHistory, 10*time.Second)
	if !result.OK {
		return fmt.Errorf("FAILED: expected PASS but got %s", result.Info)
	}
	fmt.Printf("PASS (checked %d ops in %dms)\n", result.CheckedOps, result.ElapsedMs)

	// Test 2: Known-bad history (non-linearizable)
	fmt.Print("Test 2: Known-bad (non-linearizable) history... ")
	badHistory := generateBadHistory()
	result = CheckHistory(badHistory, 10*time.Second)
	if result.OK {
		return fmt.Errorf("FAILED: expected FAIL but got PASS")
	}
	fmt.Printf("PASS (correctly detected violation, %d ops in %dms)\n", result.CheckedOps, result.ElapsedMs)

	// Test 3: History with TIMEOUT operations
	fmt.Print("Test 3: History with TIMEOUT ops (should PASS)... ")
	timeoutHistory := generateTimeoutHistory()
	result = CheckHistory(timeoutHistory, 10*time.Second)
	if !result.OK {
		return fmt.Errorf("FAILED: expected PASS but got %s", result.Info)
	}
	fmt.Printf("PASS (checked %d ops, %d timeouts, in %dms)\n", result.CheckedOps, result.TimeoutOps, result.ElapsedMs)

	fmt.Println("\n=== All self-tests passed ===")
	return nil
}

// generateGoodHistory creates a simple linearizable history:
// Client 0: Put(k1, "a") -> OK    at t=[100, 200]
// Client 0: Get(k1) -> FOUND("a") at t=[300, 400]
// Client 1: Put(k1, "b") -> OK    at t=[500, 600]
// Client 0: Get(k1) -> FOUND("b") at t=[700, 800]
// Client 1: Delete(k1) -> OK      at t=[900, 1000]
// Client 0: Get(k1) -> NOT_FOUND  at t=[1100, 1200]
func generateGoodHistory() []correctness.HistoryEvent {
	k := "00112233445566778899aabbccddeeff"
	return []correctness.HistoryEvent{
		{OpID: 1, ClientID: "c0", CallTime: 100, ReturnTime: 200, OpType: "Put", Key: k, InputValue: "61", Output: "OK"},
		{OpID: 2, ClientID: "c0", CallTime: 300, ReturnTime: 400, OpType: "Get", Key: k, Output: "FOUND", OutputValue: "61"},
		{OpID: 3, ClientID: "c1", CallTime: 500, ReturnTime: 600, OpType: "Put", Key: k, InputValue: "62", Output: "OK"},
		{OpID: 4, ClientID: "c0", CallTime: 700, ReturnTime: 800, OpType: "Get", Key: k, Output: "FOUND", OutputValue: "62"},
		{OpID: 5, ClientID: "c1", CallTime: 900, ReturnTime: 1000, OpType: "Delete", Key: k, Output: "OK"},
		{OpID: 6, ClientID: "c0", CallTime: 1100, ReturnTime: 1200, OpType: "Get", Key: k, Output: "NOT_FOUND"},
	}
}

// generateBadHistory creates a non-linearizable history:
// Client 0: Put(k1, "a") -> OK     at t=[100, 200]
// Client 1: Put(k1, "b") -> OK     at t=[300, 400]
// Client 0: Get(k1) -> FOUND("a")  at t=[500, 600]  <-- VIOLATION: must see "b"
func generateBadHistory() []correctness.HistoryEvent {
	k := "00112233445566778899aabbccddeeff"
	return []correctness.HistoryEvent{
		{OpID: 1, ClientID: "c0", CallTime: 100, ReturnTime: 200, OpType: "Put", Key: k, InputValue: "61", Output: "OK"},
		{OpID: 2, ClientID: "c1", CallTime: 300, ReturnTime: 400, OpType: "Put", Key: k, InputValue: "62", Output: "OK"},
		{OpID: 3, ClientID: "c0", CallTime: 500, ReturnTime: 600, OpType: "Get", Key: k, Output: "FOUND", OutputValue: "61"},
	}
}

// generateTimeoutHistory: a Put returns TIMEOUT, followed by a Get that returns
// the value. The checker should PASS because the TIMEOUT Put may have taken effect.
func generateTimeoutHistory() []correctness.HistoryEvent {
	k := "00112233445566778899aabbccddeeff"
	return []correctness.HistoryEvent{
		{OpID: 1, ClientID: "c0", CallTime: 100, ReturnTime: 200, OpType: "Put", Key: k, InputValue: "61", Output: "TIMEOUT"},
		{OpID: 2, ClientID: "c0", CallTime: 300, ReturnTime: 400, OpType: "Get", Key: k, Output: "FOUND", OutputValue: "61"},
	}
}
