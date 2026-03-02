package checker

import (
	"fmt"

	"ai-kv-store/correctness"

	"github.com/anishathalye/porcupine"
)

var _ = porcupine.Ok // ensure import is used

// SelfTest runs the built-in self-test suite without requiring a running cluster.
//
// It verifies:
//  1. goodHistory() is accepted as linearizable (should return porcupine.Ok).
//  2. badHistory()  is rejected as non-linearizable (should return porcupine.Illegal).
//
// Returns true if both sub-tests pass.
func SelfTest() bool {
	pass := true

	// --- Test 1: known-good sequential history ---
	goodOps := BuildOperations(goodHistory())
	goodOk := porcupine.CheckOperations(KVModel(), goodOps)
	if !goodOk {
		fmt.Printf("SELF-TEST FAIL: good history should be linearizable\n")
		pass = false
	} else {
		fmt.Println("SELF-TEST good-history: PASS")
	}

	// --- Test 2: known-bad concurrent history ---
	badOps := BuildOperations(badHistory())
	badOk := porcupine.CheckOperations(KVModel(), badOps)
	if badOk {
		fmt.Printf("SELF-TEST FAIL: bad history should be non-linearizable\n")
		pass = false
	} else {
		fmt.Println("SELF-TEST bad-history: PASS")
	}

	return pass
}

// goodHistory returns a simple sequential history that IS linearizable:
//
//	put(k1, v1) -> ok
//	get(k1)     -> found(v1)
//	delete(k1)  -> ok
//	get(k1)     -> not_found
//
// Operations are strictly sequential: each call starts after the previous return.
func goodHistory() []correctness.HistoryEntry {
	// Key: 32 hex chars (16 zero bytes)
	k1 := "00000000000000000000000000000001"
	// Value: base64 of "v1"
	v1 := "djE=" // base64.StdEncoding.EncodeToString([]byte("v1"))

	// Times in nanoseconds, strictly sequential.
	var t int64 = 1_000_000_000 // 1s base

	return []correctness.HistoryEntry{
		{
			OpID:         1,
			ClientID:     0,
			CallTimeNs:   t,
			ReturnTimeNs: t + 10_000_000, // +10ms
			OpType:       correctness.OpPut,
			Key:          k1,
			InputValue:   v1,
			Status:       "ok",
			OutputValue:  "",
		},
		{
			OpID:         2,
			ClientID:     0,
			CallTimeNs:   t + 20_000_000,
			ReturnTimeNs: t + 30_000_000,
			OpType:       correctness.OpGet,
			Key:          k1,
			InputValue:   "",
			Status:       "found",
			OutputValue:  v1,
		},
		{
			OpID:         3,
			ClientID:     0,
			CallTimeNs:   t + 40_000_000,
			ReturnTimeNs: t + 50_000_000,
			OpType:       correctness.OpDelete,
			Key:          k1,
			InputValue:   "",
			Status:       "ok",
			OutputValue:  "",
		},
		{
			OpID:         4,
			ClientID:     0,
			CallTimeNs:   t + 60_000_000,
			ReturnTimeNs: t + 70_000_000,
			OpType:       correctness.OpGet,
			Key:          k1,
			InputValue:   "",
			Status:       "not_found",
			OutputValue:  "",
		},
	}
}

// badHistory returns a history that is NOT linearizable.
//
// Scenario:
//   - Client 0: put(k, "v1")  call=0ms   return=100ms
//   - Client 1: get(k)        call=50ms   return=150ms -> returns "v2"
//
// "v2" was never written to k, so returning it from get violates linearizability.
// The only legal return values for the get are "not_found" (if linearized before
// the put) or "v1" (if linearized after the put). Returning "v2" is illegal.
func badHistory() []correctness.HistoryEntry {
	k := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	v1 := "djE=" // base64("v1")
	v2 := "djI=" // base64("v2") — never written

	var base int64 = 2_000_000_000 // 2s base

	return []correctness.HistoryEntry{
		{
			OpID:         10,
			ClientID:     0,
			CallTimeNs:   base,
			ReturnTimeNs: base + 100_000_000, // +100ms
			OpType:       correctness.OpPut,
			Key:          k,
			InputValue:   v1,
			Status:       "ok",
			OutputValue:  "",
		},
		{
			OpID:         11,
			ClientID:     1,
			CallTimeNs:   base + 50_000_000,  // overlaps with the put
			ReturnTimeNs: base + 150_000_000, // returns after the put
			OpType:       correctness.OpGet,
			Key:          k,
			InputValue:   "",
			// v2 was never written — illegal return value.
			Status:      "found",
			OutputValue: v2,
		},
	}
}
