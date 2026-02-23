package checkers_test

import (
	"crypto/sha256"
	"testing"
	"time"

	"github.com/tensorkv/harness/checkers"
	"github.com/tensorkv/harness/interfaces"
	"github.com/tensorkv/harness/recorder"
)

// ---- helpers ----------------------------------------------------------------

var (
	keyA = interfaces.Key{0x01}
	keyB = interfaces.Key{0x02}

	val1 = makeValue(1, 'A')
	val2 = makeValue(2, 'B')
	val3 = makeValue(3, 'C')

	hash1 = sha256.Sum256(val1)
	hash2 = sha256.Sum256(val2)
	hash3 = sha256.Sum256(val3)
)

// makeValue creates a 1 MB value filled with the given byte.
func makeValue(id int, fill byte) interfaces.Value {
	v := make([]byte, interfaces.MinValueSize)
	v[0] = byte(id)
	for i := 1; i < len(v); i++ {
		v[i] = fill
	}
	return v
}

// t0 is a base time for synthetic histories.
var t0 = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

func ts(seconds int) time.Time { return t0.Add(time.Duration(seconds) * time.Second) }

// buildRecorder constructs a Recorder pre-populated with the given events.
func buildRecorder(events []recorder.HistoryEvent) *recorder.Recorder {
	r := recorder.NewRecorder()
	for _, e := range events {
		// Use the exported Inject helper to feed synthetic events.
		r.InjectEvent(e)
	}
	return r
}

// ---- known-good (linearizable) history ---------------------------------------

// goodHistory: client 0 writes val1, client 1 reads val1. All consistent.
func goodHistory() []recorder.HistoryEvent {
	return []recorder.HistoryEvent{
		{ClientID: 0, Type: recorder.OpPut, Key: keyA, ValueHash: hash1, Err: nil, CallTime: ts(0), ReturnTime: ts(1)},
		{ClientID: 1, Type: recorder.OpGet, Key: keyA, ValueHash: hash1, Err: nil, CallTime: ts(2), ReturnTime: ts(3)},
	}
}

func TestGoodHistory_NoPhantomReads(t *testing.T) {
	r := buildRecorder(goodHistory())
	if err := checkers.CheckNoPhantomReads(r); err != nil {
		t.Errorf("expected pass, got: %v", err)
	}
}

func TestGoodHistory_MonotonicReads(t *testing.T) {
	r := buildRecorder(goodHistory())
	if err := checkers.CheckMonotonicReads(r); err != nil {
		t.Errorf("expected pass, got: %v", err)
	}
}

func TestGoodHistory_CausalConsistency(t *testing.T) {
	r := buildRecorder(goodHistory())
	if err := checkers.CheckCausalConsistency(r); err != nil {
		t.Errorf("expected pass, got: %v", err)
	}
}

// ---- phantom read history ---------------------------------------------------

// phantomHistory: client 1 reads a hash that was never written.
func phantomHistory() []recorder.HistoryEvent {
	return []recorder.HistoryEvent{
		{ClientID: 0, Type: recorder.OpPut, Key: keyA, ValueHash: hash1, Err: nil, CallTime: ts(0), ReturnTime: ts(1)},
		// hash2 was never written; this is a phantom.
		{ClientID: 1, Type: recorder.OpGet, Key: keyA, ValueHash: hash2, Err: nil, CallTime: ts(2), ReturnTime: ts(3)},
	}
}

func TestPhantomHistory_PhantomChecker_Catches(t *testing.T) {
	r := buildRecorder(phantomHistory())
	if err := checkers.CheckNoPhantomReads(r); err == nil {
		t.Error("expected phantom read violation, got nil")
	}
}

// ---- monotonic read violation -----------------------------------------------

// monotonicViolationHistory: client 0 reads val2 (written later), then reads
// val1 (written earlier) — goes backwards.
func monotonicViolationHistory() []recorder.HistoryEvent {
	return []recorder.HistoryEvent{
		// Two successful writes for keyA at different times.
		{ClientID: 99, Type: recorder.OpPut, Key: keyA, ValueHash: hash1, Err: nil, CallTime: ts(0), ReturnTime: ts(1)},
		{ClientID: 99, Type: recorder.OpPut, Key: keyA, ValueHash: hash2, Err: nil, CallTime: ts(2), ReturnTime: ts(3)},
		// Client 0 reads the newer hash first…
		{ClientID: 0, Type: recorder.OpGet, Key: keyA, ValueHash: hash2, Err: nil, CallTime: ts(4), ReturnTime: ts(5)},
		// …then the older hash — monotonic violation.
		{ClientID: 0, Type: recorder.OpGet, Key: keyA, ValueHash: hash1, Err: nil, CallTime: ts(6), ReturnTime: ts(7)},
	}
}

func TestMonotonicViolationHistory_MonotonicChecker_Catches(t *testing.T) {
	r := buildRecorder(monotonicViolationHistory())
	if err := checkers.CheckMonotonicReads(r); err == nil {
		t.Error("expected monotonic read violation, got nil")
	}
}

func TestMonotonicViolationHistory_PhantomChecker_Passes(t *testing.T) {
	r := buildRecorder(monotonicViolationHistory())
	if err := checkers.CheckNoPhantomReads(r); err != nil {
		t.Errorf("phantom checker should pass on monotonic-violation history, got: %v", err)
	}
}

// ---- causal consistency violation -------------------------------------------

// causalViolationHistory: two overlapping writes with a read that is not
// linearizable: the read observes a write that hasn't happened yet in real time.
//
// Timeline:
//   [0,4] Client 0: Put(keyA, val1) returns at t=4
//   [1,2] Client 1: Put(keyA, val2) returns at t=2   (returns BEFORE client 0's put)
//   [3,5] Client 2: Get(keyA) -> val1  (call=3, return=5)
//
// This is NOT a linearizability violation by itself.  To force a causal violation
// we arrange for a Get that returns a value that cannot be placed consistently in
// the serial order implied by the real-time intervals.
//
// Specifically: a write that starts at t=5 and returns at t=10, combined with
// a read that starts at t=3 (before the write) and returns at t=11 (after),
// returning the value of a LATER write — Porcupine will detect the contradiction.
//
// Simpler approach: construct a history where two concurrent puts both complete,
// but a get that entirely follows both (call > both ReturnTimes) returns
// neither value — this forces Porcupine to detect the stale state.
func causalViolationHistory() []recorder.HistoryEvent {
	return []recorder.HistoryEvent{
		// Write val1 to keyA.
		{ClientID: 0, Type: recorder.OpPut, Key: keyA, ValueHash: hash1, Err: nil, CallTime: ts(0), ReturnTime: ts(1)},
		// Write val2 to keyA (happens after val1 by real time).
		{ClientID: 1, Type: recorder.OpPut, Key: keyA, ValueHash: hash2, Err: nil, CallTime: ts(2), ReturnTime: ts(3)},
		// Get that entirely follows both writes but returns val1 — stale read.
		// This is illegal because val2's Put completed before this Get started.
		{ClientID: 2, Type: recorder.OpGet, Key: keyA, ValueHash: hash1, Err: nil, CallTime: ts(4), ReturnTime: ts(5)},
	}
}

func TestCausalViolationHistory_CausalChecker_Catches(t *testing.T) {
	r := buildRecorder(causalViolationHistory())
	if err := checkers.CheckCausalConsistency(r); err == nil {
		t.Error("expected causal consistency violation, got nil")
	}
}

// ---- edge cases -------------------------------------------------------------

func TestEmptyHistory_AllCheckers_Pass(t *testing.T) {
	r := buildRecorder(nil)
	if err := checkers.CheckNoPhantomReads(r); err != nil {
		t.Errorf("phantom: %v", err)
	}
	if err := checkers.CheckMonotonicReads(r); err != nil {
		t.Errorf("monotonic: %v", err)
	}
	if err := checkers.CheckCausalConsistency(r); err != nil {
		t.Errorf("causal: %v", err)
	}
}

func TestSingleClientHistory_AllCheckers_Pass(t *testing.T) {
	events := []recorder.HistoryEvent{
		{ClientID: 0, Type: recorder.OpPut, Key: keyA, ValueHash: hash1, Err: nil, CallTime: ts(0), ReturnTime: ts(1)},
		{ClientID: 0, Type: recorder.OpGet, Key: keyA, ValueHash: hash1, Err: nil, CallTime: ts(2), ReturnTime: ts(3)},
		{ClientID: 0, Type: recorder.OpPut, Key: keyA, ValueHash: hash2, Err: nil, CallTime: ts(4), ReturnTime: ts(5)},
		{ClientID: 0, Type: recorder.OpGet, Key: keyA, ValueHash: hash2, Err: nil, CallTime: ts(6), ReturnTime: ts(7)},
	}
	r := buildRecorder(events)
	if err := checkers.CheckNoPhantomReads(r); err != nil {
		t.Errorf("phantom: %v", err)
	}
	if err := checkers.CheckMonotonicReads(r); err != nil {
		t.Errorf("monotonic: %v", err)
	}
	if err := checkers.CheckCausalConsistency(r); err != nil {
		t.Errorf("causal: %v", err)
	}
}

func TestAllFailedOperations_AllCheckers_Pass(t *testing.T) {
	events := []recorder.HistoryEvent{
		{ClientID: 0, Type: recorder.OpPut, Key: keyA, ValueHash: hash1, Err: interfaces.ErrNodeDown, CallTime: ts(0), ReturnTime: ts(1)},
		{ClientID: 1, Type: recorder.OpGet, Key: keyA, ValueHash: [32]byte{}, Err: interfaces.ErrKeyNotFound, CallTime: ts(2), ReturnTime: ts(3)},
	}
	r := buildRecorder(events)
	if err := checkers.CheckNoPhantomReads(r); err != nil {
		t.Errorf("phantom: %v", err)
	}
	if err := checkers.CheckMonotonicReads(r); err != nil {
		t.Errorf("monotonic: %v", err)
	}
	if err := checkers.CheckCausalConsistency(r); err != nil {
		t.Errorf("causal: %v", err)
	}
}

// TestMonotonic_SameVersion is valid: reading the same hash twice is monotone.
func TestMonotonic_SameVersion_Passes(t *testing.T) {
	events := []recorder.HistoryEvent{
		{ClientID: 99, Type: recorder.OpPut, Key: keyA, ValueHash: hash1, Err: nil, CallTime: ts(0), ReturnTime: ts(1)},
		{ClientID: 0, Type: recorder.OpGet, Key: keyA, ValueHash: hash1, Err: nil, CallTime: ts(2), ReturnTime: ts(3)},
		{ClientID: 0, Type: recorder.OpGet, Key: keyA, ValueHash: hash1, Err: nil, CallTime: ts(4), ReturnTime: ts(5)},
	}
	r := buildRecorder(events)
	if err := checkers.CheckMonotonicReads(r); err != nil {
		t.Errorf("expected pass for same-version read, got: %v", err)
	}
}

// TestMultiKey_NoInterference: violations on one key don't affect checks on another.
func TestMultiKey_IndependentChecks(t *testing.T) {
	events := []recorder.HistoryEvent{
		// keyA: clean history
		{ClientID: 0, Type: recorder.OpPut, Key: keyA, ValueHash: hash1, Err: nil, CallTime: ts(0), ReturnTime: ts(1)},
		{ClientID: 0, Type: recorder.OpGet, Key: keyA, ValueHash: hash1, Err: nil, CallTime: ts(2), ReturnTime: ts(3)},
		// keyB: phantom read
		{ClientID: 1, Type: recorder.OpGet, Key: keyB, ValueHash: hash3, Err: nil, CallTime: ts(2), ReturnTime: ts(3)},
	}
	r := buildRecorder(events)
	if err := checkers.CheckNoPhantomReads(r); err == nil {
		t.Error("expected phantom violation on keyB, got nil")
	}
}

// ---- benchmarks -------------------------------------------------------------

// BenchmarkCheckNoPhantomReads measures the phantom checker on a 10k-event history.
func BenchmarkCheckNoPhantomReads(b *testing.B) {
	events := make([]recorder.HistoryEvent, 0, 10000)
	for i := 0; i < 5000; i++ {
		k := interfaces.Key{byte(i % 256), byte(i / 256)}
		events = append(events,
			recorder.HistoryEvent{ClientID: 0, Type: recorder.OpPut, Key: k, ValueHash: hash1, Err: nil, CallTime: ts(i * 2), ReturnTime: ts(i*2 + 1)},
			recorder.HistoryEvent{ClientID: 1, Type: recorder.OpGet, Key: k, ValueHash: hash1, Err: nil, CallTime: ts(i*2 + 1), ReturnTime: ts(i*2 + 2)},
		)
	}
	r := buildRecorder(events)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = checkers.CheckNoPhantomReads(r)
	}
}

// BenchmarkCheckCausalConsistency measures Porcupine on a 100-event single-key history.
func BenchmarkCheckCausalConsistency(b *testing.B) {
	events := make([]recorder.HistoryEvent, 0, 100)
	for i := 0; i < 50; i++ {
		events = append(events,
			recorder.HistoryEvent{ClientID: 0, Type: recorder.OpPut, Key: keyA, ValueHash: hash1, Err: nil, CallTime: ts(i * 2), ReturnTime: ts(i*2 + 1)},
			recorder.HistoryEvent{ClientID: 1, Type: recorder.OpGet, Key: keyA, ValueHash: hash1, Err: nil, CallTime: ts(i*2 + 1), ReturnTime: ts(i*2 + 2)},
		)
	}
	r := buildRecorder(events)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = checkers.CheckCausalConsistency(r)
	}
}
