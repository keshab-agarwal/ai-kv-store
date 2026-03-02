package checker

import (
	"fmt"
	"time"

	"ai-kv-store/correctness"

	"github.com/anishathalye/porcupine"
)

// CheckResult holds the outcome of a linearizability check.
type CheckResult struct {
	Linearizable bool
	Duration     time.Duration
	OpCount      int
	// RawResult is the porcupine.CheckResult value (Ok, Illegal, or Unknown).
	RawResult porcupine.CheckResult
}

// IsUnknown reports whether the checker timed out before reaching a verdict.
func (r CheckResult) IsUnknown() bool {
	return r.RawResult == porcupine.Unknown
}

// Check builds porcupine.Operations from the given history entries, then runs
// Porcupine's linearizability checker with the given timeout.
func Check(entries []correctness.HistoryEntry, timeout time.Duration) CheckResult {
	ops := BuildOperations(entries)
	model := KVModel()

	start := time.Now()
	raw, _ := porcupine.CheckOperationsVerbose(model, ops, timeout)
	elapsed := time.Since(start)

	linearizable := raw == porcupine.Ok
	return CheckResult{
		Linearizable: linearizable,
		Duration:     elapsed,
		OpCount:      len(ops),
		RawResult:    raw,
	}
}

// CheckFile loads a JSONL history file and runs Porcupine linearizability check.
func CheckFile(historyPath string, timeout time.Duration) (CheckResult, error) {
	entries, err := correctness.LoadJSONL(historyPath)
	if err != nil {
		return CheckResult{}, fmt.Errorf("load history %s: %w", historyPath, err)
	}
	result := Check(entries, timeout)
	return result, nil
}
