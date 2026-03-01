// Package checker integrates the Porcupine linearizability checker with the KV store history.
package checker

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/anishathalye/porcupine"

	"ai-kv-store/task3_harness/history"
)

// KVInput represents the input to a KV operation for the Porcupine model.
type KVInput struct {
	Op    string // "get", "put", "delete"
	Key   string // hex-encoded key
	Value string // base64-encoded value (for put)
}

// KVOutput represents the output of a KV operation for the Porcupine model.
type KVOutput struct {
	Status string // "ok", "found", "not_found"
	Value  string // base64-encoded value (for get found)
}

// KVModel is the Porcupine model for the KV store.
var KVModel = porcupine.Model{
	Init: func() interface{} {
		return make(map[string]string)
	},
	Step: func(state interface{}, input interface{}, output interface{}) (bool, interface{}) {
		st := state.(map[string]string)
		// Deep copy state (model must be pure).
		newSt := make(map[string]string, len(st))
		for k, v := range st {
			newSt[k] = v
		}

		inp := input.(KVInput)
		out := output.(KVOutput)

		switch inp.Op {
		case "put":
			newSt[inp.Key] = inp.Value
			// Put must return "ok".
			return out.Status == "ok", newSt
		case "get":
			val, exists := newSt[inp.Key]
			if exists {
				// Key exists: output must be "found" with the correct value.
				return out.Status == "found" && out.Value == val, newSt
			}
			// Key does not exist: output must be "not_found".
			return out.Status == "not_found", newSt
		case "delete":
			delete(newSt, inp.Key)
			// Delete is idempotent: deleting a missing key is ok.
			return out.Status == "ok", newSt
		default:
			return false, newSt
		}
	},
	Equal: func(state1, state2 interface{}) bool {
		s1 := state1.(map[string]string)
		s2 := state2.(map[string]string)
		if len(s1) != len(s2) {
			return false
		}
		for k, v := range s1 {
			if s2[k] != v {
				return false
			}
		}
		return true
	},
	DescribeOperation: func(input interface{}, output interface{}) string {
		inp := input.(KVInput)
		out := output.(KVOutput)
		shortKey := inp.Key
		if len(shortKey) > 8 {
			shortKey = shortKey[:8] + "..."
		}
		switch inp.Op {
		case "put":
			shortVal := inp.Value
			if len(shortVal) > 16 {
				shortVal = shortVal[:16] + "..."
			}
			return fmt.Sprintf("put(%s, %s) -> %s", shortKey, shortVal, out.Status)
		case "get":
			shortVal := out.Value
			if len(shortVal) > 16 {
				shortVal = shortVal[:16] + "..."
			}
			if out.Status == "found" {
				return fmt.Sprintf("get(%s) -> found(%s)", shortKey, shortVal)
			}
			return fmt.Sprintf("get(%s) -> %s", shortKey, out.Status)
		case "delete":
			return fmt.Sprintf("delete(%s) -> %s", shortKey, out.Status)
		default:
			return fmt.Sprintf("%s(%s) -> %s", inp.Op, shortKey, out.Status)
		}
	},
	DescribeState: func(state interface{}) string {
		st := state.(map[string]string)
		if len(st) == 0 {
			return "{}"
		}
		// Sort keys for deterministic output.
		keys := make([]string, 0, len(st))
		for k := range st {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var parts []string
		for _, k := range keys {
			shortKey := k
			if len(shortKey) > 8 {
				shortKey = shortKey[:8] + "..."
			}
			shortVal := st[k]
			if len(shortVal) > 16 {
				shortVal = shortVal[:16] + "..."
			}
			parts = append(parts, fmt.Sprintf("%s=%s", shortKey, shortVal))
		}
		return "{" + strings.Join(parts, ", ") + "}"
	},
}

// CheckResult holds the result of a linearizability check.
type CheckResult struct {
	Ok             bool
	PorcupineResult porcupine.CheckResult
	Info           porcupine.LinearizationInfo
	TotalOps       int
	SkippedOps     int // operations skipped due to error status
	Duration       time.Duration
}

// CheckHistory checks the given history for linearizability.
func CheckHistory(entries []history.HistoryEntry) CheckResult {
	start := time.Now()

	// Assign client IDs as integers (zero-indexed).
	clientMap := make(map[string]int)
	nextClientID := 0

	// Filter and convert entries to Porcupine operations.
	var ops []porcupine.Operation
	skipped := 0

	for _, entry := range entries {
		// Skip operations that returned error -- they provide no linearizability info.
		if entry.Status == "error" {
			skipped++
			continue
		}

		// Map client IDs to integers.
		cid, ok := clientMap[entry.ClientID]
		if !ok {
			cid = nextClientID
			clientMap[entry.ClientID] = cid
			nextClientID++
		}

		inp := KVInput{
			Op:    entry.OpType,
			Key:   entry.Key,
			Value: entry.InputValue,
		}
		out := KVOutput{
			Status: entry.Status,
			Value:  entry.OutputValue,
		}

		callTime := entry.CallTime
		returnTime := entry.ReturnTime

		// Handle timeout: the operation could have linearized at any point after invocation.
		if entry.Status == "timeout" {
			returnTime = math.MaxInt64 / 2
			// For timeout puts, we need to allow both "ok" and "timeout" in the model.
			// We treat the operation as if it succeeded (could have been applied).
			switch entry.OpType {
			case "put":
				out.Status = "ok"
			case "delete":
				out.Status = "ok"
			case "get":
				// For get timeouts, we skip them since we don't know the result.
				skipped++
				continue
			}
		}

		// Ensure call time < return time.
		if returnTime <= callTime {
			returnTime = callTime + 1
		}

		ops = append(ops, porcupine.Operation{
			ClientId: cid,
			Input:    inp,
			Output:   out,
			Call:     callTime,
			Return:   returnTime,
		})
	}

	if len(ops) == 0 {
		return CheckResult{
			Ok:              true,
			PorcupineResult: porcupine.Ok,
			TotalOps:        len(entries),
			SkippedOps:      skipped,
			Duration:        time.Since(start),
		}
	}

	// Run Porcupine with verbose output for visualization.
	result, info := porcupine.CheckOperationsVerbose(KVModel, ops, 30*time.Second)

	return CheckResult{
		Ok:              result == porcupine.Ok,
		PorcupineResult: result,
		Info:            info,
		TotalOps:        len(entries),
		SkippedOps:      skipped,
		Duration:        time.Since(start),
	}
}

// WriteVisualization writes a Porcupine visualization to the given path.
func WriteVisualization(result CheckResult, outputPath string) error {
	return porcupine.VisualizePath(KVModel, result.Info, outputPath)
}
