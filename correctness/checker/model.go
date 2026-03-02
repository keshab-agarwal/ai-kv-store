// Package checker provides Porcupine linearizability checking for the
// distributed KV store.
package checker

import (
	"ai-kv-store/correctness"

	"github.com/anishathalye/porcupine"
)

// KVInput is the input to a KV operation.
type KVInput struct {
	Op    correctness.OpType // "get", "put", "delete"
	Key   string             // 32-hex
	Value string             // base64 (for put), "" otherwise
}

// KVOutput is the output from a KV operation.
type KVOutput struct {
	Status string // "ok", "found", "not_found", "error", "timeout"
	Value  string // base64 (for get/found), "" otherwise
}

// KVModel returns the porcupine.Model for the KV store.
//
// State is modelled as map[string]string (key -> base64-value).
// A key absent from the map means it has never been written or was deleted.
func KVModel() porcupine.Model {
	return porcupine.Model{
		Init: func() interface{} {
			return map[string]string{}
		},

		Step: func(state interface{}, input interface{}, output interface{}) (bool, interface{}) {
			s := state.(map[string]string)
			in := input.(KVInput)
			out := output.(KVOutput)

			switch in.Op {
			case correctness.OpGet:
				val, exists := s[in.Key]
				if out.Status == "found" {
					if exists && val == out.Value {
						return true, s
					}
					return false, s
				} else if out.Status == "not_found" {
					if !exists {
						return true, s
					}
					return false, s
				}
				// Any other status (error, timeout) should have been filtered out
				// before building porcupine.Operations.
				return false, s

			case correctness.OpPut:
				if out.Status != "ok" {
					return false, s
				}
				newState := make(map[string]string, len(s)+1)
				for k, v := range s {
					newState[k] = v
				}
				newState[in.Key] = in.Value
				return true, newState

			case correctness.OpDelete:
				if out.Status != "ok" {
					return false, s
				}
				newState := make(map[string]string, len(s))
				for k, v := range s {
					if k != in.Key {
						newState[k] = v
					}
				}
				return true, newState
			}

			return false, s
		},

		Equal: func(s1, s2 interface{}) bool {
			m1 := s1.(map[string]string)
			m2 := s2.(map[string]string)
			if len(m1) != len(m2) {
				return false
			}
			for k, v := range m1 {
				if m2[k] != v {
					return false
				}
			}
			return true
		},

		DescribeOperation: func(input, output interface{}) string {
			in := input.(KVInput)
			out := output.(KVOutput)
			switch in.Op {
			case correctness.OpGet:
				return "get(" + in.Key[:8] + "…) → " + out.Status + ":" + out.Value
			case correctness.OpPut:
				return "put(" + in.Key[:8] + "…, " + in.Value + ") → " + out.Status
			case correctness.OpDelete:
				return "delete(" + in.Key[:8] + "…) → " + out.Status
			}
			return "unknown"
		},

		DescribeState: func(state interface{}) string {
			m := state.(map[string]string)
			return "kv(" + itoa(len(m)) + " keys)"
		},
	}
}

// itoa converts an int to a string without importing strconv in a hot path.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	buf := [20]byte{}
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}

// BuildOperations converts HistoryEntry records to porcupine.Operation records.
//
// Filtering rules:
//   - Entries with status "error" or "timeout" are excluded (ambiguous outcome).
//   - Entries where ReturnTimeNs == 0 (EndOp never called) are excluded.
//   - For get:    input={op:"get", key:k}          output={status, value}
//   - For put:    input={op:"put", key:k, value:v}  output={status:"ok"}
//   - For delete: input={op:"delete", key:k}        output={status:"ok"}
func BuildOperations(entries []correctness.HistoryEntry) []porcupine.Operation {
	ops := make([]porcupine.Operation, 0, len(entries))
	for _, e := range entries {
		// Skip unfinished entries.
		if e.ReturnTimeNs == 0 {
			continue
		}
		// Skip ambiguous outcomes.
		if e.Status == "error" || e.Status == "timeout" {
			continue
		}

		in := KVInput{
			Op:  e.OpType,
			Key: e.Key,
		}
		out := KVOutput{
			Status: e.Status,
		}

		switch e.OpType {
		case correctness.OpGet:
			if e.Status == "found" {
				out.Value = e.OutputValue
			}
		case correctness.OpPut:
			in.Value = e.InputValue
			if e.Status != "ok" {
				continue
			}
		case correctness.OpDelete:
			if e.Status != "ok" {
				continue
			}
		}

		ops = append(ops, porcupine.Operation{
			ClientId: e.ClientID,
			Input:    in,
			Call:     e.CallTimeNs,
			Output:   out,
			Return:   e.ReturnTimeNs,
		})
	}
	return ops
}
