package correctness

import (
	"fmt"

	"github.com/anishathalye/porcupine"
)

// KVInput represents the input to a KV store operation for the Porcupine model.
type KVInput struct {
	OpType string // "get", "put", "delete"
	Key    string // hex-encoded key
	Value  string // hex-encoded value (for put)
}

func (i KVInput) String() string {
	switch i.OpType {
	case "put":
		return fmt.Sprintf("put(%s, %s)", i.Key, truncate(i.Value, 16))
	case "get":
		return fmt.Sprintf("get(%s)", i.Key)
	case "delete":
		return fmt.Sprintf("delete(%s)", i.Key)
	default:
		return fmt.Sprintf("%s(%s)", i.OpType, i.Key)
	}
}

// KVOutput represents the output of a KV store operation for the Porcupine model.
type KVOutput struct {
	Status string // "ok", "found", "not_found"
	Value  string // hex-encoded value (for get when found)
}

func (o KVOutput) String() string {
	if o.Status == "found" {
		return fmt.Sprintf("found(%s)", truncate(o.Value, 16))
	}
	return o.Status
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// kvState is the state type for the Porcupine model: a map from key to value.
// A missing key means the key has never been written or has been deleted.
type kvState map[string]string

func copyState(s kvState) kvState {
	out := make(kvState, len(s))
	for k, v := range s {
		out[k] = v
	}
	return out
}

// KVModel returns a porcupine.Model for the distributed KV store.
//
// Sequential specification:
//   - Get returns the value from the last Put for that key ("found" + value),
//     or "not_found" if the key was never written or was deleted.
//   - Put stores a value for a key and returns "ok".
//   - Delete removes a key and returns "ok". Deleting a non-existent key also returns "ok".
//
// TIMEOUT handling:
//   Operations that returned "timeout" or "error" are EXCLUDED from the
//   Porcupine check before calling this model. They are indeterminate:
//   the operation may or may not have been applied. Including them would
//   make the linearizability check unsound (false failures) or incomplete.
//   They are still recorded in the history JSONL for auditing purposes.
func KVModel() porcupine.Model {
	return porcupine.Model{
		Init: func() interface{} {
			return make(kvState)
		},
		Step: func(state interface{}, input interface{}, output interface{}) (bool, interface{}) {
			s := copyState(state.(kvState))
			inp := input.(KVInput)
			out := output.(KVOutput)

			switch inp.OpType {
			case "get":
				val, exists := s[inp.Key]
				if exists {
					// Key exists: output must be "found" with matching value.
					return out.Status == "found" && out.Value == val, s
				}
				// Key does not exist: output must be "not_found".
				return out.Status == "not_found", s

			case "put":
				s[inp.Key] = inp.Value
				return out.Status == "ok", s

			case "delete":
				delete(s, inp.Key)
				return out.Status == "ok", s

			default:
				return false, s
			}
		},
		Equal: func(state1, state2 interface{}) bool {
			s1 := state1.(kvState)
			s2 := state2.(kvState)
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
			return fmt.Sprintf("%s -> %s", input.(KVInput), output.(KVOutput))
		},
	}
}

// HistoryToOperations converts a slice of HistoryEvents into porcupine.Operation
// entries suitable for linearizability checking.
//
// Events with status "timeout" or "error" are SKIPPED because they are
// indeterminate — the operation may or may not have been applied on the server.
// Including indeterminate operations would cause spurious failures.
func HistoryToOperations(events []HistoryEvent) []porcupine.Operation {
	var ops []porcupine.Operation
	for _, ev := range events {
		// Skip indeterminate operations.
		if ev.Status == "timeout" || ev.Status == "error" {
			continue
		}

		inp := KVInput{
			OpType: ev.OpType,
			Key:    ev.Key,
			Value:  ev.InputValue,
		}

		out := KVOutput{
			Status: ev.Status,
			Value:  ev.OutputValue,
		}

		ops = append(ops, porcupine.Operation{
			ClientId: ev.ClientID,
			Input:    inp,
			Output:   out,
			Call:     ev.CallTime,
			Return:   ev.ReturnTime,
		})
	}
	return ops
}
