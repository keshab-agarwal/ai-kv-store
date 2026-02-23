package checkers

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/tensorkv/harness/interfaces"
	"github.com/tensorkv/harness/recorder"
)

// kvInput is the input side of a Porcupine operation for a single-key KV store.
type kvInput struct {
	isPut bool
	hash  [32]byte // only meaningful for Put: the hash of the value being written
}

// kvOutput is the output side of a Porcupine operation.
type kvOutput struct {
	hash [32]byte // hash of the returned value (zero for Put, or for failed Get)
	err  error
}

// kvState is the linearizable state for a single key: the current hash, or nil
// if the key has never been written.
type kvState struct {
	hash    [32]byte
	written bool
}

// buildKVModel constructs the Porcupine Model for a single-key KV store.
// State: kvState (nil/zero = key never written, otherwise the current hash).
// Put always succeeds and transitions state to the new hash.
// Get must return exactly the current state (or ErrKeyNotFound if never written).
func buildKVModel() porcupine.Model {
	return porcupine.Model{
		Init: func() interface{} {
			return kvState{}
		},
		Step: func(state, input, output interface{}) (bool, interface{}) {
			st := state.(kvState)
			in := input.(kvInput)
			out := output.(kvOutput)

			if in.isPut {
				// Put always succeeds in the linearizable model.
				// (Failures are modelled as the operation having no effect.)
				if out.err != nil {
					// Failed Put: state unchanged.
					return true, st
				}
				return true, kvState{hash: in.hash, written: true}
			}

			// Get: must return the current state.
			if out.err != nil {
				if errors.Is(out.err, interfaces.ErrKeyNotFound) {
					// key-not-found is only valid when the key has truly never been written.
					return !st.written, st
				}
				// Any other failure (ErrNodeDown, context cancellation, …) is a valid
				// availability failure — the node may be unreachable regardless of what
				// the key's value is.  Do not change state.
				return true, st
			}
			if !st.written {
				// Key not yet written but Get returned a value — invalid.
				return false, st
			}
			if out.hash != st.hash {
				// Returned a hash that does not match the linearized current value.
				return false, st
			}
			return true, st
		},
		Equal: func(state1, state2 interface{}) bool {
			return state1.(kvState) == state2.(kvState)
		},
		DescribeOperation: func(input, output interface{}) string {
			in := input.(kvInput)
			out := output.(kvOutput)
			if in.isPut {
				if out.err != nil {
					return fmt.Sprintf("Put(%x) -> err(%v)", in.hash, out.err)
				}
				return fmt.Sprintf("Put(%x) -> ok", in.hash)
			}
			if out.err != nil {
				return fmt.Sprintf("Get -> err(%v)", out.err)
			}
			return fmt.Sprintf("Get -> %x", out.hash)
		},
	}
}

// CheckCausalConsistency uses Porcupine to verify per-key linearizability.
//
// Causal consistency for a KV store with a single key is equivalent to
// linearizability for that key: the history of operations on that key must be
// explainable by some serial execution consistent with real time. Porcupine
// supports P-compositionality, so we partition the history by key and check
// each partition independently in parallel.
//
// On violation, a Porcupine HTML visualization is written to a temp file and
// its path is included in the error message.
func CheckCausalConsistency(r *recorder.Recorder) error {
	history := r.GetHistory()

	// Partition events by key.
	byKey := make(map[interfaces.Key][]recorder.HistoryEvent)
	for _, e := range history {
		byKey[e.Key] = append(byKey[e.Key], e)
	}

	model := buildKVModel()

	for key, events := range byKey {
		ops := make([]porcupine.Operation, 0, len(events))
		for _, e := range events {
			in := kvInput{}
			out := kvOutput{err: e.Err}

			if e.Type == recorder.OpPut {
				in.isPut = true
				in.hash = e.ValueHash // the value being written
			} else {
				// in.hash is not used for Gets; only out.hash matters.
				out.hash = e.ValueHash // the value observed (zero on error)
			}

			ops = append(ops, porcupine.Operation{
				ClientId: e.ClientID,
				Input:    in,
				Call:     e.CallTime.UnixNano(),
				Output:   out,
				Return:   e.ReturnTime.UnixNano(),
			})
		}

		result, info := porcupine.CheckOperationsVerbose(model, ops, 30*time.Second)
		if result == porcupine.Illegal {
			vizPath, vizErr := writeVisualization(key, info)
			vizNote := ""
			if vizErr == nil {
				vizNote = fmt.Sprintf(" (HTML visualization: %s)", vizPath)
			}
			// Build a human-readable summary of the conflicting operations so that
			// text-based consumers (LLM evolvers) can diagnose the violation without
			// opening the HTML file.
			return fmt.Errorf(
				"causal consistency violation on key %x%s\n%s",
				key, vizNote, describeViolation(key, events, ops),
			)
		}
		if result == porcupine.Unknown {
			// Timeout — inconclusive, surface as a warning rather than a hard failure.
			fmt.Printf("warning: causal consistency check timed out for key %x (inconclusive)\n", key)
		}
	}
	return nil
}

// writeVisualization generates a Porcupine HTML visualization for a key and
// writes it to a temp file. Returns the file path.
func writeVisualization(key interfaces.Key, info porcupine.LinearizationInfo) (string, error) {
	f, err := os.CreateTemp("", fmt.Sprintf("porcupine-%x-*.html", key))
	if err != nil {
		return "", fmt.Errorf("creating visualization file: %w", err)
	}
	defer f.Close()

	if err := porcupine.Visualize(buildKVModel(), info, f); err != nil {
		return "", fmt.Errorf("generating visualization: %w", err)
	}
	return f.Name(), nil
}

// describeViolation builds a plain-text summary of a linearizability violation
// that is readable by an LLM evolver without access to the HTML visualization.
//
// It replays the per-key Put history in ReturnTime order (the simplest possible
// serial execution) and annotates each Get whose returned hash does not match
// the expected state, showing exactly which operation is anomalous and what the
// correct value should have been.
func describeViolation(key interfaces.Key, events []recorder.HistoryEvent, _ []porcupine.Operation) string {
	// Sort a copy by CallTime for display.
	sorted := make([]recorder.HistoryEvent, len(events))
	copy(sorted, events)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].CallTime.Before(sorted[j].CallTime)
	})

	// Build the serial write timeline: Puts in ReturnTime order.
	type writeRecord struct {
		hash       [32]byte
		returnTime time.Time
	}
	var puts []writeRecord
	for _, e := range sorted {
		if e.Type == recorder.OpPut && e.Err == nil {
			puts = append(puts, writeRecord{e.ValueHash, e.ReturnTime})
		}
	}
	sort.Slice(puts, func(i, j int) bool {
		return puts[i].returnTime.Before(puts[j].returnTime)
	})

	// expectedAt returns the hash that a serial execution would hold at time t,
	// and whether the key had been written at all by that point.
	expectedAt := func(t time.Time) (hash [32]byte, written bool) {
		for _, p := range puts {
			if !p.returnTime.After(t) {
				hash = p.hash
				written = true
			}
		}
		return
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("  %d operations on key %x (chronological order):\n", len(sorted), key))
	for _, e := range sorted {
		lat := e.ReturnTime.Sub(e.CallTime)
		if e.Type == recorder.OpPut {
			status := "OK"
			if e.Err != nil {
				status = fmt.Sprintf("FAILED(%v)", e.Err)
			}
			sb.WriteString(fmt.Sprintf("    [client %2d] Put hash=%x  %s  latency=%v\n",
				e.ClientID, e.ValueHash[:8], status, lat))
		} else {
			if e.Err != nil {
				sb.WriteString(fmt.Sprintf("    [client %2d] Get -> err=%v  latency=%v\n",
					e.ClientID, e.Err, lat))
			} else {
				exp, written := expectedAt(e.ReturnTime)
				var annotation string
				switch {
				case !written:
					annotation = "  ← ANOMALY: key has no committed write before this Get in any serial order"
				case exp != e.ValueHash:
					annotation = fmt.Sprintf("  ← ANOMALY: expected hash=%x (last committed Put), got hash=%x (stale or phantom)",
						exp[:8], e.ValueHash[:8])
				}
				sb.WriteString(fmt.Sprintf("    [client %2d] Get -> hash=%x  latency=%v%s\n",
					e.ClientID, e.ValueHash[:8], lat, annotation))
			}
		}
	}
	return sb.String()
}
