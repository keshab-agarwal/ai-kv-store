// Package checkers provides independent invariant checkers for distributed KV store histories.
// Each checker examines a recorded history and returns nil (pass) or a descriptive error (fail).
package checkers

import (
	"fmt"

	"github.com/tensorkv/harness/interfaces"
	"github.com/tensorkv/harness/recorder"
)

// CheckNoPhantomReads verifies that every successful Get returned a value that was
// actually Put for the same key at some point in the history.
//
// A phantom read occurs when a client observes a value that was never written —
// this indicates fabricated data or a corrupted storage layer.
//
// The check is conservative: a value seen in a Get is valid if ANY successful Put
// for that key produced the same hash, regardless of timing. This avoids false
// positives from concurrent writes. The monotonic and causal checkers enforce
// ordering constraints on top of this baseline.
func CheckNoPhantomReads(r *recorder.Recorder) error {
	history := r.GetHistory()

	// Build a per-key set of written hashes from successful Puts.
	writtenByKey := make(map[interfaces.Key]map[[32]byte]bool)
	for _, e := range history {
		if e.Type == recorder.OpPut && e.Err == nil {
			if writtenByKey[e.Key] == nil {
				writtenByKey[e.Key] = make(map[[32]byte]bool)
			}
			writtenByKey[e.Key][e.ValueHash] = true
		}
	}

	// Verify every successful Get.
	for _, e := range history {
		if e.Type != recorder.OpGet || e.Err != nil {
			continue
		}

		hashes := writtenByKey[e.Key]
		if len(hashes) == 0 || !hashes[e.ValueHash] {
			return fmt.Errorf(
				"phantom read: client %d read hash %x for key %x at time %v, "+
					"but this hash was never written for this key",
				e.ClientID, e.ValueHash, e.Key, e.ReturnTime,
			)
		}
	}
	return nil
}
