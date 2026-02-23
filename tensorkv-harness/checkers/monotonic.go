package checkers

import (
	"fmt"
	"time"

	"github.com/tensorkv/harness/interfaces"
	"github.com/tensorkv/harness/recorder"
)

// CheckMonotonicReads verifies the per-client, per-key monotonic reads guarantee.
//
// Monotonic reads: if a client reads key K and observes value V1, any subsequent
// read of K by the same client must return V1 or a value that was written AFTER V1.
//
// Write ordering is determined per-read, not globally per-hash. For each Get that
// returned hash H, the "effective write time" is the ReturnTime of the latest
// successful Put of H that completed at or before the Get's own ReturnTime. A
// sequence of reads is monotone if their effective write times are non-decreasing.
//
// Using the latest eligible write (rather than the global earliest write of a hash)
// avoids false positives when the same hash appears multiple times in the history —
// common under hot-key workloads where a frequently-written key is re-written with
// the same random value bytes.
//
// Edge cases handled:
//   - H1 == H2: same version, trivially monotone (effective times are equal).
//   - No eligible Put for hash at or before the read: phantom territory, skipped.
func CheckMonotonicReads(r *recorder.Recorder) error {
	history := r.GetHistory()

	// Collect all successful Puts per key (all occurrences, including duplicates).
	type writeInfo struct {
		hash       [32]byte
		returnTime time.Time
	}
	allWrites := make(map[interfaces.Key][]writeInfo)
	for _, e := range history {
		if e.Type == recorder.OpPut && e.Err == nil {
			allWrites[e.Key] = append(allWrites[e.Key], writeInfo{e.ValueHash, e.ReturnTime})
		}
	}

	// latestWriteAtOrBefore returns the ReturnTime of the latest Put for (key, hash)
	// that completed at or before deadline. Returns zero if none exists.
	latestWriteAtOrBefore := func(key interfaces.Key, hash [32]byte, deadline time.Time) time.Time {
		var best time.Time
		for _, w := range allWrites[key] {
			if w.hash == hash && !w.returnTime.After(deadline) && w.returnTime.After(best) {
				best = w.returnTime
			}
		}
		return best
	}

	// Track per client per key: the effective write time of the last observed value.
	type clientKey struct {
		clientID int
		key      interfaces.Key
	}
	lastWriteTime := make(map[clientKey]time.Time)
	lastHash := make(map[clientKey][32]byte)

	// Walk events in CallTime order (GetHistory sorts by CallTime).
	for _, e := range history {
		if e.Type != recorder.OpGet || e.Err != nil {
			continue
		}

		// Find the latest write of this hash that could have produced this read.
		// If none exists, it is a phantom read — the phantom checker handles it.
		wt := latestWriteAtOrBefore(e.Key, e.ValueHash, e.ReturnTime)
		if wt.IsZero() {
			continue
		}

		ck := clientKey{e.ClientID, e.Key}
		prev, seen := lastWriteTime[ck]
		if seen && wt.Before(prev) {
			prevHash := lastHash[ck]
			return fmt.Errorf(
				"monotonic read violation: client %d read key %x, "+
					"got hash %x (written at %v) after previously reading hash %x (written at %v) "+
					"— went backwards in time",
				e.ClientID,
				e.Key,
				e.ValueHash,
				wt,
				prevHash,
				prev,
			)
		}

		lastWriteTime[ck] = wt
		lastHash[ck] = e.ValueHash
	}
	return nil
}
