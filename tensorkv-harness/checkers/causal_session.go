package checkers

import (
	"fmt"
	"sort"
	"time"

	"github.com/tensorkv/harness/interfaces"
	"github.com/tensorkv/harness/recorder"
)

// CheckCausalConsistencySession verifies causal consistency via the four session
// guarantees (read your writes, monotonic reads, monotonic writes, writes follow reads).
// This checks causal consistency rather than per-key linearizability.
func CheckCausalConsistencySession(r *recorder.Recorder) error {
	history := r.GetHistory()

	putReturnTimeByKeyHash := make(map[interfaces.Key]map[[32]byte]time.Time)
	for _, e := range history {
		if e.Type == recorder.OpPut && e.Err == nil {
			if putReturnTimeByKeyHash[e.Key] == nil {
				putReturnTimeByKeyHash[e.Key] = make(map[[32]byte]time.Time)
			}
			putReturnTimeByKeyHash[e.Key][e.ValueHash] = e.ReturnTime
		}
	}

	// Order hashes per key by put return time (causal order).
	keyHashOrder := make(map[interfaces.Key][]hashWithTime)
	for key, m := range putReturnTimeByKeyHash {
		for h, t := range m {
			keyHashOrder[key] = append(keyHashOrder[key], hashWithTime{h: h, t: t})
		}
		sort.Slice(keyHashOrder[key], func(i, j int) bool {
			return keyHashOrder[key][i].t.Before(keyHashOrder[key][j].t)
		})
	}

	// Read your writes: per client per key, after Put(H), subsequent Get must return H or later.
	type ck struct {
		clientID int
		key      interfaces.Key
	}
	lastWriteTime := make(map[ck]time.Time)
	lastWriteHash := make(map[ck][32]byte)

	for _, e := range history {
		if e.Type == recorder.OpPut && e.Err == nil {
			ck := ck{e.ClientID, e.Key}
			lastWriteTime[ck] = e.ReturnTime
			lastWriteHash[ck] = e.ValueHash
			continue
		}
		if e.Type != recorder.OpGet || e.Err != nil {
			continue
		}
		ck := ck{e.ClientID, e.Key}
		wt, ok := lastWriteTime[ck]
		if !ok {
			continue
		}
		writtenHash := lastWriteHash[ck]
		if e.ValueHash == writtenHash {
			continue
		}
		eTime, ok := putReturnTimeByKeyHash[e.Key][e.ValueHash]
		if !ok {
			continue
		}
		if eTime.Before(wt) {
			return fmt.Errorf(
				"read your writes violation: client %d wrote key %x (hash %x at %v), then read key %x got hash %x (written at %v) — older than own write",
				e.ClientID, e.Key, writtenHash, wt, e.Key, e.ValueHash, eTime,
			)
		}
	}

	// Writes follow reads: if C read X=H1 then wrote Y=H2, any client D that does Get(Y)=H2
	// must have a view where Get(X) returns H1 or newer (when D later reads X).
	type readDep struct {
		key  interfaces.Key
		hash [32]byte
	}
	var deps []struct {
		writer   int
		writeKey interfaces.Key
		writeHash [32]byte
		readKey  interfaces.Key
		readHash [32]byte
	}
	clientReads := make(map[int][]readDep)
	for _, e := range history {
		if e.Type == recorder.OpGet && e.Err == nil {
			clientReads[e.ClientID] = append(clientReads[e.ClientID], readDep{e.Key, e.ValueHash})
			continue
		}
		if e.Type == recorder.OpPut && e.Err == nil {
			for _, d := range clientReads[e.ClientID] {
				deps = append(deps, struct {
					writer    int
					writeKey  interfaces.Key
					writeHash [32]byte
					readKey   interfaces.Key
					readHash  [32]byte
				}{e.ClientID, e.Key, e.ValueHash, d.key, d.hash})
			}
			clientReads[e.ClientID] = nil
		}
	}
	for _, dep := range deps {
		depTime, ok := putReturnTimeByKeyHash[dep.readKey][dep.readHash]
		if !ok {
			continue
		}
		for _, e2 := range history {
			if e2.Type != recorder.OpGet || e2.Err != nil || e2.Key != dep.writeKey || e2.ValueHash != dep.writeHash {
				continue
			}
			if e2.ReturnTime.Before(depTime) {
				continue
			}
			c2 := e2.ClientID
			for _, e3 := range history {
				if e3.ClientID != c2 || e3.Type != recorder.OpGet || e3.Key != dep.readKey {
					continue
				}
				if e3.CallTime.Before(e2.ReturnTime) {
					continue
				}
				if e3.Err != nil {
					continue
				}
				e3Time, ok := putReturnTimeByKeyHash[dep.readKey][e3.ValueHash]
				if !ok {
					continue
				}
				if e3Time.Before(depTime) {
					return fmt.Errorf(
						"writes follow reads violation: client read key %x hash %x then wrote key %x; client %d observed that write but later read key %x got hash %x (older than %x)",
						dep.readKey, dep.readHash[:8], dep.writeKey, c2, dep.readKey, e3.ValueHash[:8], dep.readHash[:8],
					)
				}
			}
		}
	}
	return nil
}

type hashWithTime struct {
	h [32]byte
	t time.Time
}
