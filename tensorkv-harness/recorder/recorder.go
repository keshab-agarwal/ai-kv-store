// Package recorder provides a thread-safe recording proxy that wraps any Store
// implementation and logs every operation with precise timestamps. The recorded
// history is the foundation for all invariant checkers.
package recorder

import (
	"context"
	"crypto/sha256"
	"sort"
	"sync"
	"time"

	"github.com/tensorkv/harness/interfaces"
)

// OpType distinguishes Put from Get operations in the history.
type OpType int

const (
	// OpPut represents a Put operation.
	OpPut OpType = iota
	// OpGet represents a Get operation.
	OpGet
)

// HistoryEvent captures a single completed operation with timing and outcome.
type HistoryEvent struct {
	// ClientID identifies the goroutine/client that issued the operation.
	ClientID int

	// Type is OpPut or OpGet.
	Type OpType

	// Key is the key that was accessed.
	Key interfaces.Key

	// ValueHash is the SHA-256 of the value. For failed operations this is zero.
	ValueHash [32]byte

	// Err is the error returned by the operation, or nil on success.
	Err error

	// CallTime is when the client issued the operation (before network send).
	CallTime time.Time

	// ReturnTime is when the client received the response.
	ReturnTime time.Time
}

// RecordingStore is a Store proxy that records every operation into a shared Recorder.
// Multiple RecordingStores can share a single Recorder (for multi-client tests).
type RecordingStore struct {
	inner    interfaces.Store
	clientID int
	recorder *Recorder
}

// Recorder collects HistoryEvents from one or more RecordingStores.
// It is safe for concurrent use.
type Recorder struct {
	mu      sync.Mutex
	history []HistoryEvent

	// writtenHashes tracks, per key, every ValueHash that was successfully Put.
	// Used by the phantom-read checker.
	writtenHashes map[interfaces.Key]map[[32]byte]bool
}

// NewRecorder creates a new, empty Recorder.
func NewRecorder() *Recorder {
	return &Recorder{
		writtenHashes: make(map[interfaces.Key]map[[32]byte]bool),
	}
}

// NewRecordingStore wraps inner with recording, tagging all events with clientID.
// The clientID should be unique per concurrent client in a workload.
func NewRecordingStore(inner interfaces.Store, clientID int, r *Recorder) *RecordingStore {
	return &RecordingStore{
		inner:    inner,
		clientID: clientID,
		recorder: r,
	}
}

// Put records the call, delegates to the inner store, and records the return.
func (rs *RecordingStore) Put(ctx context.Context, key interfaces.Key, value interfaces.Value) error {
	callTime := time.Now()
	err := rs.inner.Put(ctx, key, value)
	returnTime := time.Now()

	h := hashValue(value)
	event := HistoryEvent{
		ClientID:   rs.clientID,
		Type:       OpPut,
		Key:        key,
		ValueHash:  h,
		Err:        err,
		CallTime:   callTime,
		ReturnTime: returnTime,
	}

	rs.recorder.record(event, true)
	return err
}

// Get records the call, delegates to the inner store, and records the return.
func (rs *RecordingStore) Get(ctx context.Context, key interfaces.Key) (interfaces.Value, error) {
	callTime := time.Now()
	value, err := rs.inner.Get(ctx, key)
	returnTime := time.Now()

	var h [32]byte
	if err == nil {
		h = hashValue(value)
	}
	event := HistoryEvent{
		ClientID:   rs.clientID,
		Type:       OpGet,
		Key:        key,
		ValueHash:  h,
		Err:        err,
		CallTime:   callTime,
		ReturnTime: returnTime,
	}

	rs.recorder.record(event, false)
	return value, err
}

// record appends an event to the history and, for successful Puts, registers the hash.
func (r *Recorder) record(event HistoryEvent, isPut bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.history = append(r.history, event)

	if isPut && event.Err == nil {
		if r.writtenHashes[event.Key] == nil {
			r.writtenHashes[event.Key] = make(map[[32]byte]bool)
		}
		r.writtenHashes[event.Key][event.ValueHash] = true
	}
}

// GetHistory returns a snapshot of all recorded events, sorted by CallTime.
func (r *Recorder) GetHistory() []HistoryEvent {
	r.mu.Lock()
	defer r.mu.Unlock()

	snapshot := make([]HistoryEvent, len(r.history))
	copy(snapshot, r.history)

	sort.Slice(snapshot, func(i, j int) bool {
		return snapshot[i].CallTime.Before(snapshot[j].CallTime)
	})
	return snapshot
}

// WasWritten returns true if the given hash was ever successfully Put for key.
// This is the authoritative source for phantom-read checking.
func (r *Recorder) WasWritten(key interfaces.Key, hash [32]byte) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	hashes, ok := r.writtenHashes[key]
	if !ok {
		return false
	}
	return hashes[hash]
}

// WrittenHashesForKey returns a copy of all hashes successfully written for key,
// sorted by the ReturnTime of their Put operations (oldest first).
// This is used by the monotonic-reads checker to determine write ordering.
func (r *Recorder) WrittenHashesForKey(key interfaces.Key) [][32]byte {
	r.mu.Lock()
	history := make([]HistoryEvent, len(r.history))
	copy(history, r.history)
	r.mu.Unlock()

	// Collect successful Puts for this key, keeping track of their ReturnTimes.
	type putRecord struct {
		hash       [32]byte
		returnTime time.Time
	}
	var puts []putRecord
	for _, e := range history {
		if e.Type == OpPut && e.Err == nil && e.Key == key {
			puts = append(puts, putRecord{hash: e.ValueHash, returnTime: e.ReturnTime})
		}
	}

	sort.Slice(puts, func(i, j int) bool {
		return puts[i].returnTime.Before(puts[j].returnTime)
	})

	result := make([][32]byte, len(puts))
	for i, p := range puts {
		result[i] = p.hash
	}
	return result
}

// PutReturnTimeForHash returns the ReturnTime of the first successful Put that
// produced the given hash for the given key. Returns zero time if not found.
func (r *Recorder) PutReturnTimeForHash(key interfaces.Key, hash [32]byte) time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()

	var earliest time.Time
	for _, e := range r.history {
		if e.Type == OpPut && e.Err == nil && e.Key == key && e.ValueHash == hash {
			if earliest.IsZero() || e.ReturnTime.Before(earliest) {
				earliest = e.ReturnTime
			}
		}
	}
	return earliest
}

// InjectEvent directly inserts a pre-built HistoryEvent into the recorder.
// This is intended for testing the checkers with synthetic histories — do not
// use it in production workloads.
func (r *Recorder) InjectEvent(event HistoryEvent) {
	r.record(event, event.Type == OpPut)
}

// Merge combines histories from multiple Recorders into a single Recorder.
// This is used after a multi-client workload to aggregate all events.
func Merge(recorders ...*Recorder) *Recorder {
	merged := NewRecorder()

	for _, r := range recorders {
		r.mu.Lock()
		for _, e := range r.history {
			merged.history = append(merged.history, e)
			if e.Type == OpPut && e.Err == nil {
				if merged.writtenHashes[e.Key] == nil {
					merged.writtenHashes[e.Key] = make(map[[32]byte]bool)
				}
				merged.writtenHashes[e.Key][e.ValueHash] = true
			}
		}
		r.mu.Unlock()
	}

	return merged
}

// hashValue computes the SHA-256 of a value.
func hashValue(v interfaces.Value) [32]byte {
	return sha256.Sum256(v)
}
