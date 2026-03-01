package server

import (
	"sync"

	"ai-kv-store/shared/types"
)

// ReplicatedLog is a thread-safe in-memory append-only log.
type ReplicatedLog struct {
	mu      sync.RWMutex
	entries []types.LogEntry
}

// NewReplicatedLog creates a new empty log.
func NewReplicatedLog() *ReplicatedLog {
	return &ReplicatedLog{
		entries: make([]types.LogEntry, 0, 1024),
	}
}

// Append adds an entry to the log. The entry's Index should be set by the caller.
func (rl *ReplicatedLog) Append(entry types.LogEntry) {
	rl.mu.Lock()
	rl.entries = append(rl.entries, entry)
	rl.mu.Unlock()
}

// AppendBatch adds multiple entries to the log.
func (rl *ReplicatedLog) AppendBatch(entries []types.LogEntry) {
	if len(entries) == 0 {
		return
	}
	rl.mu.Lock()
	rl.entries = append(rl.entries, entries...)
	rl.mu.Unlock()
}

// Get returns the entry at the given 1-based index.
// Returns nil if index is out of range.
func (rl *ReplicatedLog) Get(index uint64) *types.LogEntry {
	if index == 0 {
		return nil
	}
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	arrayIdx := int(index - 1)
	if arrayIdx < 0 || arrayIdx >= len(rl.entries) {
		return nil
	}
	e := rl.entries[arrayIdx]
	return &e
}

// Entries returns entries from 'from' to 'to' (1-based, inclusive).
// Returns nil if range is invalid.
func (rl *ReplicatedLog) Entries(from, to uint64) []types.LogEntry {
	if from == 0 || to < from {
		return nil
	}
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	fromIdx := int(from - 1)
	toIdx := int(to)
	if fromIdx >= len(rl.entries) {
		return nil
	}
	if toIdx > len(rl.entries) {
		toIdx = len(rl.entries)
	}
	result := make([]types.LogEntry, toIdx-fromIdx)
	copy(result, rl.entries[fromIdx:toIdx])
	return result
}

// EntriesFrom returns all entries starting from the given 1-based index.
func (rl *ReplicatedLog) EntriesFrom(from uint64) []types.LogEntry {
	if from == 0 {
		from = 1
	}
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	fromIdx := int(from - 1)
	if fromIdx >= len(rl.entries) {
		return nil
	}
	result := make([]types.LogEntry, len(rl.entries)-fromIdx)
	copy(result, rl.entries[fromIdx:])
	return result
}

// LastIndex returns the index of the last entry (0 if empty).
func (rl *ReplicatedLog) LastIndex() uint64 {
	rl.mu.RLock()
	n := len(rl.entries)
	rl.mu.RUnlock()
	return uint64(n)
}

// Truncate removes all entries from the given 1-based index onwards.
func (rl *ReplicatedLog) Truncate(from uint64) {
	if from == 0 {
		return
	}
	rl.mu.Lock()
	fromIdx := int(from - 1)
	if fromIdx < len(rl.entries) {
		rl.entries = rl.entries[:fromIdx]
	}
	rl.mu.Unlock()
}

// Reset clears the entire log.
func (rl *ReplicatedLog) Reset() {
	rl.mu.Lock()
	rl.entries = rl.entries[:0]
	rl.mu.Unlock()
}
