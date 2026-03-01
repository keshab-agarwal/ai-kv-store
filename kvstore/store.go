// Package kvstore implements a distributed in-memory key-value store using
// the Resonance Consensus Protocol — a novel consensus mechanism inspired by
// coupled oscillator synchronization in physics.
//
// Terminology mapping:
//   Phase      = epoch/term (monotonically increasing)
//   Resonator  = leader (the node driving synchronization)
//   Wavefront  = log entry (a replicated state transition)
//   Propagate  = log replication RPC
//   Pulse      = heartbeat (keeps followers in phase)
//   Crystallize = commit (wavefront becomes durable)
//   Coherence Window = lease (time window for local reads)
package kvstore

import (
	"sync"
)

// Store is a thread-safe in-memory key-value store.
// Keys are 16-byte opaque identifiers; values are arbitrary byte slices up to 1 MiB.
type Store struct {
	mu   sync.RWMutex
	data map[[16]byte][]byte
}

// NewStore creates an empty Store.
func NewStore() *Store {
	return &Store{
		data: make(map[[16]byte][]byte),
	}
}

// Get retrieves the value for a key. Returns (value, true) if found, (nil, false) otherwise.
func (s *Store) Get(key [16]byte) ([]byte, bool) {
	s.mu.RLock()
	v, ok := s.data[key]
	s.mu.RUnlock()
	if !ok {
		return nil, false
	}
	// Return a copy to avoid data races
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp, true
}

// Put stores a key-value pair, overwriting any existing value.
func (s *Store) Put(key [16]byte, value []byte) {
	cp := make([]byte, len(value))
	copy(cp, value)
	s.mu.Lock()
	s.data[key] = cp
	s.mu.Unlock()
}

// Delete removes a key from the store.
func (s *Store) Delete(key [16]byte) {
	s.mu.Lock()
	delete(s.data, key)
	s.mu.Unlock()
}

// Len returns the number of keys in the store.
func (s *Store) Len() int {
	s.mu.RLock()
	n := len(s.data)
	s.mu.RUnlock()
	return n
}
