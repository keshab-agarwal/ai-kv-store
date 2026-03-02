package kvstore

import "sync"

// KVStore is an in-memory key-value state machine protected by a read-write mutex.
// Keys and values are raw byte slices. All methods are safe for concurrent use.
type KVStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// NewKVStore creates and returns an empty KVStore.
func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string][]byte),
	}
}

// Get returns the value associated with key and true if the key exists,
// or nil and false if it does not.
func (s *KVStore) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	if !ok {
		return nil, false
	}
	// Return a defensive copy so callers cannot mutate internal state.
	out := make([]byte, len(v))
	copy(out, v)
	return out, true
}

// Put stores val under key, overwriting any previous value.
func (s *KVStore) Put(key string, val []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]byte, len(val))
	copy(cp, val)
	s.data[key] = cp
}

// Delete removes key from the store. It is a no-op if the key does not exist.
func (s *KVStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}

// Snapshot returns a shallow copy of the entire store as a map.
// Values are copied defensively.
func (s *KVStore) Snapshot() map[string][]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string][]byte, len(s.data))
	for k, v := range s.data {
		cp := make([]byte, len(v))
		copy(cp, v)
		out[k] = cp
	}
	return out
}

// ApplySnapshot replaces the store's contents with the provided map.
// Values are copied defensively.
func (s *KVStore) ApplySnapshot(m map[string][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[string][]byte, len(m))
	for k, v := range m {
		cp := make([]byte, len(v))
		copy(cp, v)
		s.data[k] = cp
	}
}

// Len returns the number of keys currently stored.
func (s *KVStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}
