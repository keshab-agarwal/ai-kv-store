package kvstore

import "sync"

type KVStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewKVStore() *KVStore {
	return &KVStore{data: make(map[string][]byte)}
}

func (s *KVStore) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	if !ok {
		return nil, false
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true
}

func (s *KVStore) Put(key string, val []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]byte, len(val))
	copy(cp, val)
	s.data[key] = cp
}

func (s *KVStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}

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

func (s *KVStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}
