package kvstore

import "sync"

type Store struct {
	mu   sync.RWMutex
	data map[Key][]byte
}

func NewStore() *Store {
	return &Store{data: make(map[Key][]byte)}
}

func (s *Store) Get(key Key) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	if !ok {
		return nil, false
	}
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp, true
}

func (s *Store) Put(key Key, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]byte, len(value))
	copy(cp, value)
	s.data[key] = cp
}

func (s *Store) Delete(key Key) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}

func (s *Store) Snapshot() map[Key][]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := make(map[Key][]byte, len(s.data))
	for k, v := range s.data {
		cp := make([]byte, len(v))
		copy(cp, v)
		snap[k] = cp
	}
	return snap
}

func (s *Store) Restore(snap map[Key][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[Key][]byte, len(snap))
	for k, v := range snap {
		cp := make([]byte, len(v))
		copy(cp, v)
		s.data[k] = cp
	}
}

func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}
