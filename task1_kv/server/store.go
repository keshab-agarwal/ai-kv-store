package server

import (
	"sync"
	"sync/atomic"

	"ai-kv-store/shared/types"
)

const numShards = 256

// shard is a single partition of the key space.
type shard struct {
	mu      sync.RWMutex
	data    map[types.Key][]byte
	access  map[types.Key]uint64 // simple access counter for hot-key tracking
}

// ShardedMap is a concurrent sharded in-memory map.
type ShardedMap struct {
	shards [numShards]shard
	size   atomic.Int64
}

// NewShardedMap creates a new sharded map.
func NewShardedMap() *ShardedMap {
	sm := &ShardedMap{}
	for i := range sm.shards {
		sm.shards[i].data = make(map[types.Key][]byte)
		sm.shards[i].access = make(map[types.Key]uint64)
	}
	return sm
}

func shardIndex(k types.Key) uint8 {
	return k[0]
}

// Get returns the value for a key and whether it exists.
func (sm *ShardedMap) Get(key types.Key) ([]byte, bool) {
	idx := shardIndex(key)
	s := &sm.shards[idx]
	s.mu.RLock()
	val, ok := s.data[key]
	if ok {
		s.access[key]++
	}
	s.mu.RUnlock()
	if ok {
		// Return a copy to prevent mutation
		cp := make([]byte, len(val))
		copy(cp, val)
		return cp, true
	}
	return nil, false
}

// Put stores a key-value pair. Returns true if it was a new key.
func (sm *ShardedMap) Put(key types.Key, value []byte) bool {
	idx := shardIndex(key)
	s := &sm.shards[idx]
	cp := make([]byte, len(value))
	copy(cp, value)
	s.mu.Lock()
	_, existed := s.data[key]
	s.data[key] = cp
	s.access[key]++
	s.mu.Unlock()
	if !existed {
		sm.size.Add(1)
	}
	return !existed
}

// Delete removes a key. Returns true if it existed.
func (sm *ShardedMap) Delete(key types.Key) bool {
	idx := shardIndex(key)
	s := &sm.shards[idx]
	s.mu.Lock()
	_, existed := s.data[key]
	if existed {
		delete(s.data, key)
		delete(s.access, key)
	}
	s.mu.Unlock()
	if existed {
		sm.size.Add(-1)
	}
	return existed
}

// Snapshot returns a copy of all data.
func (sm *ShardedMap) Snapshot() map[types.Key][]byte {
	result := make(map[types.Key][]byte)
	for i := range sm.shards {
		s := &sm.shards[i]
		s.mu.RLock()
		for k, v := range s.data {
			cp := make([]byte, len(v))
			copy(cp, v)
			result[k] = cp
		}
		s.mu.RUnlock()
	}
	return result
}

// LoadSnapshot replaces all data with the provided snapshot.
func (sm *ShardedMap) LoadSnapshot(data map[types.Key][]byte) {
	// Clear all shards first
	for i := range sm.shards {
		s := &sm.shards[i]
		s.mu.Lock()
		s.data = make(map[types.Key][]byte)
		s.access = make(map[types.Key]uint64)
		s.mu.Unlock()
	}
	var count int64
	for k, v := range data {
		idx := shardIndex(k)
		s := &sm.shards[idx]
		cp := make([]byte, len(v))
		copy(cp, v)
		s.mu.Lock()
		s.data[k] = cp
		s.mu.Unlock()
		count++
	}
	sm.size.Store(count)
}

// Size returns the total number of keys.
func (sm *ShardedMap) Size() int64 {
	return sm.size.Load()
}
