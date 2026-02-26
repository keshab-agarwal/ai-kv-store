package store

import (
	"kv-store/interfaces"
	"kv-store/internal/version"
	"sync"
)

type Versioned struct {
	Value interfaces.Value
	VV    version.Vector
}

type CausalStore struct {
	mu      sync.RWMutex
	entries map[interfaces.Key][]Versioned
	nodeID  interfaces.NodeID
}

func NewCausalStore(nodeID interfaces.NodeID) *CausalStore {
	return &CausalStore{
		entries: make(map[interfaces.Key][]Versioned),
		nodeID:  nodeID,
	}
}

func (s *CausalStore) Put(key interfaces.Key, value interfaces.Value, clientVV version.Vector) version.Vector {
	s.mu.Lock()
	defer s.mu.Unlock()
	local := s.maxVVLocked(key)
	merged := clientVV.Merge(local)
	newVV := merged.Inc(s.nodeID)
	v := make(interfaces.Value, len(value))
	copy(v, value)
	s.addVersionLocked(key, Versioned{Value: v, VV: newVV})
	return newVV
}

func (s *CausalStore) addVersionLocked(key interfaces.Key, v Versioned) {
	list := s.entries[key]
	var kept []Versioned
	for _, w := range list {
		if v.VV.Dominates(w.VV) {
			continue
		}
		kept = append(kept, w)
	}
	s.entries[key] = append(kept, v)
}

func (s *CausalStore) maxVVLocked(key interfaces.Key) version.Vector {
	var m version.Vector
	for _, v := range s.entries[key] {
		m = m.Merge(v.VV)
	}
	return m
}

func (s *CausalStore) Get(key interfaces.Key, clientVV version.Vector) (interfaces.Value, version.Vector, bool) {
	s.mu.RLock()
	list := s.entries[key]
	if len(list) == 0 {
		s.mu.RUnlock()
		return nil, version.Vector{}, false
	}
	visible := make([]Versioned, 0, len(list))
	zero := version.Vector{}
	for _, v := range list {
		if clientVV == zero || v.VV.LEQ(clientVV) {
			visible = append(visible, v)
		}
	}
	s.mu.RUnlock()
	if len(visible) == 0 {
		return nil, version.Vector{}, false
	}
	best := visible[0]
	for i := 1; i < len(visible); i++ {
		if visible[i].VV.Dominates(best.VV) {
			best = visible[i]
		}
	}
	out := make(interfaces.Value, len(best.Value))
	copy(out, best.Value)
	return out, best.VV, true
}

func (s *CausalStore) ApplyReplicated(key interfaces.Key, value interfaces.Value, vv version.Vector) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := make(interfaces.Value, len(value))
	copy(v, value)
	s.addVersionLocked(key, Versioned{Value: v, VV: vv})
}

func (s *CausalStore) GetReplicated(key interfaces.Key, vv version.Vector) (interfaces.Value, version.Vector, bool) {
	return s.Get(key, vv)
}

func KeyOwner(key interfaces.Key, numNodes int) interfaces.NodeID {
	var h uint32
	for _, b := range key {
		h = h*31 + uint32(b)
	}
	return interfaces.NodeID(h % uint32(numNodes))
}
