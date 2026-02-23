// Package mock provides a trivially correct, single-machine in-memory
// implementation of interfaces.Store and interfaces.Cluster. It is used
// exclusively for testing the evaluation harness — it is NOT a distributed
// system and makes no durability or replication guarantees.
package mock

import (
	"context"
	"fmt"
	"sync"

	"github.com/tensorkv/harness/interfaces"
)

// node tracks liveness for a single simulated node.
// All nodes in a Cluster share a common data map; the node struct only
// controls whether operations through this node are permitted (down flag).
type node struct {
	down bool // protected by Cluster.mu
}

func newNode() *node { return &node{} }

// nodeStore routes operations to the cluster's shared data map while
// respecting its node's liveness state.
type nodeStore struct {
	cluster *Cluster
	node    *node
}

func (s *nodeStore) Put(ctx context.Context, key interfaces.Key, value interfaces.Value) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(value) < interfaces.MinValueSize {
		return interfaces.ErrValueTooSmall
	}
	if len(value) > interfaces.MaxValueSize {
		return interfaces.ErrValueTooLarge
	}
	s.cluster.mu.Lock()
	defer s.cluster.mu.Unlock()
	if s.node.down {
		return interfaces.ErrNodeDown
	}
	// Copy value to avoid aliasing with the caller's pool buffer.
	v := make(interfaces.Value, len(value))
	copy(v, value)
	s.cluster.data[key] = v
	return nil
}

func (s *nodeStore) Get(ctx context.Context, key interfaces.Key) (interfaces.Value, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.cluster.mu.RLock()
	defer s.cluster.mu.RUnlock()
	if s.node.down {
		return nil, interfaces.ErrNodeDown
	}
	v, ok := s.cluster.data[key]
	if !ok {
		return nil, interfaces.ErrKeyNotFound
	}
	return v, nil
}

// ---- partition table --------------------------------------------------------

// partitionKey represents an unordered pair of nodes.
type partitionKey struct {
	a, b interfaces.NodeID
}

func makePartitionKey(a, b interfaces.NodeID) partitionKey {
	if a > b {
		a, b = b, a
	}
	return partitionKey{a, b}
}

// ---- Cluster ----------------------------------------------------------------

// Cluster is a mock interfaces.Cluster backed by a single shared in-memory
// data store. All nodes read from and write to the same map, so the mock
// satisfies phantom-read, monotonic-read, and causal-consistency invariants
// without implementing any replication protocol.
//
// KillNode/RestartNode control per-node liveness (down flag). The shared data
// is never cleared on restart, so durability trivially holds for the mock.
// Network partitions are recorded but have no effect on data visibility (there
// is no inter-node network in the mock).
type Cluster struct {
	mu         sync.RWMutex
	data       map[interfaces.Key]interfaces.Value // shared across all nodes
	nodes      map[interfaces.NodeID]*node
	partitions map[partitionKey]bool
	nodeIDs    []interfaces.NodeID
}

// NewCluster creates a new mock Cluster. Call Start() to initialise nodes.
func NewCluster() *Cluster {
	return &Cluster{
		data:       make(map[interfaces.Key]interfaces.Value),
		nodes:      make(map[interfaces.NodeID]*node),
		partitions: make(map[partitionKey]bool),
	}
}

// Start launches n in-memory nodes with IDs 0..n-1.
func (c *Cluster) Start(n int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n <= 0 {
		return fmt.Errorf("mock: node count must be > 0, got %d", n)
	}
	c.data = make(map[interfaces.Key]interfaces.Value)
	c.nodes = make(map[interfaces.NodeID]*node, n)
	c.nodeIDs = make([]interfaces.NodeID, n)
	for i := 0; i < n; i++ {
		id := interfaces.NodeID(i)
		c.nodes[id] = newNode()
		c.nodeIDs[i] = id
	}
	return nil
}

// Connect returns a Store client for the given node.
func (c *Cluster) Connect(nodeID interfaces.NodeID) (interfaces.Store, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	n, ok := c.nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("mock: node %d does not exist", nodeID)
	}
	return &nodeStore{cluster: c, node: n}, nil
}

// NodeIDs returns the IDs of all nodes in the cluster.
func (c *Cluster) NodeIDs() []interfaces.NodeID {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ids := make([]interfaces.NodeID, len(c.nodeIDs))
	copy(ids, c.nodeIDs)
	return ids
}

// KillNode marks a node as down. Subsequent operations through that node
// return ErrNodeDown. Shared data is unaffected.
func (c *Cluster) KillNode(nodeID interfaces.NodeID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.nodes[nodeID]
	if !ok {
		return fmt.Errorf("mock: node %d does not exist", nodeID)
	}
	n.down = true
	return nil
}

// RestartNode brings a killed node back online. Because the mock uses a shared
// data map, the node immediately sees all previously written keys — durability
// is trivially satisfied without any replication.
func (c *Cluster) RestartNode(nodeID interfaces.NodeID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.nodes[nodeID]
	if !ok {
		return fmt.Errorf("mock: node %d does not exist", nodeID)
	}
	n.down = false
	return nil
}

// PartitionNodes records the partition between (a, b). In the mock this has no
// effect on data visibility because there is no inter-node network to partition.
func (c *Cluster) PartitionNodes(a, b interfaces.NodeID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.partitions[makePartitionKey(a, b)] = true
	return nil
}

// HealPartition removes the recorded partition between a and b.
func (c *Cluster) HealPartition(a, b interfaces.NodeID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.partitions, makePartitionKey(a, b))
	return nil
}

// Shutdown marks all nodes as down and clears the shared data map.
func (c *Cluster) Shutdown() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, n := range c.nodes {
		n.down = true
	}
	c.data = nil
	return nil
}
