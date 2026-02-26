// Package interfaces defines the typed contracts that a distributed KV store
// implementation must satisfy. These interfaces are intentionally algorithm-agnostic:
// they describe WHAT must hold, not HOW to achieve it.
package interfaces

import (
	"context"
	"errors"
)

// Key is a 128-bit content-addressed identifier.
type Key [16]byte

// NodeID identifies a single node in a cluster.
type NodeID uint32

// Value is the payload stored under a Key.
// Valid values must be between MinValueSize and MaxValueSize bytes inclusive.
type Value []byte

const (
	// MinValueSize is the minimum allowed value size (1 byte).
	MinValueSize = 1

	// MaxValueSize is the maximum allowed value size (1 MB).
	MaxValueSize = 1 << 20
)

var (
	// ErrKeyNotFound is returned by Get when the key has never been written.
	ErrKeyNotFound = errors.New("key not found")

	// ErrValueTooSmall is returned when the value is smaller than MinValueSize.
	ErrValueTooSmall = errors.New("value smaller than 1 byte")

	// ErrValueTooLarge is returned when the value is larger than MaxValueSize.
	ErrValueTooLarge = errors.New("value larger than 1MB")

	// ErrNodeDown is returned when the target node is unavailable.
	ErrNodeDown = errors.New("node unavailable")
)

// Store is the core KV interface. Implementations must satisfy:
//
// Put postconditions:
//   - After Put returns nil, the value is durable against single node failure.
//   - A subsequent Get from ANY node returns this value or a causally later one.
//
// Get postconditions:
//   - Returns a value that was actually Put for this key (no phantom reads).
//   - Monotonic: if this client previously read version V for key K,
//     the returned version is >= V in causal order (no going back in time).
type Store interface {
	// Put stores the given value under the given key.
	// Returns ErrValueTooSmall or ErrValueTooLarge if the value is out of range.
	// Returns nil on success, at which point the write is durable.
	Put(ctx context.Context, key Key, value Value) error

	// Get retrieves the current value for the given key.
	// Returns ErrKeyNotFound if the key has never been successfully written.
	Get(ctx context.Context, key Key) (Value, error)
}

// Cluster manages the lifecycle of a distributed KV store and provides
// fault injection capabilities for testing.
type Cluster interface {
	// Start launches a cluster with the given number of nodes.
	// Must be called before any other method.
	Start(nodes int) error

	// Connect returns a Store client pinned to a specific node.
	// The returned Store routes all operations through that node.
	Connect(node NodeID) (Store, error)

	// NodeIDs returns all node IDs currently in the cluster.
	NodeIDs() []NodeID

	// KillNode ungracefully terminates the specified node.
	// Subsequent operations to this node return ErrNodeDown.
	KillNode(node NodeID) error

	// RestartNode brings a previously killed node back online.
	// The node may replay a write-ahead log or recover from snapshots.
	RestartNode(node NodeID) error

	// PartitionNodes induces a bidirectional network partition between nodes a and b.
	// Messages between them are dropped; other nodes are unaffected.
	PartitionNodes(a, b NodeID) error

	// HealPartition removes the partition between nodes a and b.
	HealPartition(a, b NodeID) error

	// Shutdown tears down the entire cluster, releasing all resources.
	Shutdown() error
}
