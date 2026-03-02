// Package ycsb defines the DB interface used by the YCSB workload engine.
// Any storage backend must implement this interface to be benchmarked.
package ycsb

import "ai-kv-store/kvstore"

// DB is the storage interface exposed to the YCSB workload engine.
// Keys are hex-encoded 32-character strings representing 128-bit opaque keys.
// Values are raw byte slices.
type DB interface {
	// Read retrieves the value for the given key.
	Read(key string) (kvstore.Status, []byte, error)

	// Insert stores a new key-value pair (treated as Put).
	Insert(key string, value []byte) (kvstore.Status, error)

	// Update overwrites an existing key (also treated as Put).
	Update(key string, value []byte) (kvstore.Status, error)

	// Delete removes the key; deleting a missing key returns StatusOK.
	Delete(key string) (kvstore.Status, error)

	// Scan is not supported by this store. Must be implemented but
	// scanproportion=0 ensures it is never called during a benchmark run.
	// Implementations should return StatusOK with empty results.
	Scan(startKey string, count int) (kvstore.Status, error)

	// Close releases all resources (connection pools, etc.).
	Close() error
}
