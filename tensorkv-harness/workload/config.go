// Package workload provides a YCSB-style workload generator for driving
// concurrent clients against a distributed KV store cluster.
package workload

import (
	"time"

	"github.com/tensorkv/harness/interfaces"
)

// WorkloadConfig parameterises a workload run.
type WorkloadConfig struct {
	// NumClients is the number of concurrent client goroutines.
	NumClients int

	// NumKeys is the key-space size. Keys are deterministically derived by
	// hashing integers 0..NumKeys-1 to 128-bit Keys.
	NumKeys int

	// ReadRatio is the fraction of operations that are Gets (0.0–1.0).
	// E.g. 0.9 means 90% reads, 10% writes.
	ReadRatio float64

	// KeyDistribution selects the key-access distribution.
	// Valid values: "zipfian", "uniform".
	KeyDistribution string

	// ZipfianConstant is the theta parameter for the Zipfian distribution.
	// Typical value: 0.99 (high skew). Only used when KeyDistribution == "zipfian".
	ZipfianConstant float64

	// ValueSize is the fixed size of generated values in bytes.
	// Must be within [interfaces.MinValueSize, interfaces.MaxValueSize].
	ValueSize int

	// Duration is the total wall-clock time to run the workload.
	Duration time.Duration

	// RampUp is the warm-up period at the start of Duration.
	// Operations executed during RampUp are not included in the returned history.
	RampUp time.Duration

	// TargetNode pins all clients to a single node when non-nil.
	// When nil, clients are assigned to nodes in round-robin order.
	TargetNode *interfaces.NodeID
}
