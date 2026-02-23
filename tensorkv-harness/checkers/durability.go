package checkers

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/tensorkv/harness/interfaces"
	"github.com/tensorkv/harness/recorder"
)

// DurabilityCheckConfig controls how the durability checker reconnects after a crash.
type DurabilityCheckConfig struct {
	// CrashedNode is the node that was killed.
	CrashedNode interfaces.NodeID

	// CrashTime is the wall-clock time at which the node was killed.
	// Only Puts with ReturnTime <= CrashTime are expected to be durable.
	CrashTime time.Time

	// ReadFromNodes is the list of nodes to use for post-crash Gets.
	// Typically the surviving nodes (all nodes except CrashedNode).
	ReadFromNodes []interfaces.NodeID

	// Restarted indicates whether CrashedNode was restarted before we check.
	// If true, we also read from CrashedNode.
	Restarted bool
}

// CheckDurability verifies that every key successfully Put before a crash remains
// readable from surviving nodes after the crash.
//
// The checker:
//  1. Identifies all Puts that returned nil (err == nil) and whose ReturnTime
//     is at or before cfg.CrashTime. These writes must be durable.
//  2. For each such key+hash pair, connects to every node in cfg.ReadFromNodes
//     and issues a Get.
//  3. Verifies the returned value hash matches the written hash (or a causally
//     later one — i.e., a hash written after the pre-crash write). Any node
//     that returns an error or a stale/phantom hash fails the check.
func CheckDurability(cluster interfaces.Cluster, preHistory *recorder.Recorder, cfg DurabilityCheckConfig) error {
	history := preHistory.GetHistory()

	// Build the set of durable writes: key → hash of the last durable write
	// (the one with the latest ReturnTime that is still <= CrashTime).
	type durableWrite struct {
		hash       [32]byte
		returnTime time.Time
	}
	durable := make(map[interfaces.Key]durableWrite)

	for _, e := range history {
		if e.Type != recorder.OpPut || e.Err != nil {
			continue
		}
		if e.ReturnTime.After(cfg.CrashTime) {
			continue
		}
		existing, found := durable[e.Key]
		if !found || e.ReturnTime.After(existing.returnTime) {
			durable[e.Key] = durableWrite{hash: e.ValueHash, returnTime: e.ReturnTime}
		}
	}

	if len(durable) == 0 {
		// Nothing durable to check — trivially passes.
		return nil
	}

	// Determine which nodes to read from.
	readNodes := cfg.ReadFromNodes
	if cfg.Restarted {
		readNodes = append(readNodes, cfg.CrashedNode)
	}

	if len(readNodes) == 0 {
		return fmt.Errorf("durability check: no nodes to read from after crash of node %d", cfg.CrashedNode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Build per-key set of all hashes that are causally >= the durable write.
	// A hash H2 is causally >= H1 if H2's Put ReturnTime >= H1's Put ReturnTime.
	type writeTime struct {
		hash       [32]byte
		returnTime time.Time
	}
	allWrites := make(map[interfaces.Key][]writeTime)
	for _, e := range history {
		if e.Type == recorder.OpPut && e.Err == nil {
			allWrites[e.Key] = append(allWrites[e.Key], writeTime{hash: e.ValueHash, returnTime: e.ReturnTime})
		}
	}

	// isAcceptable returns true if readHash is >= the durable write for key.
	isAcceptable := func(key interfaces.Key, readHash [32]byte, durableWrite durableWrite) bool {
		if readHash == durableWrite.hash {
			return true
		}
		// Check if readHash was written after the durable write.
		for _, wt := range allWrites[key] {
			if wt.hash == readHash && !wt.returnTime.Before(durableWrite.returnTime) {
				return true
			}
		}
		return false
	}

	for _, nodeID := range readNodes {
		store, err := cluster.Connect(nodeID)
		if err != nil {
			return fmt.Errorf(
				"durability check: failed to connect to node %d after crash of node %d: %v",
				nodeID, cfg.CrashedNode, err,
			)
		}

		for key, dw := range durable {
			val, err := store.Get(ctx, key)
			if err != nil {
				return fmt.Errorf(
					"durability violation: key %x was successfully Put with hash %x at %v, "+
						"but after node %d crash, Get from node %d returned error: %v",
					key, dw.hash, dw.returnTime, cfg.CrashedNode, nodeID, err,
				)
			}

			// Hash the returned value.
			readHash := hashValueDurability(val)
			if !isAcceptable(key, readHash, dw) {
				return fmt.Errorf(
					"durability violation: key %x was successfully Put with hash %x at %v, "+
						"but after node %d crash, Get from node %d returned unexpected hash %x",
					key, dw.hash, dw.returnTime, cfg.CrashedNode, nodeID, readHash,
				)
			}
		}
	}
	return nil
}

// hashValueDurability computes the SHA-256 of a value for durability checking.
func hashValueDurability(v interfaces.Value) [32]byte {
	return sha256.Sum256(v)
}
