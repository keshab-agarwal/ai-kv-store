// Package faultinjector provides a time-based fault injection scheduler that
// runs alongside a workload, injecting network and node failures at pre-defined
// offsets relative to workload start.
package faultinjector

import (
	"fmt"
	"sync"
	"time"

	"github.com/tensorkv/harness/interfaces"
	"github.com/tensorkv/harness/recorder"
	"github.com/tensorkv/harness/workload"
)

// FaultAction describes the type of fault to inject.
type FaultAction int

const (
	// FaultKillNode ungracefully terminates a node.
	FaultKillNode FaultAction = iota
	// FaultRestartNode brings a previously killed node back online.
	FaultRestartNode
	// FaultPartition induces a bidirectional network partition between two nodes.
	FaultPartition
	// FaultHealPartition removes an existing network partition.
	FaultHealPartition
)

// FaultEvent describes a single fault to inject at a specific time offset.
type FaultEvent struct {
	// At is the time offset from workload start at which to inject the fault.
	At time.Duration

	// Action is the type of fault to inject.
	Action FaultAction

	// Target is the primary node to affect.
	Target interfaces.NodeID

	// Target2 is the second node for partition-related faults. Ignored for
	// KillNode and RestartNode.
	Target2 *interfaces.NodeID
}

// RunWithFaults starts the workload generator and, in parallel, fires the
// scheduled fault events against the cluster at their specified offsets.
//
// The returned history contains all operations that occurred during the
// workload, including those that happened during active faults.
func RunWithFaults(
	cluster interfaces.Cluster,
	wCfg workload.WorkloadConfig,
	faults []FaultEvent,
) ([]recorder.HistoryEvent, workload.PerfResult, error) {
	var (
		history []recorder.HistoryEvent
		perf    workload.PerfResult
		wlErr   error
	)

	var wg sync.WaitGroup

	// Run the workload in its own goroutine.
	wg.Add(1)
	start := time.Now()
	go func() {
		defer wg.Done()
		history, perf, wlErr = workload.RunWorkload(cluster, wCfg)
	}()

	// Fire each fault at its scheduled offset.
	for _, fe := range faults {
		fe := fe // capture loop variable
		wg.Add(1)
		go func() {
			defer wg.Done()
			fireAt := start.Add(fe.At)
			now := time.Now()
			if fireAt.After(now) {
				time.Sleep(fireAt.Sub(now))
			}
			if err := executeFault(cluster, fe); err != nil {
				// Log but do not abort the workload; fault errors are expected
				// (e.g. trying to kill an already-dead node).
				fmt.Printf("fault injection warning at offset %v: %v\n", fe.At, err)
			}
		}()
	}

	wg.Wait()
	return history, perf, wlErr
}

// executeFault dispatches a single FaultEvent to the cluster.
func executeFault(cluster interfaces.Cluster, fe FaultEvent) error {
	switch fe.Action {
	case FaultKillNode:
		return cluster.KillNode(fe.Target)
	case FaultRestartNode:
		return cluster.RestartNode(fe.Target)
	case FaultPartition:
		if fe.Target2 == nil {
			return fmt.Errorf("FaultPartition requires Target2 to be set")
		}
		return cluster.PartitionNodes(fe.Target, *fe.Target2)
	case FaultHealPartition:
		if fe.Target2 == nil {
			return fmt.Errorf("FaultHealPartition requires Target2 to be set")
		}
		return cluster.HealPartition(fe.Target, *fe.Target2)
	default:
		return fmt.Errorf("unknown fault action: %d", fe.Action)
	}
}

// ---- Preset fault schedules -------------------------------------------------

// HappyPath returns an empty fault schedule: no faults, just concurrent load.
func HappyPath() []FaultEvent {
	return nil
}

// CrashDuringLoad returns a schedule that kills node 1 at 30% of d, then
// restarts it at 60%. This tests write durability and read availability during
// a single-node failure.
func CrashDuringLoad(d time.Duration) []FaultEvent {
	node1 := interfaces.NodeID(1)
	return []FaultEvent{
		{At: time.Duration(float64(d) * 0.30), Action: FaultKillNode, Target: node1},
		{At: time.Duration(float64(d) * 0.60), Action: FaultRestartNode, Target: node1},
	}
}

// NetworkPartition returns a schedule that partitions node 0 from node 1 at
// 30% of d, then heals at 60%. This tests consistency under split-brain.
func NetworkPartition(d time.Duration) []FaultEvent {
	node0 := interfaces.NodeID(0)
	node1 := interfaces.NodeID(1)
	return []FaultEvent{
		{At: time.Duration(float64(d) * 0.30), Action: FaultPartition, Target: node0, Target2: &node1},
		{At: time.Duration(float64(d) * 0.60), Action: FaultHealPartition, Target: node0, Target2: &node1},
	}
}
