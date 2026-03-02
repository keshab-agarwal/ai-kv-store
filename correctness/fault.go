package correctness

import (
	"fmt"
	"math/rand"
	"net/http"
	"time"
)

// FaultSchedule determines when faults occur during a test run.
type FaultSchedule struct {
	Seed      int64
	Duration  time.Duration
	crashAt   time.Duration // when to crash, relative to test start
	recoverAt time.Duration // when to recover, relative to test start
	nodeID    int           // which node to crash (1-based)
}

// NewFaultSchedule creates a FaultSchedule with randomised crash and recovery
// times derived from the given seed.
//
//   - crashAt:   uniformly drawn from [25%, 75%) of duration
//   - recoverAt: crashAt + uniformly drawn from [5s, 15s)
//   - nodeID:    uniformly drawn from [1, n]
func NewFaultSchedule(seed int64, duration time.Duration, n int) *FaultSchedule {
	rng := rand.New(rand.NewSource(seed))

	// crashAt in [25%, 75%) of total duration.
	lo := float64(duration) * 0.25
	hi := float64(duration) * 0.75
	crashAt := time.Duration(lo + rng.Float64()*(hi-lo))

	// recoverAt: crashAt + [5s, 15s).
	recoverDelay := 5*time.Second + time.Duration(rng.Int63n(int64(10*time.Second)))
	recoverAt := crashAt + recoverDelay

	nodeID := 1 + rng.Intn(n)

	return &FaultSchedule{
		Seed:      seed,
		Duration:  duration,
		crashAt:   crashAt,
		recoverAt: recoverAt,
		nodeID:    nodeID,
	}
}

// NodeID returns the ID of the node that will be crashed.
func (f *FaultSchedule) NodeID() int {
	return f.nodeID
}

// Run executes the fault schedule against the orchestrator.
//
// It:
//  1. Waits until crashAt after startTime, then crashes nodeID.
//  2. Waits until recoverAt after startTime, then recovers nodeID.
//
// Returns (faultEvents, error) where faultEvents counts executed crash events.
func (f *FaultSchedule) Run(o *Orchestrator, startTime time.Time) (int, error) {
	faultEvents := 0

	// --- Crash phase ---
	crashDeadline := startTime.Add(f.crashAt)
	now := time.Now()
	if crashDeadline.After(now) {
		time.Sleep(crashDeadline.Sub(now))
	}

	if err := o.Crash(f.nodeID); err != nil {
		return faultEvents, fmt.Errorf("crash node %d: %w", f.nodeID, err)
	}
	faultEvents++

	// --- Recovery phase ---
	recoverDeadline := startTime.Add(f.recoverAt)
	now = time.Now()
	if recoverDeadline.After(now) {
		time.Sleep(recoverDeadline.Sub(now))
	}

	if err := o.Recover(f.nodeID); err != nil {
		// Log but do not count as additional event; crash already counted.
		return faultEvents, fmt.Errorf("recover node %d: %w", f.nodeID, err)
	}

	return faultEvents, nil
}

// VerifyReReplication polls the recovered node's /api/status until its
// commit_index matches (or exceeds) the highest commit_index seen across all
// other live nodes, or until timeout.
//
// This confirms that the re-joined node has caught up with the cluster.
func VerifyReReplication(o *Orchestrator, nodeID int, timeout time.Duration) error {
	client := &http.Client{Timeout: 500 * time.Millisecond}
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		// Determine the highest commit index among all other nodes.
		var maxCommit uint64
		for _, addr := range o.ClientAddresses() {
			st, err := getNodeStatus(client, addr)
			if err != nil {
				continue
			}
			if st.CommitIndex > maxCommit {
				maxCommit = st.CommitIndex
			}
		}

		// Check the recovered node's commit index.
		recoveredAddr, err := o.NodeClientAddr(nodeID)
		if err != nil {
			return fmt.Errorf("node addr: %w", err)
		}
		st, err := getNodeStatus(client, recoveredAddr)
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}

		if st.CommitIndex >= maxCommit {
			return nil
		}

		time.Sleep(200 * time.Millisecond)
	}

	return fmt.Errorf("node %d did not catch up within %s", nodeID, timeout)
}
