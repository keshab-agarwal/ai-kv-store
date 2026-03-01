package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"ai-kv-store/shared/types"
)

// VoteRequest is sent by candidates to peers.
type VoteRequest struct {
	Epoch       uint64 `json:"epoch"`
	CandidateID int    `json:"candidate_id"`
	LastApplied uint64 `json:"last_applied"`
}

// VoteResponse is the peer's reply to a vote request.
type VoteResponse struct {
	Epoch   uint64 `json:"epoch"`
	Granted bool   `json:"granted"`
}

// HeartbeatRequest is sent by the coordinator to followers.
type HeartbeatRequest struct {
	Epoch         uint64 `json:"epoch"`
	CoordinatorID int    `json:"coordinator_id"`
	CommitIndex   uint64 `json:"commit_index"`
}

// HeartbeatResponse is the follower's reply.
type HeartbeatResponse struct {
	Epoch        uint64 `json:"epoch"`
	NodeID       int    `json:"node_id"`
	AppliedIndex uint64 `json:"applied_index"`
}

// SyncRequest asks the coordinator for missing entries.
type SyncRequest struct {
	FromIndex uint64 `json:"from_index"`
}

// SnapshotEntry is used for JSON serialization of snapshot data.
type SnapshotEntry struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

// SyncResponse contains missing entries or a full snapshot.
type SyncResponse struct {
	Entries  []types.LogEntry `json:"entries,omitempty"`
	Snapshot []SnapshotEntry  `json:"snapshot,omitempty"`
}

// Coordinator manages election state, heartbeats, and lease tracking.
type Coordinator struct {
	mu sync.RWMutex

	nodeID int
	peers  []string
	config types.ClusterConfig

	role         types.NodeRole
	epoch        uint64
	votedForEpoch uint64 // epoch in which we last voted
	votedFor     int     // candidate we voted for

	// Lease tracking
	lastHeartbeatRound time.Time
	leaseValid         atomic.Bool

	// Election timer
	electionTimer  *time.Timer
	electionResetC chan struct{}

	// Heartbeat ticker for coordinator (protected by hbMu, NOT c.mu)
	hbMu          sync.Mutex
	heartbeatDone chan struct{}

	// Applied index tracking
	appliedIndex atomic.Uint64
	commitIndex  atomic.Uint64

	// Coordinator ID (who is the current known coordinator)
	coordinatorID atomic.Int32

	// HTTP client for RPCs
	httpClient *http.Client

	// Callbacks
	onBecomeCoordinator func()
	onBecomeFollower    func(epoch uint64)
	onHeartbeatResp     func(peerID int, appliedIndex uint64)

	// Logger
	logFunc func(msg string, fields map[string]interface{})

	// Shutdown
	stopC  chan struct{}
	stoppedC chan struct{}
}

// NewCoordinator creates a new coordinator state machine.
func NewCoordinator(nodeID int, peers []string, config types.ClusterConfig, logFunc func(string, map[string]interface{})) *Coordinator {
	c := &Coordinator{
		nodeID:         nodeID,
		peers:          peers,
		config:         config,
		role:           types.RoleFollower,
		epoch:          0,
		votedFor:       -1,
		electionResetC: make(chan struct{}, 1),
		stopC:          make(chan struct{}),
		stoppedC:       make(chan struct{}),
		logFunc:        logFunc,
		httpClient: &http.Client{
			Timeout: time.Duration(config.RPCTimeoutMs) * time.Millisecond,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 4,
				MaxIdleConns:        20,
				IdleConnTimeout:     60 * time.Second,
			},
		},
	}
	c.coordinatorID.Store(-1)
	return c
}

// SetCallbacks sets the coordinator callbacks.
func (c *Coordinator) SetCallbacks(
	onBecomeCoordinator func(),
	onBecomeFollower func(epoch uint64),
	onHeartbeatResp func(peerID int, appliedIndex uint64),
) {
	c.onBecomeCoordinator = onBecomeCoordinator
	c.onBecomeFollower = onBecomeFollower
	c.onHeartbeatResp = onHeartbeatResp
}

// Start begins the election timer and related goroutines.
func (c *Coordinator) Start() {
	go c.electionLoop()
}

// Stop shuts down the coordinator.
func (c *Coordinator) Stop() {
	close(c.stopC)
	<-c.stoppedC
}

// Role returns the current role.
func (c *Coordinator) Role() types.NodeRole {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.role
}

// Epoch returns the current epoch.
func (c *Coordinator) Epoch() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.epoch
}

// IsLeaseValid returns whether the coordinator lease is currently valid.
func (c *Coordinator) IsLeaseValid() bool {
	return c.leaseValid.Load()
}

// CoordinatorID returns the known coordinator's node ID.
func (c *Coordinator) CoordinatorID() int {
	return int(c.coordinatorID.Load())
}

// SetAppliedIndex updates the applied index.
func (c *Coordinator) SetAppliedIndex(idx uint64) {
	c.appliedIndex.Store(idx)
}

// AppliedIndex returns the applied index.
func (c *Coordinator) AppliedIndex() uint64 {
	return c.appliedIndex.Load()
}

// SetCommitIndex updates the commit index.
func (c *Coordinator) SetCommitIndex(idx uint64) {
	c.commitIndex.Store(idx)
}

// CommitIndex returns the commit index.
func (c *Coordinator) CommitIndex() uint64 {
	return c.commitIndex.Load()
}

func (c *Coordinator) randomElectionTimeout() time.Duration {
	min := c.config.ElectionMinMs
	max := c.config.ElectionMaxMs
	ms := min + rand.Intn(max-min+1)
	return time.Duration(ms) * time.Millisecond
}

// ResetElectionTimer resets the election timer (called on heartbeat received).
func (c *Coordinator) ResetElectionTimer() {
	select {
	case c.electionResetC <- struct{}{}:
	default:
	}
}

func (c *Coordinator) electionLoop() {
	defer close(c.stoppedC)
	timer := time.NewTimer(c.randomElectionTimeout())
	defer timer.Stop()

	for {
		select {
		case <-c.stopC:
			return
		case <-c.electionResetC:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(c.randomElectionTimeout())
		case <-timer.C:
			c.mu.RLock()
			role := c.role
			c.mu.RUnlock()

			if role != types.RoleCoordinator {
				c.startElection()
			}
			timer.Reset(c.randomElectionTimeout())
		}
	}
}

func (c *Coordinator) startElection() {
	c.mu.Lock()
	newEpoch := c.epoch + 1
	c.epoch = newEpoch
	c.role = types.RoleCandidate
	c.votedForEpoch = newEpoch
	c.votedFor = c.nodeID
	lastApplied := c.appliedIndex.Load()
	c.mu.Unlock()

	c.logFunc("starting election", map[string]interface{}{
		"epoch":        newEpoch,
		"last_applied": lastApplied,
	})

	totalNodes := len(c.peers)
	majority := totalNodes/2 + 1
	votes := 1 // vote for self
	maxPeerEpoch := newEpoch

	var mu sync.Mutex
	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.config.RPCTimeoutMs)*time.Millisecond)
	defer cancel()

	for i, peer := range c.peers {
		if i == c.nodeID {
			continue
		}
		wg.Add(1)
		go func(peerID int, addr string) {
			defer wg.Done()
			granted, respEpoch, err := c.requestVote(ctx, addr, newEpoch, lastApplied)
			if err != nil {
				return
			}
			mu.Lock()
			if granted {
				votes++
			}
			// Track the highest epoch we see from any peer
			if respEpoch > maxPeerEpoch {
				maxPeerEpoch = respEpoch
			}
			mu.Unlock()
		}(i, peer)
	}

	wg.Wait()

	c.mu.Lock()

	// If we saw a higher epoch from a peer, adopt it and step down
	mu.Lock()
	peerEpoch := maxPeerEpoch
	won := votes >= majority
	mu.Unlock()

	if peerEpoch > c.epoch {
		c.epoch = peerEpoch
		c.role = types.RoleFollower
		c.votedFor = -1
		c.mu.Unlock()
		return
	}

	// Check we're still candidate for this epoch
	if c.role != types.RoleCandidate || c.epoch != newEpoch {
		c.mu.Unlock()
		return
	}

	if won {
		c.role = types.RoleCoordinator
		c.coordinatorID.Store(int32(c.nodeID))
		c.lastHeartbeatRound = time.Now()
		c.leaseValid.Store(true)
		c.logFunc("became coordinator", map[string]interface{}{
			"epoch": newEpoch,
			"votes": votes,
		})
		// Release lock before callback to avoid deadlock (callback may acquire c.mu)
		c.mu.Unlock()
		if c.onBecomeCoordinator != nil {
			c.onBecomeCoordinator()
		}
	} else {
		c.role = types.RoleFollower
		c.votedFor = -1
		c.mu.Unlock()
	}
}

func (c *Coordinator) requestVote(ctx context.Context, addr string, epoch uint64, lastApplied uint64) (bool, uint64, error) {
	req := VoteRequest{
		Epoch:       epoch,
		CandidateID: c.nodeID,
		LastApplied: lastApplied,
	}
	body, err := json.Marshal(req)
	if err != nil {
		return false, 0, err
	}

	url := fmt.Sprintf("http://%s/internal/vote", addr)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return false, 0, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return false, 0, err
	}
	defer resp.Body.Close()

	var voteResp VoteResponse
	if err := json.NewDecoder(resp.Body).Decode(&voteResp); err != nil {
		return false, 0, err
	}

	return voteResp.Granted, voteResp.Epoch, nil
}

// HandleVoteRequest processes an incoming vote request from a candidate.
func (c *Coordinator) HandleVoteRequest(req VoteRequest) VoteResponse {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Reject if not a newer epoch
	if req.Epoch <= c.epoch {
		return VoteResponse{Epoch: c.epoch, Granted: false}
	}

	// Reject if candidate is behind us in applied entries
	myApplied := c.appliedIndex.Load()
	if req.LastApplied < myApplied {
		// Do NOT update our epoch here -- rejecting the vote means we don't
		// acknowledge this epoch. The candidate will fail to get a majority.
		return VoteResponse{Epoch: c.epoch, Granted: false}
	}

	// Check if we already voted in this epoch for a different candidate
	if c.votedForEpoch == req.Epoch && c.votedFor != req.CandidateID {
		return VoteResponse{Epoch: c.epoch, Granted: false}
	}

	// Grant vote: update epoch, step down if needed
	c.epoch = req.Epoch
	c.role = types.RoleFollower
	c.votedForEpoch = req.Epoch
	c.votedFor = req.CandidateID
	c.leaseValid.Store(false)

	c.logFunc("granted vote", map[string]interface{}{
		"epoch":     req.Epoch,
		"candidate": req.CandidateID,
	})

	return VoteResponse{Epoch: c.epoch, Granted: true}
}

// HandleHeartbeat processes an incoming heartbeat from the coordinator.
func (c *Coordinator) HandleHeartbeat(req HeartbeatRequest) HeartbeatResponse {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Accept heartbeats from the current or higher epoch.
	// Also accept from a coordinator whose epoch equals ours -- this is the
	// normal steady-state case.
	// Reject only if the heartbeat epoch is strictly less than our epoch AND
	// we are the coordinator at our epoch (meaning we legitimately own a higher epoch).
	if req.Epoch < c.epoch && c.role == types.RoleCoordinator {
		return HeartbeatResponse{
			Epoch:        c.epoch,
			NodeID:       c.nodeID,
			AppliedIndex: c.appliedIndex.Load(),
		}
	}

	// Accept the heartbeat: adopt the coordinator's epoch if higher or equal
	if req.Epoch >= c.epoch {
		c.epoch = req.Epoch
	}

	if c.role != types.RoleFollower {
		c.role = types.RoleFollower
		c.leaseValid.Store(false)
	}

	c.votedFor = -1
	c.coordinatorID.Store(int32(req.CoordinatorID))

	// Update commit index if the coordinator's is higher
	if req.CommitIndex > c.commitIndex.Load() {
		c.commitIndex.Store(req.CommitIndex)
	}

	// Reset election timer
	c.ResetElectionTimer()

	return HeartbeatResponse{
		Epoch:        c.epoch,
		NodeID:       c.nodeID,
		AppliedIndex: c.appliedIndex.Load(),
	}
}

// StartHeartbeatLoop begins sending periodic heartbeats (coordinator only).
// Must NOT be called while holding c.mu.
func (c *Coordinator) StartHeartbeatLoop() {
	c.hbMu.Lock()
	if c.heartbeatDone != nil {
		c.hbMu.Unlock()
		return
	}
	c.heartbeatDone = make(chan struct{})
	c.hbMu.Unlock()

	go c.heartbeatLoop()
}

// StopHeartbeatLoop stops the heartbeat sender.
// Must NOT be called while holding c.mu.
func (c *Coordinator) StopHeartbeatLoop() {
	c.hbMu.Lock()
	done := c.heartbeatDone
	c.heartbeatDone = nil
	c.hbMu.Unlock()
	if done != nil {
		close(done)
	}
}

func (c *Coordinator) heartbeatLoop() {
	c.hbMu.Lock()
	done := c.heartbeatDone
	c.hbMu.Unlock()

	ticker := time.NewTicker(time.Duration(c.config.HeartbeatMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopC:
			return
		case <-done:
			return
		case <-ticker.C:
			c.sendHeartbeats()
		}
	}
}

func (c *Coordinator) sendHeartbeats() {
	c.mu.RLock()
	if c.role != types.RoleCoordinator {
		c.mu.RUnlock()
		return
	}
	epoch := c.epoch
	commitIdx := c.commitIndex.Load()
	c.mu.RUnlock()

	totalNodes := len(c.peers)
	majority := totalNodes/2 + 1
	acks := 1 // self
	var mu sync.Mutex
	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.config.RPCTimeoutMs)*time.Millisecond)
	defer cancel()

	for i, peer := range c.peers {
		if i == c.nodeID {
			continue
		}
		wg.Add(1)
		go func(peerID int, addr string) {
			defer wg.Done()
			resp, err := c.sendHeartbeat(ctx, addr, epoch, commitIdx)
			if err != nil {
				return
			}
			if resp.Epoch > epoch {
				c.mu.Lock()
				if resp.Epoch > c.epoch {
					c.epoch = resp.Epoch
					c.role = types.RoleFollower
					c.leaseValid.Store(false)
					if c.onBecomeFollower != nil {
						c.onBecomeFollower(resp.Epoch)
					}
				}
				c.mu.Unlock()
				return
			}
			mu.Lock()
			acks++
			mu.Unlock()
			if c.onHeartbeatResp != nil {
				c.onHeartbeatResp(peerID, resp.AppliedIndex)
			}
		}(i, peer)
	}

	wg.Wait()

	mu.Lock()
	gotMajority := acks >= majority
	mu.Unlock()

	if gotMajority {
		c.mu.Lock()
		if c.role == types.RoleCoordinator {
			c.lastHeartbeatRound = time.Now()
			c.leaseValid.Store(true)
		}
		c.mu.Unlock()
	} else {
		c.leaseValid.Store(false)
	}
}

func (c *Coordinator) sendHeartbeat(ctx context.Context, addr string, epoch uint64, commitIndex uint64) (*HeartbeatResponse, error) {
	req := HeartbeatRequest{
		Epoch:         epoch,
		CoordinatorID: c.nodeID,
		CommitIndex:   commitIndex,
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("http://%s/internal/heartbeat", addr)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var hbResp HeartbeatResponse
	if err := json.NewDecoder(resp.Body).Decode(&hbResp); err != nil {
		return nil, err
	}

	return &hbResp, nil
}

// CheckLease checks if the lease is still valid (not expired).
func (c *Coordinator) CheckLease() bool {
	c.mu.RLock()
	if c.role != types.RoleCoordinator {
		c.mu.RUnlock()
		return false
	}
	lastRound := c.lastHeartbeatRound
	c.mu.RUnlock()

	leaseDeadline := lastRound.Add(time.Duration(c.config.LeaseMs) * time.Millisecond)
	valid := time.Now().Before(leaseDeadline)
	c.leaseValid.Store(valid)
	return valid
}

// StepDown transitions the node to follower.
func (c *Coordinator) StepDown(newEpoch uint64) {
	c.mu.Lock()
	if newEpoch > c.epoch {
		c.epoch = newEpoch
	}
	c.role = types.RoleFollower
	c.leaseValid.Store(false)
	c.mu.Unlock()
	// StopHeartbeatLoop must be called without c.mu held
	c.StopHeartbeatLoop()
}
