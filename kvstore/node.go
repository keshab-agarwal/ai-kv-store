package kvstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Role represents the role of a node in the Photon Quorum cluster.
type Role int

const (
	Primary   Role = iota // Active leader that accepts writes and serves reads.
	Secondary             // Passive follower that replicates from the primary.
	Candidate             // Requesting votes to become the new primary.
)

func (r Role) String() string {
	switch r {
	case Primary:
		return "primary"
	case Secondary:
		return "secondary"
	case Candidate:
		return "candidate"
	default:
		return "unknown"
	}
}

// OpType constants for log entries.
const (
	opPut    byte = 1
	opDelete byte = 2
	opNoop   byte = 3
)

// LogEntry is a single entry in the replicated log.
type LogEntry struct {
	Epoch     uint64 `json:"epoch"`
	Index     uint64 `json:"index"` // 1-based
	OpType    byte   `json:"op_type"`
	Key       string `json:"key,omitempty"`
	Value     []byte `json:"value,omitempty"`
	ClientID  string `json:"client_id,omitempty"`
	ClientSeq uint64 `json:"client_seq,omitempty"`
}

// DedupEntry caches the result of the most recent operation from a client
// for at-most-once / exactly-once semantics.
type DedupEntry struct {
	Seq    uint64
	Status string
	Value  []byte
}

// PeerInfo holds addressing information for a peer node.
type PeerInfo struct {
	ID         int
	RPCAddr    string // e.g., "localhost:17002"
	ClientAddr string // learned via heartbeats
}

// Node is the main struct for a Photon Quorum cluster member.
//
// The Photon Quorum protocol works as follows:
//   - Writes are "photon pulses" broadcast to all replicas.
//   - When a quorum of replicas "absorbs" the pulse the write collapses into
//     a committed state.
//   - The primary maintains a "coherence window" (lease) during which it can
//     serve reads directly from its local state machine without a quorum read.
type Node struct {
	id         int
	rpcAddr    string
	clientAddr string
	dataDir    string
	numShards  int

	mu          sync.RWMutex
	epoch       uint64
	role        Role
	votedFor    int // nodeID or -1 (0 is a valid ID so we use -1 as "none")
	votedEpoch  uint64

	log         []LogEntry // in-memory log; log[i].Index == i+1 when no snapshot compaction has occurred
	commitIndex uint64
	lastApplied uint64

	// snapshotIndex is the Index of the last entry included in a snapshot.
	// After a snapshot install, entries with Index <= snapshotIndex are gone
	// from n.log. We keep the last included epoch for log matching.
	snapshotIndex uint64
	snapshotEpoch uint64

	// Primary-only state
	nextIndex   map[int]uint64
	matchIndex  map[int]uint64
	leaseExpiry time.Time

	// State machine
	kv    *KVStore
	dedup map[string]DedupEntry // clientID -> last completed op

	// Peer discovery
	peers            map[int]*PeerInfo
	leaderID         int
	leaderClientAddr string

	wal *WAL

	// electionReset is used to reset the election timer from the Pulse handler.
	electionReset chan struct{}

	stopCh  chan struct{}
	stopped bool

	rpcHTTP *http.Client
}

// NewNode creates a Node, opens the WAL, and replays existing entries.
func NewNode(id int, rpcAddr, clientAddr, dataDir string, peers map[int]string, numShards int) (*Node, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dataDir, err)
	}

	walPath := filepath.Join(dataDir, "wal.log")
	wal, entries, err := OpenWAL(walPath)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}

	peerMap := make(map[int]*PeerInfo, len(peers))
	for pid, addr := range peers {
		peerMap[pid] = &PeerInfo{ID: pid, RPCAddr: addr}
	}

	n := &Node{
		id:            id,
		rpcAddr:       rpcAddr,
		clientAddr:    clientAddr,
		dataDir:       dataDir,
		numShards:     numShards,
		role:          Secondary,
		votedFor:      -1,
		leaderID:      -1,
		peers:         peerMap,
		kv:            NewKVStore(),
		dedup:         make(map[string]DedupEntry),
		nextIndex:     make(map[int]uint64),
		matchIndex:    make(map[int]uint64),
		electionReset: make(chan struct{}, 16),
		stopCh:        make(chan struct{}),
		wal:           wal,
		rpcHTTP: &http.Client{
			Timeout: 500 * time.Millisecond,
		},
	}

	// Check for a snapshot file and load it first.
	snapPath := filepath.Join(dataDir, "snapshot.json")
	if err := n.loadSnapshot(snapPath); err != nil {
		log.Printf("node %d: no snapshot or error loading snapshot: %v", id, err)
	}

	// Replay WAL entries on top of snapshot.
	for _, we := range entries {
		if we.Index <= n.snapshotIndex {
			continue // already applied via snapshot
		}
		le := walToLog(we)
		n.log = append(n.log, le)
		// Apply to state machine during replay.
		n.applyEntryLocked(le)
		n.lastApplied = we.Index
		n.commitIndex = we.Index
	}

	return n, nil
}

// Start launches the background goroutines.
func (n *Node) Start() {
	go n.runElection()
	go n.runHeartbeat()
}

// Stop shuts down the node cleanly.
func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	n.mu.Unlock()
	close(n.stopCh)
	n.wal.Close()
}

// -----------------------------------------------------------------------
// Election loop
// -----------------------------------------------------------------------

// runElection is the election timer goroutine.
// When the node is not the Primary, it waits for an election timeout.
// If no heartbeat arrives, it starts a new election.
func (n *Node) runElection() {
	for {
		timeout := electionTimeout()
		select {
		case <-n.stopCh:
			return
		case <-n.electionReset:
			// Heartbeat received — restart timer.
			continue
		case <-time.After(timeout):
			n.mu.RLock()
			role := n.role
			n.mu.RUnlock()
			if role == Primary {
				// Already primary; no election needed.
				continue
			}
			n.startElection()
		}
	}
}

// electionTimeout returns a randomised election timeout between 150ms and 300ms.
func electionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

// startElection increments the epoch, transitions to Candidate, and
// broadcasts RequestVote RPCs to peers.
func (n *Node) startElection() {
	n.mu.Lock()
	n.epoch++
	n.role = Candidate
	n.votedFor = n.id
	n.votedEpoch = n.epoch
	epoch := n.epoch
	lastIdx, lastEpoch := n.lastLogIndexAndEpoch()
	n.mu.Unlock()

	log.Printf("node %d: starting election for epoch %d", n.id, epoch)

	votes := 1 // vote for self
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, peer := range n.peers {
		wg.Add(1)
		go func(p *PeerInfo) {
			defer wg.Done()
			req := VoteRequest{
				Epoch:               epoch,
				CandidateID:         n.id,
				LastLogIndex:        lastIdx,
				LastLogEpoch:        lastEpoch,
				CandidateClientAddr: n.clientAddr,
			}
			resp, err := n.sendVoteRPC(p.RPCAddr, req)
			if err != nil {
				return
			}
			n.mu.Lock()
			if resp.Epoch > n.epoch {
				n.epoch = resp.Epoch
				n.role = Secondary
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()
			if resp.Granted {
				mu.Lock()
				votes++
				mu.Unlock()
			}
		}(peer)
	}
	wg.Wait()

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Candidate || n.epoch != epoch {
		// Something changed while we were waiting.
		return
	}

	total := len(n.peers) + 1
	quorum := total/2 + 1
	if votes >= quorum {
		log.Printf("node %d: elected as Primary for epoch %d (%d/%d votes)", n.id, epoch, votes, total)
		n.role = Primary
		n.leaderID = n.id
		n.leaderClientAddr = n.clientAddr

		// Initialise nextIndex and matchIndex.
		nextIdx := n.lastLogIndexLocked() + 1
		for pid := range n.peers {
			n.nextIndex[pid] = nextIdx
			n.matchIndex[pid] = 0
		}
		// Append a no-op entry to commit previous epoch's entries.
		noopEntry := LogEntry{
			Epoch:  epoch,
			Index:  n.lastLogIndexLocked() + 1,
			OpType: opNoop,
		}
		n.log = append(n.log, noopEntry)
		// Write no-op to WAL (best effort; don't block election).
		go func(e LogEntry) {
			if err := n.wal.Append([]WALEntry{logToWAL(e)}); err != nil {
				log.Printf("node %d: wal append noop: %v", n.id, err)
			}
		}(noopEntry)
	} else {
		n.role = Secondary
	}
}

// -----------------------------------------------------------------------
// Heartbeat / replication loop
// -----------------------------------------------------------------------

// runHeartbeat sends PhotonPulse RPCs to all peers every 10 ms when Primary.
func (n *Node) runHeartbeat() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.RLock()
			role := n.role
			n.mu.RUnlock()
			if role != Primary {
				continue
			}
			n.sendHeartbeats()
		}
	}
}

// sendHeartbeats fans out PhotonPulse RPCs to all peers and extends the lease
// if a majority respond successfully.
func (n *Node) sendHeartbeats() {
	n.mu.RLock()
	epoch := n.epoch
	commitIdx := n.commitIndex
	peers := make([]*PeerInfo, 0, len(n.peers))
	for _, p := range n.peers {
		peers = append(peers, p)
	}
	n.mu.RUnlock()

	ackCh := make(chan bool, len(peers))
	for _, peer := range peers {
		go func(p *PeerInfo) {
			req := n.buildPulseRequest(p.ID, epoch, commitIdx)
			resp, err := n.sendPulseRPC(p.RPCAddr, req)
			if err != nil {
				ackCh <- false
				return
			}
			n.mu.Lock()
			if resp.Epoch > n.epoch {
				n.epoch = resp.Epoch
				n.role = Secondary
				n.mu.Unlock()
				ackCh <- false
				return
			}
			if resp.Success && n.role == Primary {
				if resp.MatchIndex > n.matchIndex[p.ID] {
					n.matchIndex[p.ID] = resp.MatchIndex
					n.nextIndex[p.ID] = resp.MatchIndex + 1
				}
				n.advanceCommitIndex()
			} else if !resp.Success {
				// Decrement nextIndex and retry on next heartbeat.
				if n.nextIndex[p.ID] > n.snapshotIndex+1 {
					n.nextIndex[p.ID]--
				}
			}
			n.mu.Unlock()
			ackCh <- resp.Success
		}(peer)
	}

	// Count successes (primary counts as 1).
	total := len(peers) + 1
	quorum := total/2 + 1
	acks := 1 // self
	deadline := time.After(40 * time.Millisecond)
	for acks < quorum {
		select {
		case ok := <-ackCh:
			if ok {
				acks++
			}
		case <-deadline:
			goto done
		}
	}
	// Extend the coherence lease.
	n.mu.Lock()
	if n.role == Primary {
		n.leaseExpiry = time.Now().Add(40 * time.Millisecond)
	}
	n.mu.Unlock()
done:
}

// buildPulseRequest creates a PulseRequest for the given peer.
// Must be called with n.mu held or a copy of needed fields.
func (n *Node) buildPulseRequest(peerID int, epoch, commitIdx uint64) PulseRequest {
	n.mu.RLock()
	defer n.mu.RUnlock()

	nextIdx := n.nextIndex[peerID]
	if nextIdx == 0 {
		nextIdx = 1
	}

	// If peer is so far behind that we have snapshot-compacted those entries,
	// we'll handle that in a separate snapshot path. Here just send empty
	// entries if we can't satisfy from log.
	var prevLogIndex uint64
	var prevLogEpoch uint64
	var entries []LogEntry

	if nextIdx > 1 {
		prevLogIndex = nextIdx - 1
		prevLogEpoch = n.epochForIndex(prevLogIndex)
	}

	// Send entries starting at nextIdx.
	for _, e := range n.log {
		if e.Index >= nextIdx {
			entries = append(entries, e)
			// Cap at 100 entries per pulse to limit message size.
			if len(entries) >= 100 {
				break
			}
		}
	}

	return PulseRequest{
		Epoch:            epoch,
		LeaderID:         n.id,
		LeaderClientAddr: n.clientAddr,
		LeaseExpiry:      n.leaseExpiry.UnixNano(),
		PrevLogIndex:     prevLogIndex,
		PrevLogEpoch:     prevLogEpoch,
		Entries:          entries,
		CommitIndex:      commitIdx,
	}
}

// -----------------------------------------------------------------------
// Client-facing operations
// -----------------------------------------------------------------------

// HandleGet processes a read request.
// Returns (status, value) where status is a Status string.
func (n *Node) HandleGet(key string) (string, []byte) {
	n.mu.RLock()
	role := n.role
	lca := n.leaderClientAddr
	lease := n.leaseExpiry
	n.mu.RUnlock()

	if role != Primary {
		return "redirect", []byte(lca)
	}

	// Coherence window: serve from local state if lease is still valid.
	if time.Now().Before(lease) {
		val, ok := n.kv.Get(key)
		if ok {
			return StatusFound.String(), val
		}
		return StatusNotFound.String(), nil
	}

	// Lease expired — wait up to 200 ms for the heartbeat to renew it.
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		n.mu.RLock()
		lease = n.leaseExpiry
		role = n.role
		n.mu.RUnlock()
		if role != Primary {
			return "redirect", []byte(lca)
		}
		if time.Now().Before(lease) {
			val, ok := n.kv.Get(key)
			if ok {
				return StatusFound.String(), val
			}
			return StatusNotFound.String(), nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Lease still not renewed; serve with stale read under optimistic assumption.
	// In a production system we would return an error, but for availability we serve.
	val, ok := n.kv.Get(key)
	if ok {
		return StatusFound.String(), val
	}
	return StatusNotFound.String(), nil
}

// HandlePut processes a write (Put) request.
// Returns (status, error) — status is "ok" on success, "redirect" if not Primary.
func (n *Node) HandlePut(key string, value []byte, clientID string, clientSeq uint64) (string, error) {
	return n.handleWrite(opPut, key, value, clientID, clientSeq)
}

// HandleDelete processes a write (Delete) request.
func (n *Node) HandleDelete(key string, clientID string, clientSeq uint64) (string, error) {
	return n.handleWrite(opDelete, key, nil, clientID, clientSeq)
}

// handleWrite is the shared implementation for Put and Delete.
func (n *Node) handleWrite(opType byte, key string, value []byte, clientID string, clientSeq uint64) (string, error) {
	n.mu.Lock()

	if n.role != Primary {
		lca := n.leaderClientAddr
		n.mu.Unlock()
		return "redirect:" + lca, nil
	}

	// Deduplication: return cached result if this is a replay.
	if clientID != "" {
		if d, ok := n.dedup[clientID]; ok && d.Seq == clientSeq {
			n.mu.Unlock()
			return d.Status, nil
		}
	}

	// Append new log entry.
	entry := LogEntry{
		Epoch:     n.epoch,
		Index:     n.lastLogIndexLocked() + 1,
		OpType:    opType,
		Key:       key,
		Value:     value,
		ClientID:  clientID,
		ClientSeq: clientSeq,
	}
	n.log = append(n.log, entry)
	n.nextIndex[n.id] = entry.Index + 1
	peers := make([]*PeerInfo, 0, len(n.peers))
	for _, p := range n.peers {
		peers = append(peers, p)
	}
	epoch := n.epoch
	commitIdx := n.commitIndex
	n.mu.Unlock()

	// Persist to WAL before replicating.
	if err := n.wal.Append([]WALEntry{logToWAL(entry)}); err != nil {
		return "", fmt.Errorf("wal append: %w", err)
	}

	// Fan out replication to all peers in parallel.
	total := len(peers) + 1
	needed := total/2 + 1 - 1 // additional acks needed beyond self
	if needed < 0 {
		needed = 0
	}

	ackCh := make(chan bool, len(peers))
	for _, peer := range peers {
		go func(p *PeerInfo) {
			req := n.buildPulseRequest(p.ID, epoch, commitIdx)
			resp, err := n.sendPulseRPC(p.RPCAddr, req)
			if err != nil {
				ackCh <- false
				return
			}
			n.mu.Lock()
			if resp.Epoch > n.epoch {
				n.epoch = resp.Epoch
				n.role = Secondary
				n.mu.Unlock()
				ackCh <- false
				return
			}
			if resp.Success {
				if resp.MatchIndex > n.matchIndex[p.ID] {
					n.matchIndex[p.ID] = resp.MatchIndex
					n.nextIndex[p.ID] = resp.MatchIndex + 1
				}
				n.advanceCommitIndex()
			} else {
				if n.nextIndex[p.ID] > n.snapshotIndex+1 {
					n.nextIndex[p.ID]--
				}
			}
			n.mu.Unlock()
			ackCh <- resp.Success
		}(peer)
	}

	// Wait for quorum acknowledgment.
	got := 0
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for got < needed {
		select {
		case ok := <-ackCh:
			if ok {
				got++
			}
		case <-ctx.Done():
			return "", fmt.Errorf("timeout waiting for quorum")
		}
	}

	// Apply to state machine.
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Primary {
		return "redirect:" + n.leaderClientAddr, nil
	}

	// Apply all committed entries up to this entry's index.
	n.commitIndex = entry.Index
	n.applyUpToLocked(entry.Index)

	// Cache dedup result.
	if clientID != "" {
		n.dedup[clientID] = DedupEntry{Seq: clientSeq, Status: StatusOK.String()}
	}

	return StatusOK.String(), nil
}

// -----------------------------------------------------------------------
// RPC handlers (called from api.go)
// -----------------------------------------------------------------------

// HandleVote processes an incoming VoteRequest.
func (n *Node) HandleVote(req VoteRequest) VoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := VoteResponse{Epoch: n.epoch}

	if req.Epoch < n.epoch {
		// Stale candidate.
		return resp
	}

	if req.Epoch > n.epoch {
		n.epoch = req.Epoch
		n.role = Secondary
		n.votedFor = -1
	}

	// Check if we have already voted in this epoch.
	if n.votedEpoch == req.Epoch && n.votedFor != -1 && n.votedFor != req.CandidateID {
		return resp
	}

	// Log completeness check: candidate log must be at least as up-to-date as ours.
	myLastIdx, myLastEpoch := n.lastLogIndexAndEpoch()
	candUpToDate := req.LastLogEpoch > myLastEpoch ||
		(req.LastLogEpoch == myLastEpoch && req.LastLogIndex >= myLastIdx)
	if !candUpToDate {
		return resp
	}

	// Grant vote.
	n.votedFor = req.CandidateID
	n.votedEpoch = req.Epoch
	resp.Granted = true
	resp.Epoch = n.epoch

	// Reset election timer since we heard from a valid candidate.
	select {
	case n.electionReset <- struct{}{}:
	default:
	}

	return resp
}

// HandlePulse processes an incoming PulseRequest (heartbeat + replication).
func (n *Node) HandlePulse(req PulseRequest) PulseResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := PulseResponse{Epoch: n.epoch, NodeID: n.id}

	if req.Epoch < n.epoch {
		// Stale leader.
		return resp
	}

	// Accept the pulse.
	if req.Epoch > n.epoch {
		n.epoch = req.Epoch
		n.votedFor = -1
	}
	n.role = Secondary
	n.leaderID = req.LeaderID
	n.leaderClientAddr = req.LeaderClientAddr

	// Reset election timer.
	select {
	case n.electionReset <- struct{}{}:
	default:
	}

	// Consistency check: verify PrevLogIndex / PrevLogEpoch.
	if req.PrevLogIndex > 0 {
		prevEpoch := n.epochForIndex(req.PrevLogIndex)
		if prevEpoch == 0 && req.PrevLogIndex > n.snapshotIndex {
			// We don't have this entry.
			resp.MatchIndex = n.lastLogIndexLocked()
			return resp
		}
		if prevEpoch != req.PrevLogEpoch {
			// Conflict — truncate from PrevLogIndex onward.
			n.truncateLogFrom(req.PrevLogIndex)
			resp.MatchIndex = n.lastLogIndexLocked()
			return resp
		}
	}

	// Append new entries.
	for _, e := range req.Entries {
		existing := n.epochForIndex(e.Index)
		if existing != 0 {
			if existing != e.Epoch {
				// Conflict — truncate from this index onward.
				n.truncateLogFrom(e.Index)
				n.log = append(n.log, e)
			}
			// else: already have this entry, skip
		} else {
			n.log = append(n.log, e)
		}
	}

	// Persist new entries to WAL.
	if len(req.Entries) > 0 {
		walEntries := make([]WALEntry, len(req.Entries))
		for i, e := range req.Entries {
			walEntries[i] = logToWAL(e)
		}
		// Async WAL write; we respond before WAL confirms for performance.
		go func() {
			if err := n.wal.Append(walEntries); err != nil {
				log.Printf("node %d: secondary wal append: %v", n.id, err)
			}
		}()
	}

	// Advance commitIndex.
	if req.CommitIndex > n.commitIndex {
		lastIdx := n.lastLogIndexLocked()
		newCommit := req.CommitIndex
		if newCommit > lastIdx {
			newCommit = lastIdx
		}
		n.commitIndex = newCommit
		n.applyUpToLocked(n.commitIndex)
	}

	resp.Success = true
	resp.MatchIndex = n.lastLogIndexLocked()
	resp.Epoch = n.epoch
	return resp
}

// HandleSnapshot installs a snapshot sent by the leader.
func (n *Node) HandleSnapshot(req SnapshotRequest2) SnapshotResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := SnapshotResponse{Epoch: n.epoch}

	if req.Epoch < n.epoch {
		return resp
	}
	if req.Epoch > n.epoch {
		n.epoch = req.Epoch
		n.role = Secondary
		n.votedFor = -1
	}
	n.leaderID = req.LeaderID

	// Reset election timer.
	select {
	case n.electionReset <- struct{}{}:
	default:
	}

	if req.LastIncludedIndex <= n.snapshotIndex {
		// Already have a newer snapshot.
		resp.Success = true
		return resp
	}

	// Decode snapshot data.
	kvData := make(map[string][]byte, len(req.Data.Keys))
	for k, b64v := range req.Data.Keys {
		kvData[k] = []byte(b64v) // stored as raw bytes; api.go decodes base64
	}
	n.kv.ApplySnapshot(kvData)
	n.snapshotIndex = req.LastIncludedIndex
	n.snapshotEpoch = req.LastIncludedEpoch
	n.commitIndex = req.LastIncludedIndex
	n.lastApplied = req.LastIncludedIndex

	// Discard log entries covered by the snapshot.
	var remaining []LogEntry
	for _, e := range n.log {
		if e.Index > req.LastIncludedIndex {
			remaining = append(remaining, e)
		}
	}
	n.log = remaining

	// Truncate WAL to remove covered entries.
	go func() {
		if err := n.wal.Truncate(req.LastIncludedIndex); err != nil {
			log.Printf("node %d: wal truncate after snapshot: %v", n.id, err)
		}
		// Persist snapshot to disk.
		if err := n.persistSnapshot(); err != nil {
			log.Printf("node %d: persist snapshot: %v", n.id, err)
		}
	}()

	resp.Success = true
	resp.Epoch = n.epoch
	return resp
}

// -----------------------------------------------------------------------
// Snapshot persistence
// -----------------------------------------------------------------------

// snapshotFile is the on-disk representation of a snapshot.
type snapshotFile struct {
	SnapshotIndex uint64            `json:"snapshot_index"`
	SnapshotEpoch uint64            `json:"snapshot_epoch"`
	CommitIndex   uint64            `json:"commit_index"`
	Data          map[string][]byte `json:"data"`
}

// persistSnapshot writes the current KV state to disk.
// Must be called without holding n.mu (it calls n.kv.Snapshot which takes its own lock).
func (n *Node) persistSnapshot() error {
	kvData := n.kv.Snapshot()
	n.mu.RLock()
	sf := snapshotFile{
		SnapshotIndex: n.snapshotIndex,
		SnapshotEpoch: n.snapshotEpoch,
		CommitIndex:   n.commitIndex,
		Data:          kvData,
	}
	snapPath := filepath.Join(n.dataDir, "snapshot.json")
	n.mu.RUnlock()

	data, err := json.Marshal(sf)
	if err != nil {
		return err
	}
	tmp := snapPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, snapPath)
}

// loadSnapshot reads a snapshot file from disk and applies it to the node state.
// Called during startup before WAL replay.
func (n *Node) loadSnapshot(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err // typically os.ErrNotExist
	}
	var sf snapshotFile
	if err := json.Unmarshal(data, &sf); err != nil {
		return err
	}
	n.kv.ApplySnapshot(sf.Data)
	n.snapshotIndex = sf.SnapshotIndex
	n.snapshotEpoch = sf.SnapshotEpoch
	n.commitIndex = sf.CommitIndex
	n.lastApplied = sf.CommitIndex
	return nil
}

// -----------------------------------------------------------------------
// Helper methods (all require n.mu to be held by caller unless noted)
// -----------------------------------------------------------------------

// lastLogIndexLocked returns the Index of the last log entry, or snapshotIndex if empty.
// Caller must hold n.mu.
func (n *Node) lastLogIndexLocked() uint64 {
	if len(n.log) == 0 {
		return n.snapshotIndex
	}
	return n.log[len(n.log)-1].Index
}

// lastLogIndexAndEpoch returns the index and epoch of the last log entry.
// Caller must hold n.mu.
func (n *Node) lastLogIndexAndEpoch() (uint64, uint64) {
	if len(n.log) == 0 {
		return n.snapshotIndex, n.snapshotEpoch
	}
	last := n.log[len(n.log)-1]
	return last.Index, last.Epoch
}

// epochForIndex returns the epoch of the log entry at the given 1-based index,
// or 0 if not found. Caller must hold n.mu.
func (n *Node) epochForIndex(idx uint64) uint64 {
	if idx == n.snapshotIndex {
		return n.snapshotEpoch
	}
	for _, e := range n.log {
		if e.Index == idx {
			return e.Epoch
		}
	}
	return 0
}

// truncateLogFrom removes all log entries with Index >= fromIndex.
// Caller must hold n.mu.
func (n *Node) truncateLogFrom(fromIndex uint64) {
	j := 0
	for _, e := range n.log {
		if e.Index < fromIndex {
			n.log[j] = e
			j++
		}
	}
	n.log = n.log[:j]
}

// advanceCommitIndex updates commitIndex based on matchIndex quorum.
// Caller must hold n.mu.
func (n *Node) advanceCommitIndex() {
	total := len(n.peers) + 1
	quorum := total/2 + 1

	// Find the highest index replicated to a quorum.
	myLastIdx := n.lastLogIndexLocked()
	for idx := myLastIdx; idx > n.commitIndex; idx-- {
		// Check the epoch of this entry — only commit entries from current epoch.
		entryEpoch := n.epochForIndex(idx)
		if entryEpoch != n.epoch {
			break
		}
		count := 1 // self
		for _, mi := range n.matchIndex {
			if mi >= idx {
				count++
			}
		}
		if count >= quorum {
			n.commitIndex = idx
			n.applyUpToLocked(idx)
			break
		}
	}
}

// applyUpToLocked applies all log entries with Index <= upTo that have not
// yet been applied. Caller must hold n.mu.
func (n *Node) applyUpToLocked(upTo uint64) {
	for _, e := range n.log {
		if e.Index <= n.lastApplied {
			continue
		}
		if e.Index > upTo {
			break
		}
		n.applyEntryLocked(e)
		n.lastApplied = e.Index
	}
}

// applyEntryLocked applies a single log entry to the state machine.
// Caller must hold n.mu (or be in single-threaded startup replay).
func (n *Node) applyEntryLocked(e LogEntry) {
	switch e.OpType {
	case opPut:
		n.kv.Put(e.Key, e.Value)
	case opDelete:
		n.kv.Delete(e.Key)
	case opNoop:
		// No-op.
	}
}

// logToWAL converts a LogEntry to a WALEntry.
func logToWAL(e LogEntry) WALEntry {
	return WALEntry{
		Epoch:     e.Epoch,
		Index:     e.Index,
		OpType:    e.OpType,
		Key:       e.Key,
		Value:     e.Value,
		ClientID:  e.ClientID,
		ClientSeq: e.ClientSeq,
	}
}

// walToLog converts a WALEntry back to a LogEntry.
func walToLog(e WALEntry) LogEntry {
	return LogEntry{
		Epoch:     e.Epoch,
		Index:     e.Index,
		OpType:    e.OpType,
		Key:       e.Key,
		Value:     e.Value,
		ClientID:  e.ClientID,
		ClientSeq: e.ClientSeq,
	}
}

// -----------------------------------------------------------------------
// Outgoing RPC calls
// -----------------------------------------------------------------------

// sendVoteRPC sends a RequestVote RPC to the given address.
func (n *Node) sendVoteRPC(addr string, req VoteRequest) (VoteResponse, error) {
	var resp VoteResponse
	return resp, n.doRPC("http://"+addr+"/rpc/vote", req, &resp)
}

// sendPulseRPC sends a PhotonPulse RPC to the given address.
func (n *Node) sendPulseRPC(addr string, req PulseRequest) (PulseResponse, error) {
	var resp PulseResponse
	return resp, n.doRPC("http://"+addr+"/rpc/pulse", req, &resp)
}

// sendSnapshotRPC sends a Snapshot install RPC to the given address.
func (n *Node) sendSnapshotRPC(addr string, req SnapshotRequest2) (SnapshotResponse, error) {
	var resp SnapshotResponse
	return resp, n.doRPC("http://"+addr+"/rpc/snapshot", req, &resp)
}

// doRPC sends a JSON-encoded request and decodes the JSON response.
func (n *Node) doRPC(url string, reqBody interface{}, respBody interface{}) error {
	data, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshal rpc: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpResp, err := n.rpcHTTP.Do(httpReq)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()
	if err := json.NewDecoder(httpResp.Body).Decode(respBody); err != nil {
		return fmt.Errorf("decode rpc response: %w", err)
	}
	return nil
}

// -----------------------------------------------------------------------
// Status
// -----------------------------------------------------------------------

// Status returns the current status of the node for the /api/status endpoint.
func (n *Node) StatusInfo() StatusResponse {
	n.mu.RLock()
	defer n.mu.RUnlock()
	roleStr := n.role.String()
	return StatusResponse{
		NodeID:           n.id,
		Role:             roleStr,
		Epoch:            n.epoch,
		CommitIndex:      n.commitIndex,
		LeaderID:         n.leaderID,
		LeaderClientAddr: n.leaderClientAddr,
		Keys:             n.kv.Len(),
	}
}

// MaybeInstallSnapshot checks whether a peer is so far behind that we should
// send a snapshot instead of individual entries. Called from the heartbeat path.
func (n *Node) MaybeInstallSnapshot(peerID int) {
	n.mu.RLock()
	nextIdx := n.nextIndex[peerID]
	peer, ok := n.peers[peerID]
	commitIdx := n.commitIndex
	snapshotIdx := n.snapshotIndex
	epoch := n.epoch
	n.mu.RUnlock()

	if !ok {
		return
	}
	if nextIdx > 0 && commitIdx-nextIdx < 1000 {
		return // not far enough behind
	}
	if snapshotIdx == 0 {
		return // no snapshot to send
	}

	// Build snapshot request.
	kvSnap := n.kv.Snapshot()
	keys := make(map[string]string, len(kvSnap))
	for k, v := range kvSnap {
		keys[k] = string(v) // raw bytes; api.go handles base64 for HTTP
	}

	req := SnapshotRequest2{
		Epoch:             epoch,
		LeaderID:          n.id,
		LastIncludedIndex: snapshotIdx,
		LastIncludedEpoch: n.snapshotEpoch,
		Data:              SnapshotData{Keys: keys},
	}

	resp, err := n.sendSnapshotRPC(peer.RPCAddr, req)
	if err != nil {
		log.Printf("node %d: snapshot to peer %d: %v", n.id, peerID, err)
		return
	}
	n.mu.Lock()
	if resp.Epoch > n.epoch {
		n.epoch = resp.Epoch
		n.role = Secondary
	}
	if resp.Success {
		n.nextIndex[peerID] = snapshotIdx + 1
		n.matchIndex[peerID] = snapshotIdx
	}
	n.mu.Unlock()
}

// _ is used to suppress the walToLog unused warning — it is used during WAL replay at startup.
var _ = walToLog
