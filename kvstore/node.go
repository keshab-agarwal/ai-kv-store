package kvstore

import (
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"
)

type Node struct {
	mu sync.Mutex

	id     uint64
	config Config

	role     Role
	epoch    uint64
	votedFor uint64

	log         *OpLog
	commitIndex int64
	lastApplied int64

	nextIndex  map[uint64]int64
	matchIndex map[uint64]int64

	coordinatorID uint64

	store *Store

	dedup map[string]OpResult

	leaseDeadline time.Time

	pending   map[int64]chan OpResult
	applyCond *sync.Cond

	replicateNotify map[uint64]chan struct{}

	trans         *transport
	coordStopCh   chan struct{}
	electionTimer *time.Timer
	stopCh        chan struct{}
	stopped       bool

	logger *log.Logger
}

func NewNode(cfg Config, logger *log.Logger) *Node {
	n := &Node{
		id:              cfg.NodeID,
		config:          cfg,
		role:            Follower,
		log:             NewOpLog(),
		store:           NewStore(),
		dedup:           make(map[string]OpResult),
		pending:         make(map[int64]chan OpResult),
		replicateNotify: make(map[uint64]chan struct{}),
		stopCh:          make(chan struct{}),
		logger:          logger,
	}
	n.applyCond = sync.NewCond(&n.mu)
	n.trans = newTransport(n)
	return n
}

func (n *Node) Start() error {
	if err := n.trans.start(n.config.RPCAddr); err != nil {
		return err
	}
	n.resetElectionTimer()
	go n.electionLoop()
	go n.applyLoop()
	n.logger.Printf("node %d started rpc=%s client=%s", n.id, n.config.RPCAddr, n.config.ClientAddr)
	return nil
}

func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	close(n.stopCh)
	if n.coordStopCh != nil {
		close(n.coordStopCh)
		n.coordStopCh = nil
	}
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	n.applyCond.Broadcast()
	n.mu.Unlock()
	n.trans.stop()
}

func (n *Node) NodeID() uint64    { return n.id }
func (n *Node) StoreRef() *Store  { return n.store }
func (n *Node) LogRef() *OpLog    { return n.log }

func (n *Node) RoleInfo() (Role, uint64, uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role, n.epoch, n.coordinatorID
}

// --- Election ---

func (n *Node) resetElectionTimer() {
	timeout := n.config.ElectionTimeoutMin +
		time.Duration(rand.Int63n(int64(n.config.ElectionTimeoutMax-n.config.ElectionTimeoutMin)))
	n.mu.Lock()
	defer n.mu.Unlock()
	n.resetElectionTimerLocked(timeout)
}

func (n *Node) resetElectionTimerLocked(timeout time.Duration) {
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	n.electionTimer = time.NewTimer(timeout)
}

func (n *Node) electionLoop() {
	for {
		n.mu.Lock()
		if n.stopped {
			n.mu.Unlock()
			return
		}
		timer := n.electionTimer
		n.mu.Unlock()

		select {
		case <-n.stopCh:
			return
		case <-timer.C:
		}

		n.mu.Lock()
		if n.stopped {
			n.mu.Unlock()
			return
		}
		if n.role == Coordinator {
			timeout := n.config.ElectionTimeoutMin +
				time.Duration(rand.Int63n(int64(n.config.ElectionTimeoutMax-n.config.ElectionTimeoutMin)))
			n.resetElectionTimerLocked(timeout)
			n.mu.Unlock()
			continue
		}
		n.mu.Unlock()
		n.startElection()
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.epoch++
	n.role = Candidate
	n.votedFor = n.id
	n.coordinatorID = 0
	epoch := n.epoch
	lastIdx := n.log.LastIndex()
	lastEp := n.log.LastEpoch()
	n.mu.Unlock()

	n.logger.Printf("node %d election epoch=%d", n.id, epoch)

	votes := 1
	total := len(n.config.Peers) + 1
	majority := total/2 + 1

	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, peer := range n.config.Peers {
		wg.Add(1)
		go func(pid uint64) {
			defer wg.Done()
			req := &VoteRequest{
				Epoch:        epoch,
				CandidateID:  n.id,
				LastLogIndex: lastIdx,
				LastLogEpoch: lastEp,
			}
			var resp VoteResponse
			if err := n.trans.call(pid, "NodeRPC.RequestVote", req, &resp, n.config.RPCTimeout); err != nil {
				return
			}
			n.mu.Lock()
			if resp.Epoch > n.epoch {
				n.stepDownLocked(resp.Epoch)
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()
			if resp.Granted {
				mu.Lock()
				votes++
				mu.Unlock()
			}
		}(peer.ID)
	}
	wg.Wait()

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.epoch != epoch || n.role != Candidate {
		return
	}
	if votes >= majority {
		n.becomeCoordinatorLocked()
	} else {
		timeout := n.config.ElectionTimeoutMin +
			time.Duration(rand.Int63n(int64(n.config.ElectionTimeoutMax-n.config.ElectionTimeoutMin)))
		n.resetElectionTimerLocked(timeout)
	}
}

func (n *Node) becomeCoordinatorLocked() {
	n.logger.Printf("node %d became coordinator epoch=%d", n.id, n.epoch)
	n.role = Coordinator
	n.coordinatorID = n.id
	n.leaseDeadline = time.Time{}

	lastIdx := n.log.LastIndex()
	n.nextIndex = make(map[uint64]int64)
	n.matchIndex = make(map[uint64]int64)
	for _, peer := range n.config.Peers {
		n.nextIndex[peer.ID] = lastIdx + 1
		n.matchIndex[peer.ID] = 0
	}

	noop := LogEntry{
		Index: n.log.NextIndex(),
		Epoch: n.epoch,
		Op:    OpNoop,
	}
	n.log.Append(noop)

	if n.coordStopCh != nil {
		close(n.coordStopCh)
	}
	n.coordStopCh = make(chan struct{})

	for _, peer := range n.config.Peers {
		ch := make(chan struct{}, 1)
		n.replicateNotify[peer.ID] = ch
		go n.replicatorLoop(peer.ID, n.coordStopCh, ch)
	}
}

func (n *Node) stepDownLocked(newEpoch uint64) {
	if newEpoch > n.epoch {
		n.epoch = newEpoch
	}
	wasCoord := n.role == Coordinator
	n.role = Follower
	n.votedFor = 0

	if wasCoord && n.coordStopCh != nil {
		close(n.coordStopCh)
		n.coordStopCh = nil
	}

	for idx, ch := range n.pending {
		select {
		case ch <- OpResult{Status: StatusError}:
		default:
		}
		delete(n.pending, idx)
	}

	timeout := n.config.ElectionTimeoutMin +
		time.Duration(rand.Int63n(int64(n.config.ElectionTimeoutMax-n.config.ElectionTimeoutMin)))
	n.resetElectionTimerLocked(timeout)
}

// --- Replication ---

func (n *Node) replicatorLoop(peerID uint64, stopCh chan struct{}, notifyCh chan struct{}) {
	ticker := time.NewTicker(n.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-n.stopCh:
			return
		case <-ticker.C:
		case <-notifyCh:
		}
		n.replicateTo(peerID)
	}
}

func (n *Node) replicateTo(peerID uint64) {
	n.mu.Lock()
	if n.role != Coordinator {
		n.mu.Unlock()
		return
	}

	nextIdx := n.nextIndex[peerID]
	baseIdx := n.log.BaseIndex()

	if nextIdx <= baseIdx {
		n.sendSnapshot(peerID)
		n.mu.Unlock()
		return
	}

	entries := n.log.GetFrom(nextIdx)
	prevIndex := nextIdx - 1
	prevEpoch := n.log.EpochAt(prevIndex)
	epoch := n.epoch
	commitIdx := n.commitIndex
	n.mu.Unlock()

	req := &AppendRequest{
		Epoch:         epoch,
		CoordinatorID: n.id,
		PrevLogIndex:  prevIndex,
		PrevLogEpoch:  prevEpoch,
		Entries:       entries,
		CommitIndex:   commitIdx,
	}
	var resp AppendResponse
	if err := n.trans.call(peerID, "NodeRPC.AppendEntries", req, &resp, n.config.RPCTimeout); err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Coordinator || n.epoch != epoch {
		return
	}
	if resp.Epoch > n.epoch {
		n.stepDownLocked(resp.Epoch)
		return
	}
	if resp.Success {
		if resp.MatchIndex > n.matchIndex[peerID] {
			n.matchIndex[peerID] = resp.MatchIndex
			n.nextIndex[peerID] = resp.MatchIndex + 1
		}
		n.leaseDeadline = time.Now().Add(n.config.LeaseDuration)
		n.advanceCommitLocked()
	} else {
		if n.nextIndex[peerID] > 1 {
			n.nextIndex[peerID]--
		}
	}
}

func (n *Node) sendSnapshot(peerID uint64) {
	snap := n.store.Snapshot()
	lastIdx := n.log.BaseIndex()
	lastEp := n.log.EpochAt(lastIdx)
	epoch := n.epoch
	n.mu.Unlock()

	req := &SnapshotRequest{
		Epoch:         epoch,
		CoordinatorID: n.id,
		LastIndex:     lastIdx,
		LastEpoch:     lastEp,
		Data:          snap,
	}
	var resp SnapshotResponse
	if err := n.trans.call(peerID, "NodeRPC.InstallSnapshot", req, &resp, n.config.WriteTimeout); err != nil {
		n.mu.Lock()
		return
	}

	n.mu.Lock()
	if resp.Success && n.role == Coordinator && n.epoch == epoch {
		n.nextIndex[peerID] = lastIdx + 1
		n.matchIndex[peerID] = lastIdx
	}
}

func (n *Node) advanceCommitLocked() {
	matches := make([]int64, 0, len(n.config.Peers)+1)
	matches = append(matches, n.log.LastIndex())
	for _, peer := range n.config.Peers {
		matches = append(matches, n.matchIndex[peer.ID])
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })

	majority := len(matches)/2 + 1
	newCommit := matches[majority-1]

	if newCommit > n.commitIndex && n.log.EpochAt(newCommit) == n.epoch {
		n.commitIndex = newCommit
		n.applyCond.Broadcast()
	}
}

func (n *Node) notifyReplicators() {
	for _, ch := range n.replicateNotify {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// --- Apply ---

func (n *Node) applyLoop() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for {
		for !n.stopped && n.lastApplied >= n.commitIndex {
			n.applyCond.Wait()
		}
		if n.stopped {
			return
		}
		for n.lastApplied < n.commitIndex {
			n.lastApplied++
			entry, ok := n.log.Get(n.lastApplied)
			if !ok {
				break
			}
			result := n.applyEntry(entry)
			if entry.ClientID != "" {
				dedupKey := fmt.Sprintf("%s:%d", entry.ClientID, entry.ClientSeq)
				n.dedup[dedupKey] = result
			}
			if ch, ok := n.pending[entry.Index]; ok {
				select {
				case ch <- result:
				default:
				}
				delete(n.pending, entry.Index)
			}
		}
	}
}

func (n *Node) applyEntry(e LogEntry) OpResult {
	switch e.Op {
	case OpPut:
		n.store.Put(e.Key, e.Value)
		return OpResult{Status: StatusOK}
	case OpDelete:
		n.store.Delete(e.Key)
		return OpResult{Status: StatusOK}
	default:
		return OpResult{Status: StatusOK}
	}
}

// --- Client Operations ---

func (n *Node) HandleWrite(op OpType, key Key, value []byte, clientID string, clientSeq uint64) OpResult {
	n.mu.Lock()

	if n.role != Coordinator {
		coordID := n.coordinatorID
		n.mu.Unlock()
		if coordID == 0 {
			return OpResult{Status: StatusError}
		}
		return n.forwardWrite(coordID, op, key, value, clientID, clientSeq)
	}

	if clientID != "" {
		dedupKey := fmt.Sprintf("%s:%d", clientID, clientSeq)
		if result, ok := n.dedup[dedupKey]; ok {
			n.mu.Unlock()
			return result
		}
	}

	idx := n.log.NextIndex()
	entry := LogEntry{
		Index:     idx,
		Epoch:     n.epoch,
		Op:        op,
		Key:       key,
		Value:     value,
		ClientID:  clientID,
		ClientSeq: clientSeq,
	}
	n.log.Append(entry)

	ch := make(chan OpResult, 1)
	n.pending[idx] = ch

	n.notifyReplicators()
	n.mu.Unlock()

	select {
	case result := <-ch:
		return result
	case <-time.After(n.config.WriteTimeout):
		n.mu.Lock()
		delete(n.pending, idx)
		n.mu.Unlock()
		return OpResult{Status: StatusTimeout}
	}
}

func (n *Node) HandleRead(key Key) OpResult {
	n.mu.Lock()

	if n.role != Coordinator {
		coordID := n.coordinatorID
		n.mu.Unlock()
		if coordID == 0 {
			return OpResult{Status: StatusError}
		}
		return n.forwardRead(coordID, key)
	}

	leaseOK := time.Now().Before(n.leaseDeadline)
	n.mu.Unlock()

	if !leaseOK {
		if !n.confirmLeadership() {
			return OpResult{Status: StatusError}
		}
	}

	n.mu.Lock()
	for n.lastApplied < n.commitIndex && !n.stopped {
		n.applyCond.Wait()
	}
	n.mu.Unlock()

	value, found := n.store.Get(key)
	if found {
		return OpResult{Status: StatusFound, Value: value}
	}
	return OpResult{Status: StatusNotFound}
}

func (n *Node) confirmLeadership() bool {
	n.mu.Lock()
	if n.role != Coordinator {
		n.mu.Unlock()
		return false
	}
	epoch := n.epoch
	commitIdx := n.commitIndex
	n.mu.Unlock()

	acks := 1
	majority := (len(n.config.Peers)+1)/2 + 1
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, peer := range n.config.Peers {
		wg.Add(1)
		go func(pid uint64) {
			defer wg.Done()
			req := &AppendRequest{
				Epoch:         epoch,
				CoordinatorID: n.id,
				PrevLogIndex:  0,
				Entries:       nil,
				CommitIndex:   commitIdx,
			}
			var resp AppendResponse
			if err := n.trans.call(pid, "NodeRPC.AppendEntries", req, &resp, n.config.RPCTimeout); err != nil {
				return
			}
			if resp.Success {
				mu.Lock()
				acks++
				mu.Unlock()
			}
		}(peer.ID)
	}
	wg.Wait()

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.epoch != epoch || n.role != Coordinator {
		return false
	}
	if acks >= majority {
		n.leaseDeadline = time.Now().Add(n.config.LeaseDuration)
		return true
	}
	return false
}

func (n *Node) forwardWrite(coordID uint64, op OpType, key Key, value []byte, clientID string, clientSeq uint64) OpResult {
	req := &ForwardRequest{Op: op, Key: key, Value: value, ClientID: clientID, ClientSeq: clientSeq}
	var resp ForwardResponse
	if err := n.trans.call(coordID, "NodeRPC.ForwardOp", req, &resp, n.config.WriteTimeout); err != nil {
		return OpResult{Status: StatusTimeout}
	}
	return resp.Result
}

func (n *Node) forwardRead(coordID uint64, key Key) OpResult {
	req := &ForwardRequest{Key: key, IsRead: true}
	var resp ForwardResponse
	if err := n.trans.call(coordID, "NodeRPC.ForwardOp", req, &resp, n.config.RPCTimeout*2); err != nil {
		return OpResult{Status: StatusTimeout}
	}
	return resp.Result
}

// --- RPC Handlers ---

type NodeRPC struct {
	node *Node
}

func (r *NodeRPC) RequestVote(req *VoteRequest, resp *VoteResponse) error {
	n := r.node
	n.mu.Lock()
	defer n.mu.Unlock()

	resp.Epoch = n.epoch
	resp.Granted = false

	if req.Epoch < n.epoch {
		return nil
	}
	if req.Epoch > n.epoch {
		n.stepDownLocked(req.Epoch)
	}

	myLastEpoch := n.log.LastEpoch()
	myLastIndex := n.log.LastIndex()
	logOK := req.LastLogEpoch > myLastEpoch ||
		(req.LastLogEpoch == myLastEpoch && req.LastLogIndex >= myLastIndex)

	if (n.votedFor == 0 || n.votedFor == req.CandidateID) && logOK {
		n.votedFor = req.CandidateID
		resp.Granted = true
		timeout := n.config.ElectionTimeoutMin +
			time.Duration(rand.Int63n(int64(n.config.ElectionTimeoutMax-n.config.ElectionTimeoutMin)))
		n.resetElectionTimerLocked(timeout)
	}

	resp.Epoch = n.epoch
	return nil
}

func (r *NodeRPC) AppendEntries(req *AppendRequest, resp *AppendResponse) error {
	n := r.node
	n.mu.Lock()
	defer n.mu.Unlock()

	resp.Epoch = n.epoch
	resp.Success = false

	if req.Epoch < n.epoch {
		return nil
	}

	if req.Epoch > n.epoch || n.role == Candidate {
		n.stepDownLocked(req.Epoch)
	}

	n.role = Follower
	n.coordinatorID = req.CoordinatorID
	timeout := n.config.ElectionTimeoutMin +
		time.Duration(rand.Int63n(int64(n.config.ElectionTimeoutMax-n.config.ElectionTimeoutMin)))
	n.resetElectionTimerLocked(timeout)

	ok := n.log.MatchAndAppend(req.PrevLogIndex, req.PrevLogEpoch, req.Entries)
	if !ok {
		return nil
	}

	resp.Success = true
	resp.MatchIndex = n.log.LastIndex()

	if req.CommitIndex > n.commitIndex {
		lastIdx := n.log.LastIndex()
		if req.CommitIndex < lastIdx {
			n.commitIndex = req.CommitIndex
		} else {
			n.commitIndex = lastIdx
		}
		n.applyCond.Broadcast()
	}

	resp.Epoch = n.epoch
	return nil
}

func (r *NodeRPC) ForwardOp(req *ForwardRequest, resp *ForwardResponse) error {
	n := r.node
	if req.IsRead {
		resp.Result = n.HandleRead(req.Key)
	} else {
		resp.Result = n.HandleWrite(req.Op, req.Key, req.Value, req.ClientID, req.ClientSeq)
	}
	return nil
}

func (r *NodeRPC) InstallSnapshot(req *SnapshotRequest, resp *SnapshotResponse) error {
	n := r.node
	n.mu.Lock()
	defer n.mu.Unlock()

	resp.Epoch = n.epoch
	resp.Success = false

	if req.Epoch < n.epoch {
		return nil
	}
	if req.Epoch > n.epoch {
		n.stepDownLocked(req.Epoch)
	}

	n.store.Restore(req.Data)
	n.log.Reset(req.LastIndex, req.LastEpoch)
	n.lastApplied = req.LastIndex
	n.commitIndex = req.LastIndex
	n.coordinatorID = req.CoordinatorID

	resp.Success = true
	resp.Epoch = n.epoch
	return nil
}
