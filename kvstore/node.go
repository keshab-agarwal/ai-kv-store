package kvstore

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"strings"
	"sync"
	"time"
)

// Role represents a node's current state in the Resonance Protocol.
type Role uint8

const (
	Follower  Role = iota // Receiving wavefronts from the resonator
	Seeker                // Seeking to become resonator (election)
	Resonator             // The active resonator (leader)
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Seeker:
		return "Seeker"
	case Resonator:
		return "Resonator"
	default:
		return "Unknown"
	}
}

// Tuning constants — calibrated for single-datacenter, low-latency operation.
const (
	pulseInterval      = 50 * time.Millisecond  // Resonator pulse (heartbeat) interval
	electionTimeoutMin = 300 * time.Millisecond  // Min election timeout
	electionTimeoutMax = 600 * time.Millisecond  // Max election timeout
	coherenceWindow    = 150 * time.Millisecond  // Lease duration for local reads
	rpcTimeout         = 200 * time.Millisecond  // RPC call timeout
	maxBatchSize       = 64                      // Max wavefronts per Propagate RPC
)

// OpResult carries the outcome of a client operation back to the HTTP handler.
type OpResult struct {
	Value    []byte
	Found    bool
	Err      error
	Timeout  bool
}

// pendingOp tracks a client write waiting for crystallization (commit).
type pendingOp struct {
	index  int64
	result chan OpResult
}

// Node is a single participant in the Resonance Consensus Protocol.
type Node struct {
	mu sync.Mutex

	// Identity
	id    int
	peers map[int]string // peer ID -> RPC address

	// Persistent state (would be durable in production)
	currentPhase uint64
	votedFor     int // -1 = none
	log          []*Wavefront

	// Volatile state
	role        Role
	leaderID    int
	commitIndex int64
	lastApplied int64

	// Leader-only state
	nextIndex  map[int]int64
	matchIndex map[int]int64
	leaseEnd   time.Time

	// State machine
	store *Store

	// Pending writes awaiting commit
	pendingOps map[int64]*pendingOp

	// Networking
	clientAddr   string
	rpcAddr      string
	rpcClients   map[int]*rpc.Client
	rpcClientsMu sync.Mutex
	httpServer   *http.Server
	rpcListener  net.Listener

	// Leader's client address (for follower redirects)
	leaderClientAddr string

	// Timers
	electionTimer *time.Timer
	rand          *rand.Rand

	// Lifecycle
	stopCh chan struct{}
	wg     sync.WaitGroup
	logger *log.Logger
}

// NewNode creates a new consensus node.
func NewNode(id int, rpcAddr, clientAddr string, peers map[int]string) *Node {
	n := &Node{
		id:           id,
		peers:        peers,
		currentPhase: 0,
		votedFor:     -1,
		log:          make([]*Wavefront, 0),
		role:         Follower,
		leaderID:     -1,
		commitIndex:  0,
		lastApplied:  0,
		store:        NewStore(),
		pendingOps:   make(map[int64]*pendingOp),
		clientAddr:   normalizeAddr(clientAddr),
		rpcAddr:      rpcAddr,
		rpcClients:   make(map[int]*rpc.Client),
		stopCh:       make(chan struct{}),
		rand:         rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)*1000)),
		logger:       log.New(log.Writer(), fmt.Sprintf("[node-%d] ", id), log.LstdFlags|log.Lmicroseconds),
	}
	return n
}

// Start boots the node: starts RPC server, HTTP server, and background loops.
func (n *Node) Start() error {
	// Start RPC server
	svc := &RPCService{Node: n}
	server := rpc.NewServer()
	if err := server.Register(svc); err != nil {
		return fmt.Errorf("rpc register: %w", err)
	}

	var err error
	n.rpcListener, err = net.Listen("tcp", n.rpcAddr)
	if err != nil {
		return fmt.Errorf("rpc listen %s: %w", n.rpcAddr, err)
	}
	n.logger.Printf("RPC listening on %s", n.rpcListener.Addr())

	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		for {
			conn, err := n.rpcListener.Accept()
			if err != nil {
				select {
				case <-n.stopCh:
					return
				default:
					continue
				}
			}
			go server.ServeConn(conn)
		}
	}()

	// Start HTTP server for client API
	mux := http.NewServeMux()
	mux.HandleFunc("/kv/", n.handleKV)
	mux.HandleFunc("/health", n.handleHealth)
	n.httpServer = &http.Server{
		Addr:    n.clientAddr,
		Handler: mux,
	}
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.logger.Printf("Client HTTP listening on %s", n.clientAddr)
		if err := n.httpServer.ListenAndServe(); err != http.ErrServerClosed {
			n.logger.Printf("HTTP server error: %v", err)
		}
	}()

	// Start election timer
	n.resetElectionTimer()

	// Start background loops
	n.wg.Add(1)
	go n.electionLoop()

	return nil
}

// Stop gracefully shuts down the node.
func (n *Node) Stop() {
	close(n.stopCh)

	if n.rpcListener != nil {
		n.rpcListener.Close()
	}
	if n.httpServer != nil {
		n.httpServer.Close()
	}

	n.rpcClientsMu.Lock()
	for id, c := range n.rpcClients {
		c.Close()
		delete(n.rpcClients, id)
	}
	n.rpcClientsMu.Unlock()

	// Fail all pending ops
	n.mu.Lock()
	for idx, op := range n.pendingOps {
		op.result <- OpResult{Err: fmt.Errorf("node shutting down")}
		delete(n.pendingOps, idx)
	}
	n.mu.Unlock()

	n.wg.Wait()
}

// ──────────────────────────────────────────────
// Election (Phase Seeking)
// ──────────────────────────────────────────────

func (n *Node) resetElectionTimer() {
	timeout := electionTimeoutMin + time.Duration(n.rand.Int63n(int64(electionTimeoutMax-electionTimeoutMin)))
	n.mu.Lock()
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	n.electionTimer = time.NewTimer(timeout)
	n.mu.Unlock()
}

func (n *Node) electionLoop() {
	defer n.wg.Done()
	for {
		n.mu.Lock()
		timer := n.electionTimer
		n.mu.Unlock()

		select {
		case <-n.stopCh:
			return
		case <-timer.C:
			// Only start election if we're a follower (not already the resonator).
			n.mu.Lock()
			role := n.role
			n.mu.Unlock()
			if role == Resonator {
				// We're the leader — just reset the timer and continue.
				n.resetElectionTimer()
				continue
			}
			n.startElection()
		}
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	// Double-check: don't start election if we're already the resonator.
	if n.role == Resonator {
		n.mu.Unlock()
		n.resetElectionTimer()
		return
	}
	n.currentPhase++
	n.role = Seeker
	n.votedFor = n.id
	phase := n.currentPhase
	lastLogIndex, lastLogPhase := n.lastLogInfo()
	n.mu.Unlock()

	n.logger.Printf("Starting election for phase %d", phase)
	n.resetElectionTimer()

	args := &PhaseVoteArgs{
		Phase:        phase,
		CandidateID:  n.id,
		LastLogIndex: lastLogIndex,
		LastLogPhase: lastLogPhase,
	}

	votes := 1 // Self-vote
	total := len(n.peers) + 1
	majority := total/2 + 1

	var voteMu sync.Mutex
	done := make(chan struct{}, 1)

	for peerID := range n.peers {
		go func(pid int) {
			reply := &PhaseVoteReply{}
			if err := n.callRPC(pid, "RPCService.PhaseVote", args, reply); err != nil {
				return
			}

			voteMu.Lock()
			defer voteMu.Unlock()

			if reply.Phase > phase {
				n.mu.Lock()
				n.stepDown(reply.Phase)
				n.mu.Unlock()
				return
			}

			if reply.Granted {
				votes++
				if votes >= majority {
					select {
					case done <- struct{}{}:
					default:
					}
				}
			}
		}(peerID)
	}

	// Wait for majority or timeout
	select {
	case <-done:
	case <-time.After(electionTimeoutMin):
		return
	case <-n.stopCh:
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.currentPhase == phase && n.role == Seeker {
		n.becomeResonator()
	}
}

// becomeResonator transitions this node to the Resonator role. Must hold n.mu.
func (n *Node) becomeResonator() {
	n.role = Resonator
	n.leaderID = n.id
	n.leaderClientAddr = n.clientAddr
	n.logger.Printf("Became resonator for phase %d", n.currentPhase)

	// Initialize leader state
	nextIdx := n.lastLogIndex() + 1
	n.nextIndex = make(map[int]int64)
	n.matchIndex = make(map[int]int64)
	for pid := range n.peers {
		n.nextIndex[pid] = nextIdx
		n.matchIndex[pid] = 0
	}

	// Reset election timer so it doesn't fire while we're the resonator.
	n.resetElectionTimerLocked()

	// Start pulsing (heartbeating)
	n.wg.Add(1)
	go n.pulseLoop(n.currentPhase)

	// Send initial empty pulse to assert leadership
	go n.sendPulses()
}

// stepDown reverts to Follower if we see a higher phase. Must hold n.mu.
func (n *Node) stepDown(newPhase uint64) {
	if newPhase > n.currentPhase {
		n.currentPhase = newPhase
		n.votedFor = -1
	}
	if n.role != Follower {
		n.role = Follower
		// Fail pending ops since we're no longer leader
		for idx, op := range n.pendingOps {
			op.result <- OpResult{Err: fmt.Errorf("lost leadership")}
			delete(n.pendingOps, idx)
		}
	}
}

// ──────────────────────────────────────────────
// Wavefront Propagation (Log Replication)
// ──────────────────────────────────────────────

func (n *Node) pulseLoop(phase uint64) {
	defer n.wg.Done()
	ticker := time.NewTicker(pulseInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			if n.currentPhase != phase || n.role != Resonator {
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()
			n.sendPulses()
		}
	}
}

func (n *Node) sendPulses() {
	n.mu.Lock()
	if n.role != Resonator {
		n.mu.Unlock()
		return
	}
	phase := n.currentPhase
	n.mu.Unlock()

	ackCount := int32(1) // Count self
	total := len(n.peers) + 1
	majority := total/2 + 1
	var ackMu sync.Mutex

	var wg sync.WaitGroup
	for peerID := range n.peers {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			if n.sendPropagateOne(pid, phase) {
				ackMu.Lock()
				ackCount++
				ackMu.Unlock()
			}
		}(peerID)
	}
	wg.Wait()

	ackMu.Lock()
	acks := int(ackCount)
	ackMu.Unlock()

	if acks >= majority {
		n.mu.Lock()
		if n.role == Resonator && n.currentPhase == phase {
			n.leaseEnd = time.Now().Add(coherenceWindow)
		}
		n.mu.Unlock()
	}
}

func (n *Node) sendPropagateOne(peerID int, phase uint64) bool {
	n.mu.Lock()
	if n.role != Resonator || n.currentPhase != phase {
		n.mu.Unlock()
		return false
	}

	nextIdx := n.nextIndex[peerID]
	prevLogIndex := nextIdx - 1
	prevLogPhase := uint64(0)
	if prevLogIndex > 0 && prevLogIndex <= int64(len(n.log)) {
		prevLogPhase = n.log[prevLogIndex-1].Phase
	}

	// Gather entries to send
	var entries []*Wavefront
	logLen := int64(len(n.log))
	if nextIdx <= logLen {
		end := nextIdx + int64(maxBatchSize)
		if end > logLen+1 {
			end = logLen + 1
		}
		entries = make([]*Wavefront, end-nextIdx)
		copy(entries, n.log[nextIdx-1:end-1])
	}

	args := &PropagateArgs{
		Phase:        phase,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogPhase: prevLogPhase,
		Entries:      entries,
		CommitIndex:  n.commitIndex,
		LeaderClient: n.clientAddr,
	}
	n.mu.Unlock()

	reply := &PropagateReply{}
	if err := n.callRPC(peerID, "RPCService.Propagate", args, reply); err != nil {
		return false
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Phase > n.currentPhase {
		n.stepDown(reply.Phase)
		return false
	}

	if n.role != Resonator || n.currentPhase != phase {
		return false
	}

	if reply.Success {
		if reply.MatchIndex > n.matchIndex[peerID] {
			n.matchIndex[peerID] = reply.MatchIndex
			n.nextIndex[peerID] = reply.MatchIndex + 1
		}
		n.advanceCommitIndex()
		return true
	}

	// Decrement nextIndex and retry on next pulse
	if n.nextIndex[peerID] > 1 {
		n.nextIndex[peerID]--
	}
	return false
}

// advanceCommitIndex checks if any new wavefronts can be crystallized (committed).
// Must hold n.mu.
func (n *Node) advanceCommitIndex() {
	for idx := n.commitIndex + 1; idx <= int64(len(n.log)); idx++ {
		if n.log[idx-1].Phase != n.currentPhase {
			continue
		}
		// Count replicas that have this entry
		replicaCount := 1 // Self
		for pid := range n.peers {
			if n.matchIndex[pid] >= idx {
				replicaCount++
			}
		}
		total := len(n.peers) + 1
		if replicaCount > total/2 {
			n.commitIndex = idx
		}
	}

	n.applyCommitted()
}

// applyCommitted applies all committed but unapplied wavefronts to the store.
// Must hold n.mu.
func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		wf := n.log[n.lastApplied-1]
		switch wf.Op {
		case OpPut:
			n.store.Put(wf.Key, wf.Value)
		case OpDelete:
			n.store.Delete(wf.Key)
		}

		// Notify pending client ops
		if op, ok := n.pendingOps[n.lastApplied]; ok {
			op.result <- OpResult{}
			delete(n.pendingOps, n.lastApplied)
		}
	}
}

// ──────────────────────────────────────────────
// RPC Handlers
// ──────────────────────────────────────────────

func (n *Node) handlePhaseVote(args *PhaseVoteArgs, reply *PhaseVoteReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply.Phase = n.currentPhase

	if args.Phase < n.currentPhase {
		reply.Granted = false
		return
	}

	if args.Phase > n.currentPhase {
		n.stepDown(args.Phase)
	}

	// Grant vote if we haven't voted yet (or already voted for this candidate)
	// AND the candidate's log is at least as up-to-date as ours
	if (n.votedFor == -1 || n.votedFor == args.CandidateID) && n.isLogUpToDate(args.LastLogIndex, args.LastLogPhase) {
		n.votedFor = args.CandidateID
		reply.Granted = true
		n.resetElectionTimerLocked()
	}
}

func (n *Node) handlePropagate(args *PropagateArgs, reply *PropagateReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply.Phase = n.currentPhase

	if args.Phase < n.currentPhase {
		reply.Success = false
		return
	}

	if args.Phase > n.currentPhase || n.role != Follower {
		n.stepDown(args.Phase)
	}

	n.leaderID = args.LeaderID
	n.leaderClientAddr = args.LeaderClient
	n.resetElectionTimerLocked()

	// Check log consistency
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > int64(len(n.log)) {
			reply.Success = false
			reply.MatchIndex = int64(len(n.log))
			return
		}
		if n.log[args.PrevLogIndex-1].Phase != args.PrevLogPhase {
			// Truncate conflicting entries
			n.log = n.log[:args.PrevLogIndex-1]
			reply.Success = false
			reply.MatchIndex = int64(len(n.log))
			return
		}
	}

	// Append new entries
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + int64(i) + 1
		if idx <= int64(len(n.log)) {
			if n.log[idx-1].Phase != entry.Phase {
				n.log = n.log[:idx-1]
				n.log = append(n.log, entry)
			}
		} else {
			n.log = append(n.log, entry)
		}
	}

	// Update commit index
	if args.CommitIndex > n.commitIndex {
		newCommit := args.CommitIndex
		logLen := int64(len(n.log))
		if newCommit > logLen {
			newCommit = logLen
		}
		n.commitIndex = newCommit
		n.applyCommitted()
	}

	reply.Success = true
	reply.MatchIndex = int64(len(n.log))
}

// ──────────────────────────────────────────────
// Client Operations
// ──────────────────────────────────────────────

// ClientGet performs a linearizable read.
func (n *Node) ClientGet(key [16]byte) OpResult {
	n.mu.Lock()

	if n.role != Resonator {
		leaderAddr := n.leaderClientAddr
		n.mu.Unlock()
		return n.forwardToLeader("GET", key, nil, leaderAddr)
	}

	// Check coherence window (lease)
	if time.Now().Before(n.leaseEnd) {
		// Lease is valid — serve read locally
		n.mu.Unlock()
		val, found := n.store.Get(key)
		return OpResult{Value: val, Found: found}
	}
	n.mu.Unlock()

	// Lease expired — confirm leadership via a round of pulses
	n.sendPulses()

	n.mu.Lock()
	if n.role != Resonator {
		leaderAddr := n.leaderClientAddr
		n.mu.Unlock()
		return n.forwardToLeader("GET", key, nil, leaderAddr)
	}
	if !time.Now().Before(n.leaseEnd) {
		n.mu.Unlock()
		return OpResult{Err: fmt.Errorf("could not confirm leadership")}
	}
	n.mu.Unlock()

	val, found := n.store.Get(key)
	return OpResult{Value: val, Found: found}
}

// ClientPut performs a linearizable write.
func (n *Node) ClientPut(key [16]byte, value []byte) OpResult {
	return n.clientWrite(OpPut, key, value)
}

// ClientDelete performs a linearizable delete.
func (n *Node) ClientDelete(key [16]byte) OpResult {
	return n.clientWrite(OpDelete, key, nil)
}

func (n *Node) clientWrite(op OpType, key [16]byte, value []byte) OpResult {
	n.mu.Lock()

	if n.role != Resonator {
		leaderAddr := n.leaderClientAddr
		n.mu.Unlock()
		method := "PUT"
		if op == OpDelete {
			method = "DELETE"
		}
		return n.forwardToLeader(method, key, value, leaderAddr)
	}

	// Append to log
	wf := &Wavefront{
		Phase: n.currentPhase,
		Index: int64(len(n.log)) + 1,
		Op:    op,
		Key:   key,
		Value: value,
	}
	n.log = append(n.log, wf)

	resultCh := make(chan OpResult, 1)
	n.pendingOps[wf.Index] = &pendingOp{
		index:  wf.Index,
		result: resultCh,
	}
	n.mu.Unlock()

	// Trigger immediate replication
	go n.sendPulses()

	// Wait for commit
	select {
	case result := <-resultCh:
		return result
	case <-time.After(5 * time.Second):
		n.mu.Lock()
		delete(n.pendingOps, wf.Index)
		n.mu.Unlock()
		return OpResult{Timeout: true}
	case <-n.stopCh:
		return OpResult{Err: fmt.Errorf("node shutting down")}
	}
}

func (n *Node) forwardToLeader(method string, key [16]byte, value []byte, leaderAddr string) OpResult {
	if leaderAddr == "" {
		return OpResult{Err: fmt.Errorf("no known leader")}
	}
	return OpResult{Err: fmt.Errorf("redirect:%s", leaderAddr)}
}

// ──────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────

func (n *Node) lastLogInfo() (index int64, phase uint64) {
	if len(n.log) == 0 {
		return 0, 0
	}
	last := n.log[len(n.log)-1]
	return last.Index, last.Phase
}

func (n *Node) lastLogIndex() int64 {
	return int64(len(n.log))
}

func (n *Node) isLogUpToDate(lastIndex int64, lastPhase uint64) bool {
	myIndex, myPhase := n.lastLogInfo()
	if lastPhase != myPhase {
		return lastPhase > myPhase
	}
	return lastIndex >= myIndex
}

func (n *Node) resetElectionTimerLocked() {
	timeout := electionTimeoutMin + time.Duration(n.rand.Int63n(int64(electionTimeoutMax-electionTimeoutMin)))
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	n.electionTimer = time.NewTimer(timeout)
}

func (n *Node) callRPC(peerID int, method string, args interface{}, reply interface{}) error {
	n.rpcClientsMu.Lock()
	client, ok := n.rpcClients[peerID]
	if !ok || client == nil {
		addr, exists := n.peers[peerID]
		if !exists {
			n.rpcClientsMu.Unlock()
			return fmt.Errorf("unknown peer %d", peerID)
		}
		var err error
		conn, err := net.DialTimeout("tcp", addr, rpcTimeout)
		if err != nil {
			n.rpcClientsMu.Unlock()
			return err
		}
		client = rpc.NewClient(conn)
		n.rpcClients[peerID] = client
	}
	n.rpcClientsMu.Unlock()

	call := client.Go(method, args, reply, nil)
	select {
	case <-call.Done:
		if call.Error != nil {
			// Connection broken — remove stale client
			n.rpcClientsMu.Lock()
			if c, ok := n.rpcClients[peerID]; ok && c == client {
				client.Close()
				delete(n.rpcClients, peerID)
			}
			n.rpcClientsMu.Unlock()
		}
		return call.Error
	case <-time.After(rpcTimeout):
		// Timeout — close stale connection
		n.rpcClientsMu.Lock()
		if c, ok := n.rpcClients[peerID]; ok && c == client {
			client.Close()
			delete(n.rpcClients, peerID)
		}
		n.rpcClientsMu.Unlock()
		return fmt.Errorf("rpc timeout")
	case <-n.stopCh:
		return fmt.Errorf("node stopped")
	}
}

// normalizeAddr ensures a listen address like ":16001" becomes "localhost:16001".
func normalizeAddr(addr string) string {
	if strings.HasPrefix(addr, ":") {
		return "localhost" + addr
	}
	return addr
}
