package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"ai-kv-store/shared/types"
)

// dedupEntry caches a write result for client deduplication.
type dedupEntry struct {
	SeqNum uint64
	Result writeResult
}

type writeResult struct {
	Status string `json:"status"`
}

// Node is the main KV store server node.
type Node struct {
	mu sync.RWMutex

	config      types.ClusterConfig
	store       *ShardedMap
	log         *ReplicatedLog
	coordinator *Coordinator
	replicator  *Replicator

	// Deduplication table: clientID -> last completed entry
	dedupMu    sync.RWMutex
	dedupTable map[string]dedupEntry

	// Apply state
	applyMu    sync.Mutex
	applyCond  *sync.Cond

	// Peer state tracking: peerID -> lastKnownApplied
	peerApplied sync.Map

	// HTTP server
	httpServer *http.Server

	// Logger
	logger *log.Logger

	// Shutdown
	stopC    chan struct{}
	stoppedC chan struct{}
}

// NewNode creates a new KV store node.
func NewNode(config types.ClusterConfig) *Node {
	logger := log.New(os.Stderr, "", 0)

	n := &Node{
		config:     config,
		store:      NewShardedMap(),
		log:        NewReplicatedLog(),
		dedupTable: make(map[string]dedupEntry),
		logger:     logger,
		stopC:      make(chan struct{}),
		stoppedC:   make(chan struct{}),
	}
	n.applyCond = sync.NewCond(&n.applyMu)

	logFunc := func(msg string, fields map[string]interface{}) {
		if fields == nil {
			fields = make(map[string]interface{})
		}
		fields["node_id"] = config.NodeID
		fields["msg"] = msg
		data, _ := json.Marshal(fields)
		logger.Println(string(data))
	}

	n.coordinator = NewCoordinator(config.NodeID, config.Peers, config, logFunc)
	n.replicator = NewReplicator(config.NodeID, config.Peers, time.Duration(config.RPCTimeoutMs)*time.Millisecond)

	n.coordinator.SetCallbacks(
		func() { // onBecomeCoordinator
			n.onBecomeCoordinator()
		},
		func(epoch uint64) { // onBecomeFollower
			n.logJSON("stepped down to follower", map[string]interface{}{"epoch": epoch})
		},
		func(peerID int, appliedIndex uint64) { // onHeartbeatResp
			n.peerApplied.Store(peerID, appliedIndex)
			// If peer is behind, send missing entries
			go n.syncPeerIfNeeded(peerID, appliedIndex)
		},
	)

	return n
}

func (n *Node) logJSON(msg string, fields map[string]interface{}) {
	if fields == nil {
		fields = make(map[string]interface{})
	}
	fields["node_id"] = n.config.NodeID
	fields["msg"] = msg
	data, _ := json.Marshal(fields)
	n.logger.Println(string(data))
}

func (n *Node) onBecomeCoordinator() {
	// Append a no-op entry to commit at start of epoch
	epoch := n.coordinator.Epoch()
	entry := types.LogEntry{
		Epoch:    epoch,
		Index:    n.log.LastIndex() + 1,
		Op:       0, // no-op
		ClientID: "",
	}
	n.log.Append(entry)

	// Start heartbeat loop
	n.coordinator.StartHeartbeatLoop()

	// Replicate no-op to establish quorum
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(n.config.WriteTimeoutMs)*time.Millisecond)
		defer cancel()
		success := n.replicator.ReplicateToQuorum(ctx, epoch, []types.LogEntry{entry}, entry.Index)
		if success {
			n.coordinator.SetCommitIndex(entry.Index)
			n.applyCommitted()
		}
	}()
}

// Start begins the node server.
func (n *Node) Start() error {
	mux := http.NewServeMux()

	// Client API
	mux.HandleFunc("/api/put", n.handlePut)
	mux.HandleFunc("/api/get", n.handleGet)
	mux.HandleFunc("/api/delete", n.handleDelete)
	mux.HandleFunc("/api/status", n.handleStatus)

	// Internal API
	mux.HandleFunc("/internal/replicate", n.handleReplicate)
	mux.HandleFunc("/internal/heartbeat", n.handleHeartbeat)
	mux.HandleFunc("/internal/vote", n.handleVote)
	mux.HandleFunc("/internal/sync", n.handleSync)

	addr := n.config.Peers[n.config.NodeID]
	n.httpServer = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	// Start apply loop
	go n.applyLoop()

	// Start coordinator (election timer, etc.)
	n.coordinator.Start()

	n.logJSON("node starting", map[string]interface{}{
		"addr":  addr,
		"peers": n.config.Peers,
	})

	// Start HTTP server
	go func() {
		if err := n.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			n.logJSON("http server error", map[string]interface{}{"error": err.Error()})
		}
	}()

	return nil
}

// Stop gracefully shuts down the node.
func (n *Node) Stop() {
	close(n.stopC)
	n.coordinator.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	n.httpServer.Shutdown(ctx)
	// Wake up apply loop
	n.applyCond.Broadcast()
}

// ---- Apply loop ----

func (n *Node) applyLoop() {
	for {
		select {
		case <-n.stopC:
			return
		default:
		}

		n.applyCommitted()

		// Wait for notification or periodic check
		select {
		case <-n.stopC:
			return
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (n *Node) applyCommitted() {
	commitIdx := n.coordinator.CommitIndex()
	appliedIdx := n.coordinator.AppliedIndex()

	for appliedIdx < commitIdx {
		nextIdx := appliedIdx + 1
		entry := n.log.Get(nextIdx)
		if entry == nil {
			break
		}
		n.applyEntry(entry)
		appliedIdx = nextIdx
		n.coordinator.SetAppliedIndex(nextIdx)
	}
}

func (n *Node) applyEntry(entry *types.LogEntry) {
	switch entry.Op {
	case types.OpPut:
		n.store.Put(entry.Key, entry.Value)
		n.updateDedup(entry.ClientID, entry.ClientSeq, writeResult{Status: "ok"})
	case types.OpDelete:
		n.store.Delete(entry.Key)
		n.updateDedup(entry.ClientID, entry.ClientSeq, writeResult{Status: "ok"})
	default:
		// no-op, ignore
	}
}

func (n *Node) updateDedup(clientID string, seq uint64, result writeResult) {
	if clientID == "" {
		return
	}
	n.dedupMu.Lock()
	existing, ok := n.dedupTable[clientID]
	if !ok || seq > existing.SeqNum {
		n.dedupTable[clientID] = dedupEntry{SeqNum: seq, Result: result}
	}
	n.dedupMu.Unlock()
}

func (n *Node) checkDedup(clientID string, seq uint64) (writeResult, bool) {
	if clientID == "" {
		return writeResult{}, false
	}
	n.dedupMu.RLock()
	entry, ok := n.dedupTable[clientID]
	n.dedupMu.RUnlock()
	if ok && entry.SeqNum >= seq {
		return entry.Result, true
	}
	return writeResult{}, false
}

// ---- Sync peer ----

func (n *Node) syncPeerIfNeeded(peerID int, peerApplied uint64) {
	commitIdx := n.coordinator.CommitIndex()
	if peerApplied >= commitIdx {
		return
	}

	// Send missing entries
	entries := n.log.EntriesFrom(peerApplied + 1)
	if len(entries) == 0 {
		return
	}

	epoch := n.coordinator.Epoch()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(n.config.RPCTimeoutMs)*time.Millisecond*2)
	defer cancel()

	peerAddr := n.config.Peers[peerID]
	n.replicator.SendEntries(ctx, peerAddr, epoch, entries, commitIdx)
}

// ---- HTTP Handlers: Client API ----

type putRequest struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	ClientID  string `json:"client_id"`
	ClientSeq uint64 `json:"client_seq"`
}

type putResponse struct {
	Status string `json:"status"`
}

func (n *Node) handlePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req putRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, putResponse{Status: "error"})
		return
	}

	key, err := types.KeyFromHex(req.Key)
	if err != nil {
		writeJSON(w, putResponse{Status: "error"})
		return
	}

	value, err := base64.StdEncoding.DecodeString(req.Value)
	if err != nil {
		writeJSON(w, putResponse{Status: "error"})
		return
	}

	// Check dedup
	if result, dup := n.checkDedup(req.ClientID, req.ClientSeq); dup {
		writeJSON(w, putResponse{Status: result.Status})
		return
	}

	// Must be coordinator to handle writes
	role := n.coordinator.Role()
	if role != types.RoleCoordinator {
		// Forward to coordinator - re-marshal since body was consumed
		fwdBody, _ := json.Marshal(req)
		resp, err := n.forwardToCoordinator(r.Context(), "/api/put", io.NopCloser(bytes.NewReader(fwdBody)))
		if err != nil {
			writeJSON(w, putResponse{Status: "error"})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(resp)
		return
	}

	// Process write as coordinator
	result := n.processWrite(r.Context(), types.OpPut, key, value, req.ClientID, req.ClientSeq)
	writeJSON(w, putResponse{Status: result})
}

type getRequest struct {
	Key string `json:"key"`
}

type getResponse struct {
	Status string `json:"status"`
	Value  string `json:"value,omitempty"`
}

func (n *Node) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req getRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, getResponse{Status: "error"})
		return
	}

	key, err := types.KeyFromHex(req.Key)
	if err != nil {
		writeJSON(w, getResponse{Status: "error"})
		return
	}

	// Reads served from coordinator with valid lease
	role := n.coordinator.Role()
	if role != types.RoleCoordinator {
		// Forward to coordinator
		bodyBytes, _ := json.Marshal(req)
		resp, err := n.forwardToCoordinator(r.Context(), "/api/get", io.NopCloser(bytes.NewReader(bodyBytes)))
		if err != nil {
			writeJSON(w, getResponse{Status: "error"})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(resp)
		return
	}

	if !n.coordinator.CheckLease() {
		writeJSON(w, getResponse{Status: "error"})
		return
	}

	val, found := n.store.Get(key)
	if found {
		writeJSON(w, getResponse{
			Status: "found",
			Value:  base64.StdEncoding.EncodeToString(val),
		})
	} else {
		writeJSON(w, getResponse{Status: "not_found"})
	}
}

type deleteRequest struct {
	Key       string `json:"key"`
	ClientID  string `json:"client_id"`
	ClientSeq uint64 `json:"client_seq"`
}

type deleteResponse struct {
	Status string `json:"status"`
}

func (n *Node) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req deleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, deleteResponse{Status: "error"})
		return
	}

	key, err := types.KeyFromHex(req.Key)
	if err != nil {
		writeJSON(w, deleteResponse{Status: "error"})
		return
	}

	// Check dedup
	if result, dup := n.checkDedup(req.ClientID, req.ClientSeq); dup {
		writeJSON(w, deleteResponse{Status: result.Status})
		return
	}

	// Must be coordinator
	role := n.coordinator.Role()
	if role != types.RoleCoordinator {
		fwdBody, _ := json.Marshal(req)
		resp, err := n.forwardToCoordinator(r.Context(), "/api/delete", io.NopCloser(bytes.NewReader(fwdBody)))
		if err != nil {
			writeJSON(w, deleteResponse{Status: "error"})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(resp)
		return
	}

	result := n.processWrite(r.Context(), types.OpDelete, key, nil, req.ClientID, req.ClientSeq)
	writeJSON(w, deleteResponse{Status: result})
}

type statusResponse struct {
	NodeID       int      `json:"node_id"`
	Role         string   `json:"role"`
	Epoch        uint64   `json:"epoch"`
	AlivePeers   []int    `json:"alive_peers"`
	AppliedIndex uint64   `json:"applied_index"`
	CommitIndex  uint64   `json:"commit_index"`
	LogLength    uint64   `json:"log_length"`
	StoreSize    int64    `json:"store_size"`
}

func (n *Node) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	alivePeers := make([]int, 0)
	for i := range n.config.Peers {
		if i == n.config.NodeID {
			continue
		}
		if _, ok := n.peerApplied.Load(i); ok {
			alivePeers = append(alivePeers, i)
		}
	}

	resp := statusResponse{
		NodeID:       n.config.NodeID,
		Role:         n.coordinator.Role().String(),
		Epoch:        n.coordinator.Epoch(),
		AlivePeers:   alivePeers,
		AppliedIndex: n.coordinator.AppliedIndex(),
		CommitIndex:  n.coordinator.CommitIndex(),
		LogLength:    n.log.LastIndex(),
		StoreSize:    n.store.Size(),
	}

	writeJSON(w, resp)
}

// ---- HTTP Handlers: Internal API ----

func (n *Node) handleReplicate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ReplicateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, ReplicateResponse{Success: false})
		return
	}

	currentEpoch := n.coordinator.Epoch()
	currentRole := n.coordinator.Role()

	// Only reject replication if we are the coordinator at a strictly higher epoch.
	// A follower or candidate should accept replication even if its epoch is
	// slightly higher (from a failed election attempt).
	if req.Epoch < currentEpoch && currentRole == types.RoleCoordinator {
		writeJSON(w, ReplicateResponse{Epoch: currentEpoch, Success: false, AppliedIndex: n.coordinator.AppliedIndex()})
		return
	}

	// Accept replication: step down if needed
	if currentRole != types.RoleFollower {
		n.coordinator.StepDown(req.Epoch)
	}

	// Append entries to log
	for _, entry := range req.Entries {
		existing := n.log.Get(entry.Index)
		if existing == nil {
			n.log.Append(entry)
		}
		// If entry already exists at this index, skip (idempotent)
	}

	// Update commit index
	if req.CommitIndex > n.coordinator.CommitIndex() {
		n.coordinator.SetCommitIndex(req.CommitIndex)
	}

	// Apply committed entries
	n.applyCommitted()

	// Reset election timer
	n.coordinator.ResetElectionTimer()

	writeJSON(w, ReplicateResponse{
		Epoch:        n.coordinator.Epoch(),
		Success:      true,
		AppliedIndex: n.coordinator.AppliedIndex(),
	})
}

func (n *Node) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req HeartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	resp := n.coordinator.HandleHeartbeat(req)

	// Apply any newly committed entries
	n.applyCommitted()

	writeJSON(w, resp)
}

func (n *Node) handleVote(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req VoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	resp := n.coordinator.HandleVoteRequest(req)
	writeJSON(w, resp)
}

func (n *Node) handleSync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req SyncRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	entries := n.log.EntriesFrom(req.FromIndex)

	// If entries are too far behind or missing, send snapshot
	if req.FromIndex > 0 && entries == nil && n.log.LastIndex() > 0 {
		snapshot := n.store.Snapshot()
		snapEntries := make([]SnapshotEntry, 0, len(snapshot))
		for k, v := range snapshot {
			snapEntries = append(snapEntries, SnapshotEntry{
				Key:   k.String(),
				Value: v,
			})
		}
		writeJSON(w, SyncResponse{Snapshot: snapEntries})
		return
	}

	writeJSON(w, SyncResponse{Entries: entries})
}

// ---- Write processing ----

func (n *Node) processWrite(ctx context.Context, op types.OpType, key types.Key, value []byte, clientID string, clientSeq uint64) string {
	// Check dedup again under lock
	if result, dup := n.checkDedup(clientID, clientSeq); dup {
		return result.Status
	}

	if !n.coordinator.CheckLease() {
		return "error"
	}

	epoch := n.coordinator.Epoch()
	nextIndex := n.log.LastIndex() + 1

	entry := types.LogEntry{
		Epoch:     epoch,
		Index:     nextIndex,
		Op:        op,
		Key:       key,
		Value:     value,
		ClientID:  clientID,
		ClientSeq: clientSeq,
	}

	// Append to own log first
	n.log.Append(entry)

	// Replicate to quorum
	writeCtx, cancel := context.WithTimeout(ctx, time.Duration(n.config.WriteTimeoutMs)*time.Millisecond)
	defer cancel()

	success := n.replicator.ReplicateToQuorum(writeCtx, epoch, []types.LogEntry{entry}, nextIndex)
	if !success {
		return "timeout"
	}

	// Commit
	n.coordinator.SetCommitIndex(nextIndex)
	n.applyCommitted()

	return "ok"
}

// ---- Forwarding ----

func (n *Node) forwardToCoordinator(ctx context.Context, path string, body io.ReadCloser) ([]byte, error) {
	coordID := n.coordinator.CoordinatorID()
	if coordID < 0 || coordID >= len(n.config.Peers) {
		return nil, fmt.Errorf("no known coordinator")
	}

	coordAddr := n.config.Peers[coordID]

	// Read original body
	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = io.ReadAll(body)
		if err != nil {
			return nil, fmt.Errorf("read body: %w", err)
		}
	}

	url := fmt.Sprintf("http://%s%s", coordAddr, path)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: time.Duration(n.config.WriteTimeoutMs) * time.Millisecond}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("forward: %w", err)
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

// ---- Helpers ----

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
