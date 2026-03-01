package kv

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxValueBytes = 1 << 20
)

type Role string

const (
	RoleFollower  Role = "follower"
	RoleCandidate Role = "candidate"
	RoleLeader    Role = "leader"
)

type Config struct {
	NodeID            int
	Addr              string
	Peers             map[int]string
	ElectionTimeoutLo time.Duration
	ElectionTimeoutHi time.Duration
	HeartbeatInterval time.Duration
	HTTPTimeout       time.Duration
}

type Node struct {
	cfg Config

	mu sync.Mutex

	role      Role
	epoch     uint64
	votedFor  int
	leaderID  int
	lastHB    time.Time
	lastApply uint64
	commitIdx uint64

	log   []Entry
	store map[string][]byte

	httpClient *http.Client
	stopped    atomic.Bool
}

func NewNode(cfg Config) *Node {
	if cfg.ElectionTimeoutLo == 0 {
		cfg.ElectionTimeoutLo = 700 * time.Millisecond
	}
	if cfg.ElectionTimeoutHi == 0 {
		cfg.ElectionTimeoutHi = 1100 * time.Millisecond
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 120 * time.Millisecond
	}
	if cfg.HTTPTimeout == 0 {
		cfg.HTTPTimeout = 400 * time.Millisecond
	}
	return &Node{
		cfg:        cfg,
		role:       RoleFollower,
		votedFor:   -1,
		leaderID:   -1,
		lastHB:     time.Now(),
		store:      make(map[string][]byte),
		httpClient: &http.Client{Timeout: cfg.HTTPTimeout},
	}
}

func (n *Node) StartBackground(ctx context.Context) {
	go n.electionLoop(ctx)
	go n.heartbeatLoop(ctx)
}

func (n *Node) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/kv/"):
		n.handleGet(w, r)
	case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/kv/"):
		n.handlePut(w, r)
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/kv/"):
		n.handleDelete(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/healthz":
		n.handleHealth(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/internal/vote":
		n.handleVote(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/internal/append":
		n.handleAppend(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/internal/snapshot":
		n.handleSnapshot(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (n *Node) handleHealth(w http.ResponseWriter, _ *http.Request) {
	n.mu.Lock()
	resp := map[string]any{
		"node_id":      n.cfg.NodeID,
		"role":         n.role,
		"epoch":        n.epoch,
		"leader":       n.leaderID,
		"commit_index": n.commitIdx,
		"keys":         len(n.store),
	}
	n.mu.Unlock()
	writeJSON(w, http.StatusOK, resp)
}

func (n *Node) parseKey(path string) (string, error) {
	s := strings.TrimPrefix(path, "/kv/")
	if len(s) != 32 {
		return "", fmt.Errorf("key must be 16 bytes (32 hex chars)")
	}
	_, err := hex.DecodeString(s)
	if err != nil {
		return "", fmt.Errorf("invalid hex key")
	}
	return strings.ToLower(s), nil
}

func (n *Node) handleGet(w http.ResponseWriter, r *http.Request) {
	key, err := n.parseKey(r.URL.Path)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, GenericStatus{Status: "ERROR", Error: err.Error()})
		return
	}
	if !n.amLeader() {
		writeJSON(w, http.StatusTemporaryRedirect, GenericStatus{Status: "ERROR_NOT_LEADER", Leader: n.leaderAddr()})
		return
	}
	n.mu.Lock()
	v, ok := n.store[key]
	n.mu.Unlock()
	if !ok {
		writeJSON(w, http.StatusOK, GetResponse{Status: "NOT_FOUND"})
		return
	}
	writeJSON(w, http.StatusOK, GetResponse{Status: "FOUND", Value: cloneBytes(v)})
}

func (n *Node) handlePut(w http.ResponseWriter, r *http.Request) {
	key, err := n.parseKey(r.URL.Path)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, GenericStatus{Status: "ERROR", Error: err.Error()})
		return
	}
	if !n.amLeader() {
		writeJSON(w, http.StatusTemporaryRedirect, GenericStatus{Status: "ERROR_NOT_LEADER", Leader: n.leaderAddr()})
		return
	}
	var req PutRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, maxValueBytes+1024)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, GenericStatus{Status: "ERROR", Error: "invalid body"})
		return
	}
	if len(req.Value) > maxValueBytes {
		writeJSON(w, http.StatusBadRequest, GenericStatus{Status: "ERROR", Error: "value exceeds 1MiB"})
		return
	}
	if err := n.replicateAndCommit(Entry{Type: OpPut, Key: key, Value: cloneBytes(req.Value)}); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, GenericStatus{Status: "TIMEOUT", Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, GenericStatus{Status: "OK"})
}

func (n *Node) handleDelete(w http.ResponseWriter, r *http.Request) {
	key, err := n.parseKey(r.URL.Path)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, GenericStatus{Status: "ERROR", Error: err.Error()})
		return
	}
	if !n.amLeader() {
		writeJSON(w, http.StatusTemporaryRedirect, GenericStatus{Status: "ERROR_NOT_LEADER", Leader: n.leaderAddr()})
		return
	}
	if err := n.replicateAndCommit(Entry{Type: OpDelete, Key: key}); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, GenericStatus{Status: "TIMEOUT", Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, GenericStatus{Status: "OK"})
}

func (n *Node) handleVote(w http.ResponseWriter, r *http.Request) {
	var req VoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, GenericStatus{Status: "ERROR", Error: "bad vote request"})
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if req.Epoch < n.epoch {
		writeJSON(w, http.StatusOK, VoteResponse{Epoch: n.epoch, Granted: false})
		return
	}
	if req.Epoch > n.epoch {
		n.epoch = req.Epoch
		n.role = RoleFollower
		n.votedFor = -1
		n.leaderID = -1
	}
	myCommit := n.commitIdx
	if req.CandidateCommit < myCommit {
		writeJSON(w, http.StatusOK, VoteResponse{Epoch: n.epoch, Granted: false})
		return
	}
	if n.votedFor == -1 || n.votedFor == req.From {
		n.votedFor = req.From
		n.lastHB = time.Now()
		writeJSON(w, http.StatusOK, VoteResponse{Epoch: n.epoch, Granted: true})
		return
	}
	writeJSON(w, http.StatusOK, VoteResponse{Epoch: n.epoch, Granted: false})
}

func (n *Node) handleAppend(w http.ResponseWriter, r *http.Request) {
	var req AppendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, GenericStatus{Status: "ERROR", Error: "bad append request"})
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if req.Epoch < n.epoch {
		writeJSON(w, http.StatusOK, AppendResponse{Epoch: n.epoch, Success: false, LastIndex: uint64(len(n.log)), NeedResync: false})
		return
	}
	if req.Epoch >= n.epoch {
		n.epoch = req.Epoch
		n.role = RoleFollower
		n.votedFor = -1
		n.leaderID = req.From
		n.lastHB = time.Now()
	}
	if len(req.Entries) > 0 {
		for _, e := range req.Entries {
			expected := uint64(len(n.log) + 1)
			if e.Index != expected {
				writeJSON(w, http.StatusOK, AppendResponse{Epoch: n.epoch, Success: false, LastIndex: uint64(len(n.log)), NeedResync: true})
				return
			}
			n.log = append(n.log, e)
		}
	}
	if req.CommitIndex > n.commitIdx {
		upper := req.CommitIndex
		if upper > uint64(len(n.log)) {
			upper = uint64(len(n.log))
		}
		for i := n.lastApply; i < upper; i++ {
			n.applyLocked(n.log[i])
			n.lastApply = i + 1
		}
		n.commitIdx = upper
	}
	writeJSON(w, http.StatusOK, AppendResponse{Epoch: n.epoch, Success: true, LastIndex: uint64(len(n.log))})
}

func (n *Node) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	var req SnapshotRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, GenericStatus{Status: "ERROR", Error: "bad snapshot"})
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if req.Epoch < n.epoch {
		writeJSON(w, http.StatusConflict, GenericStatus{Status: "ERROR", Error: "stale epoch"})
		return
	}
	n.epoch = req.Epoch
	n.role = RoleFollower
	n.leaderID = req.From
	n.lastHB = time.Now()
	n.store = cloneMap(req.Store)
	n.log = deepCopyLog(req.Log)
	n.commitIdx = req.CommitIndex
	n.lastApply = req.CommitIndex
	writeJSON(w, http.StatusOK, GenericStatus{Status: "OK"})
}

func (n *Node) electionLoop(ctx context.Context) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(n.cfg.NodeID*9973)))
	for {
		if n.stopped.Load() {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(80 * time.Millisecond):
		}

		n.mu.Lock()
		role := n.role
		elapsed := time.Since(n.lastHB)
		lo := n.cfg.ElectionTimeoutLo.Milliseconds()
		hi := n.cfg.ElectionTimeoutHi.Milliseconds()
		deadline := time.Duration(lo+rng.Int63n(max64(1, hi-lo))) * time.Millisecond
		n.mu.Unlock()

		if role == RoleLeader {
			continue
		}
		if elapsed < deadline {
			continue
		}
		n.startElection(ctx)
	}
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (n *Node) startElection(ctx context.Context) {
	n.mu.Lock()
	if n.role == RoleLeader {
		n.mu.Unlock()
		return
	}
	n.role = RoleCandidate
	n.epoch++
	epoch := n.epoch
	n.votedFor = n.cfg.NodeID
	n.lastHB = time.Now()
	commit := n.commitIdx
	n.mu.Unlock()

	votes := int32(1)
	majority := n.quorum()
	var wg sync.WaitGroup
	for peerID, addr := range n.cfg.Peers {
		if peerID == n.cfg.NodeID {
			continue
		}
		wg.Add(1)
		go func(id int, peerAddr string) {
			defer wg.Done()
			req := VoteRequest{From: n.cfg.NodeID, Epoch: epoch, CandidateCommit: commit}
			var resp VoteResponse
			if err := n.postJSON(ctx, peerAddr+"/internal/vote", req, &resp); err != nil {
				return
			}
			n.mu.Lock()
			if resp.Epoch > n.epoch {
				n.epoch = resp.Epoch
				n.role = RoleFollower
				n.votedFor = -1
				n.leaderID = -1
			}
			n.mu.Unlock()
			if resp.Granted {
				atomic.AddInt32(&votes, 1)
			}
		}(peerID, addr)
	}
	wg.Wait()

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.epoch != epoch || n.role != RoleCandidate {
		return
	}
	if int(atomic.LoadInt32(&votes)) >= majority {
		n.role = RoleLeader
		n.leaderID = n.cfg.NodeID
		n.votedFor = -1
		n.lastHB = time.Now()
		log.Printf("node %d became leader epoch=%d", n.cfg.NodeID, n.epoch)
	} else {
		n.role = RoleFollower
	}
}

func (n *Node) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(n.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		if n.stopped.Load() {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !n.amLeader() {
				continue
			}
			n.sendHeartbeats(ctx)
		}
	}
}

func (n *Node) sendHeartbeats(ctx context.Context) {
	n.mu.Lock()
	req := AppendRequest{From: n.cfg.NodeID, Epoch: n.epoch, CommitIndex: n.commitIdx, Heartbeat: true}
	n.mu.Unlock()
	for peerID, addr := range n.cfg.Peers {
		if peerID == n.cfg.NodeID {
			continue
		}
		go func(id int, peerAddr string) {
			var resp AppendResponse
			if err := n.postJSON(ctx, peerAddr+"/internal/append", req, &resp); err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if resp.Epoch > n.epoch {
				n.epoch = resp.Epoch
				n.role = RoleFollower
				n.leaderID = -1
				n.votedFor = -1
				return
			}
			if resp.NeedResync {
				go n.syncSnapshot(context.Background(), id, peerAddr)
			}
		}(peerID, addr)
	}
}

func (n *Node) replicateAndCommit(base Entry) error {
	n.mu.Lock()
	if n.role != RoleLeader {
		n.mu.Unlock()
		return fmt.Errorf("not leader")
	}
	entry := base
	entry.Epoch = n.epoch
	entry.Index = uint64(len(n.log) + 1)
	n.log = append(n.log, entry)
	epoch := n.epoch
	commitCandidate := entry.Index
	commitSoFar := n.commitIdx
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
	defer cancel()

	acks := int32(1)
	majority := n.quorum()
	var wg sync.WaitGroup
	for peerID, addr := range n.cfg.Peers {
		if peerID == n.cfg.NodeID {
			continue
		}
		wg.Add(1)
		go func(id int, peerAddr string) {
			defer wg.Done()
			req := AppendRequest{From: n.cfg.NodeID, Epoch: epoch, Entries: []Entry{entry}, CommitIndex: commitSoFar}
			var resp AppendResponse
			if err := n.postJSON(ctx, peerAddr+"/internal/append", req, &resp); err != nil {
				return
			}
			if resp.Success {
				atomic.AddInt32(&acks, 1)
				return
			}
			if resp.NeedResync {
				n.syncSnapshot(context.Background(), id, peerAddr)
				var retry AppendResponse
				_ = n.postJSON(ctx, peerAddr+"/internal/append", req, &retry)
				if retry.Success {
					atomic.AddInt32(&acks, 1)
				}
			}
		}(peerID, addr)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("replication timeout")
	case <-done:
	}

	if int(atomic.LoadInt32(&acks)) < majority {
		return fmt.Errorf("quorum unavailable")
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != RoleLeader {
		return fmt.Errorf("leadership changed")
	}
	if n.commitIdx < commitCandidate {
		n.commitIdx = commitCandidate
	}
	for n.lastApply < n.commitIdx {
		n.applyLocked(n.log[n.lastApply])
		n.lastApply++
	}
	return nil
}

func (n *Node) syncSnapshot(ctx context.Context, peerID int, addr string) {
	n.mu.Lock()
	if n.role != RoleLeader {
		n.mu.Unlock()
		return
	}
	req := SnapshotRequest{
		From:        n.cfg.NodeID,
		Epoch:       n.epoch,
		CommitIndex: n.commitIdx,
		Store:       cloneMap(n.store),
		Log:         deepCopyLog(n.log),
	}
	n.mu.Unlock()
	var resp GenericStatus
	if err := n.postJSON(ctx, addr+"/internal/snapshot", req, &resp); err != nil {
		log.Printf("snapshot sync to node %d failed: %v", peerID, err)
	}
}

func (n *Node) applyLocked(e Entry) {
	switch e.Type {
	case OpPut:
		n.store[e.Key] = cloneBytes(e.Value)
	case OpDelete:
		delete(n.store, e.Key)
	}
}

func (n *Node) postJSON(ctx context.Context, url string, reqBody, respBody any) error {
	payload, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := n.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return fmt.Errorf("http %d: %s", resp.StatusCode, string(b))
	}
	if respBody == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(respBody)
}

func (n *Node) amLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == RoleLeader
}

func (n *Node) quorum() int {
	return len(n.cfg.Peers)/2 + 1
}

func (n *Node) leaderAddr() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.leaderID >= 0 {
		if a, ok := n.cfg.Peers[n.leaderID]; ok {
			return a
		}
	}
	return ""
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(jsonEncode(v))
}

func ParsePeers(peers string) (map[int]string, error) {
	out := map[int]string{}
	if strings.TrimSpace(peers) == "" {
		return nil, fmt.Errorf("empty peers")
	}
	parts := strings.Split(peers, ",")
	for _, p := range parts {
		pair := strings.Split(strings.TrimSpace(p), "=")
		if len(pair) != 2 {
			return nil, fmt.Errorf("invalid peer entry: %s", p)
		}
		id, err := strconv.Atoi(pair[0])
		if err != nil {
			return nil, fmt.Errorf("invalid peer id: %s", pair[0])
		}
		out[id] = pair[1]
	}
	return out, nil
}
