package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"ai-kv-store/shared/types"
)

// ReplicateRequest is sent from coordinator to followers.
type ReplicateRequest struct {
	Epoch       uint64           `json:"epoch"`
	Entries     []types.LogEntry `json:"entries"`
	CommitIndex uint64           `json:"commit_index"`
}

// ReplicateResponse is the follower's reply.
type ReplicateResponse struct {
	Epoch        uint64 `json:"epoch"`
	Success      bool   `json:"success"`
	AppliedIndex uint64 `json:"applied_index"`
}

// Replicator handles sending log entries to peers.
type Replicator struct {
	nodeID     int
	peers      []string
	httpClient *http.Client
	rpcTimeout time.Duration
}

// NewReplicator creates a new replicator.
func NewReplicator(nodeID int, peers []string, rpcTimeout time.Duration) *Replicator {
	return &Replicator{
		nodeID: nodeID,
		peers:  peers,
		httpClient: &http.Client{
			Timeout: rpcTimeout,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 4,
				MaxIdleConns:        20,
				IdleConnTimeout:     60 * time.Second,
			},
		},
		rpcTimeout: rpcTimeout,
	}
}

// SendEntries sends entries to a single peer. Returns success and the peer's applied index.
func (r *Replicator) SendEntries(ctx context.Context, peerAddr string, epoch uint64, entries []types.LogEntry, commitIndex uint64) (bool, uint64, error) {
	req := ReplicateRequest{
		Epoch:       epoch,
		Entries:     entries,
		CommitIndex: commitIndex,
	}
	body, err := json.Marshal(req)
	if err != nil {
		return false, 0, fmt.Errorf("marshal: %w", err)
	}

	url := fmt.Sprintf("http://%s/internal/replicate", peerAddr)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return false, 0, fmt.Errorf("new request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(httpReq)
	if err != nil {
		return false, 0, fmt.Errorf("http do: %w", err)
	}
	defer resp.Body.Close()

	var replResp ReplicateResponse
	if err := json.NewDecoder(resp.Body).Decode(&replResp); err != nil {
		return false, 0, fmt.Errorf("decode: %w", err)
	}

	return replResp.Success, replResp.AppliedIndex, nil
}

// ReplicateToQuorum sends entries to all peers and waits for majority acks.
// Returns true if a majority (including self) acknowledged.
func (r *Replicator) ReplicateToQuorum(ctx context.Context, epoch uint64, entries []types.LogEntry, commitIndex uint64) bool {
	totalNodes := len(r.peers)
	majority := totalNodes/2 + 1

	// Self counts as one ack
	acks := 1
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i, peer := range r.peers {
		if i == r.nodeID {
			continue
		}
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			success, _, err := r.SendEntries(ctx, addr, epoch, entries, commitIndex)
			if err == nil && success {
				mu.Lock()
				acks++
				mu.Unlock()
			}
		}(peer)
	}

	wg.Wait()

	mu.Lock()
	result := acks >= majority
	mu.Unlock()
	return result
}
