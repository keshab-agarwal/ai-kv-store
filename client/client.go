// Package client provides an embeddable HTTP client library for the distributed KV store.
// It handles leader discovery, request routing, redirect following, and at-most-once
// semantics for writes via clientID + sequence number deduplication.
package client

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"ai-kv-store/kvstore"
)

// Config holds the configuration for creating a new Client.
type Config struct {
	// Nodes is the list of HTTP addresses for all known cluster nodes,
	// e.g. ["localhost:16001", "localhost:16002", "localhost:16003"].
	Nodes []string

	// Timeout is the per-operation HTTP timeout. Defaults to 5 seconds.
	Timeout time.Duration
}

// Client routes KV requests to the current cluster leader.
// It discovers the leader lazily on first use and follows redirect responses
// to stay pointed at the primary. On failure it cycles through all known nodes.
type Client struct {
	nodes    []string
	leader   string
	mu       sync.RWMutex
	clientID string
	seq      uint64
	timeout  time.Duration
	http     *http.Client
}

// getRequest is the JSON payload for POST /api/get.
type getRequest struct {
	Key string `json:"key"`
}

// getResponse is the JSON response from POST /api/get.
type getResponse struct {
	Status string `json:"status"`
	Value  string `json:"value"`
	Leader string `json:"leader"`
}

// putRequest is the JSON payload for POST /api/put.
type putRequest struct {
	Key      string `json:"key"`
	Value    string `json:"value"`
	ClientID string `json:"client_id"`
	ClientSeq uint64 `json:"client_seq"`
}

// putResponse is the JSON response from POST /api/put.
type putResponse struct {
	Status string `json:"status"`
	Leader string `json:"leader"`
}

// deleteRequest is the JSON payload for POST /api/delete.
type deleteRequest struct {
	Key      string `json:"key"`
	ClientID string `json:"client_id"`
	ClientSeq uint64 `json:"client_seq"`
}

// deleteResponse is the JSON response from POST /api/delete.
type deleteResponse struct {
	Status string `json:"status"`
	Leader string `json:"leader"`
}

// New creates a new Client using the given configuration.
// clientID is generated using crypto/rand for uniqueness across restarts.
func New(cfg Config) *Client {
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}

	// Generate a random clientID using crypto/rand.
	idBytes := make([]byte, 16)
	if _, err := rand.Read(idBytes); err != nil {
		// Fallback: use time-based ID if crypto/rand fails.
		now := time.Now().UnixNano()
		for i := 0; i < 8; i++ {
			idBytes[i] = byte(now >> (8 * i))
		}
	}
	clientID := hex.EncodeToString(idBytes)

	return &Client{
		nodes:    cfg.Nodes,
		leader:   "",
		clientID: clientID,
		seq:      0,
		timeout:  timeout,
		http: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 16,
			},
		},
	}
}

// nextSeq atomically increments and returns the next sequence number.
// This is called once per new Put/Delete operation (not on retries).
func (c *Client) nextSeq() uint64 {
	return atomic.AddUint64(&c.seq, 1)
}

// getLeader returns the current known leader address, or empty string if unknown.
func (c *Client) getLeader() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.leader
}

// setLeader updates the known leader address.
func (c *Client) setLeader(addr string) {
	if addr == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.leader = addr
}

// doRequest sends a JSON POST (or GET) to the given path on the best-known node.
// It tries the current leader first, then cycles through all nodes on failure.
// Returns the raw response body bytes and the responding node's address.
func (c *Client) doRequest(method, path string, body interface{}) ([]byte, error) {
	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal request: %w", err)
		}
	}

	// Build the ordered list of nodes to try: leader first, then all others.
	leader := c.getLeader()
	var candidates []string
	if leader != "" {
		candidates = append(candidates, leader)
	}
	for _, n := range c.nodes {
		if n != leader {
			candidates = append(candidates, n)
		}
	}

	var lastErr error
	for _, node := range candidates {
		respBytes, respondingNode, err := c.sendToNode(method, node, path, bodyBytes)
		if err != nil {
			lastErr = err
			continue
		}
		// Successful contact — update leader.
		c.setLeader(respondingNode)
		return respBytes, nil
	}

	if lastErr != nil {
		return nil, fmt.Errorf("all nodes failed, last error: %w", lastErr)
	}
	return nil, fmt.Errorf("no nodes available")
}

// sendToNode sends an HTTP request to a single node and returns the response body.
// It follows at most one layer of redirect (the server-side redirect, not HTTP redirects).
func (c *Client) sendToNode(method, node, path string, bodyBytes []byte) ([]byte, string, error) {
	url := "http://" + node + path

	var reqBody io.Reader
	if bodyBytes != nil {
		reqBody = bytes.NewReader(bodyBytes)
	}

	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		return nil, "", fmt.Errorf("create request to %s: %w", node, err)
	}
	if bodyBytes != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("http request to %s: %w", node, err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("read response from %s: %w", node, err)
	}

	return data, node, nil
}

// Get retrieves the value for a 16-byte key.
// key must be exactly 16 bytes; it is hex-encoded to a 32-char string for the API.
// Returns StatusFound + value on success, StatusNotFound if missing.
func (c *Client) Get(key []byte) (kvstore.Status, []byte, error) {
	if len(key) != 16 {
		return kvstore.StatusError, nil, fmt.Errorf("key must be 16 bytes, got %d", len(key))
	}
	hexKey := hex.EncodeToString(key)

	req := getRequest{Key: hexKey}

	// Retry loop: follow redirects and handle timeouts.
	const maxRedirects = 10
	for i := 0; i < maxRedirects; i++ {
		rawResp, err := c.doRequest(http.MethodPost, "/api/get", req)
		if err != nil {
			return kvstore.StatusError, nil, fmt.Errorf("get request failed: %w", err)
		}

		var resp getResponse
		if err := json.Unmarshal(rawResp, &resp); err != nil {
			return kvstore.StatusError, nil, fmt.Errorf("unmarshal get response: %w", err)
		}

		switch resp.Status {
		case "found":
			val, err := base64.StdEncoding.DecodeString(resp.Value)
			if err != nil {
				return kvstore.StatusError, nil, fmt.Errorf("decode value: %w", err)
			}
			return kvstore.StatusFound, val, nil

		case "not_found":
			return kvstore.StatusNotFound, nil, nil

		case "redirect":
			if resp.Leader != "" {
				c.setLeader(resp.Leader)
			}
			continue

		case "timeout":
			return kvstore.StatusTimeout, nil, nil

		case "error":
			return kvstore.StatusError, nil, fmt.Errorf("server error")

		default:
			return kvstore.StatusError, nil, fmt.Errorf("unknown status: %s", resp.Status)
		}
	}

	return kvstore.StatusError, nil, fmt.Errorf("too many redirects")
}

// Put stores a key-value pair. key must be 16 bytes, value up to 1 MiB.
// Implements at-most-once semantics via clientID+seq deduplication.
// On timeout, retries up to 3 times reusing the same seq number.
func (c *Client) Put(key, value []byte) (kvstore.Status, error) {
	if len(key) != 16 {
		return kvstore.StatusError, fmt.Errorf("key must be 16 bytes, got %d", len(key))
	}
	hexKey := hex.EncodeToString(key)
	b64Value := base64.StdEncoding.EncodeToString(value)

	// Assign a new sequence number for this operation.
	seq := c.nextSeq()

	req := putRequest{
		Key:       hexKey,
		Value:     b64Value,
		ClientID:  c.clientID,
		ClientSeq: seq,
	}

	const maxRetries = 3
	const maxRedirects = 10

	for attempt := 0; attempt < maxRetries; attempt++ {
		// Inner redirect loop.
		for redirect := 0; redirect < maxRedirects; redirect++ {
			rawResp, err := c.doRequest(http.MethodPost, "/api/put", req)
			if err != nil {
				// Connection error — break inner loop, will retry.
				break
			}

			var resp putResponse
			if err := json.Unmarshal(rawResp, &resp); err != nil {
				return kvstore.StatusError, fmt.Errorf("unmarshal put response: %w", err)
			}

			switch resp.Status {
			case "ok":
				return kvstore.StatusOK, nil

			case "redirect":
				if resp.Leader != "" {
					c.setLeader(resp.Leader)
				}
				continue

			case "timeout":
				// Timeout is uncertain — retry with same seq (idempotent).
				goto nextAttempt

			case "error":
				return kvstore.StatusError, fmt.Errorf("server error on put")

			default:
				return kvstore.StatusError, fmt.Errorf("unknown put status: %s", resp.Status)
			}
		}
	nextAttempt:
	}

	return kvstore.StatusTimeout, nil
}

// Delete removes a key from the store. Idempotent.
// On timeout, retries up to 3 times reusing the same seq number.
func (c *Client) Delete(key []byte) (kvstore.Status, error) {
	if len(key) != 16 {
		return kvstore.StatusError, fmt.Errorf("key must be 16 bytes, got %d", len(key))
	}
	hexKey := hex.EncodeToString(key)

	// Assign a new sequence number for this operation.
	seq := c.nextSeq()

	req := deleteRequest{
		Key:       hexKey,
		ClientID:  c.clientID,
		ClientSeq: seq,
	}

	const maxRetries = 3
	const maxRedirects = 10

	for attempt := 0; attempt < maxRetries; attempt++ {
		for redirect := 0; redirect < maxRedirects; redirect++ {
			rawResp, err := c.doRequest(http.MethodPost, "/api/delete", req)
			if err != nil {
				break
			}

			var resp deleteResponse
			if err := json.Unmarshal(rawResp, &resp); err != nil {
				return kvstore.StatusError, fmt.Errorf("unmarshal delete response: %w", err)
			}

			switch resp.Status {
			case "ok":
				return kvstore.StatusOK, nil

			case "redirect":
				if resp.Leader != "" {
					c.setLeader(resp.Leader)
				}
				continue

			case "timeout":
				// Timeout is uncertain — retry with same seq (idempotent).
				goto nextDeleteAttempt

			case "error":
				return kvstore.StatusError, fmt.Errorf("server error on delete")

			default:
				return kvstore.StatusError, fmt.Errorf("unknown delete status: %s", resp.Status)
			}
		}
	nextDeleteAttempt:
	}

	return kvstore.StatusTimeout, nil
}

// Close is a no-op. Connections are managed by the underlying http.Client
// with keep-alives handled automatically.
func (c *Client) Close() error {
	return nil
}
