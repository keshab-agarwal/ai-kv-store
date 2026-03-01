// Package client provides an HTTP client for the distributed KV store.
package client

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"ai-kv-store/shared/types"
)

// StatusResp is the response from the /api/status endpoint.
type StatusResp struct {
	NodeID       int    `json:"node_id"`
	Role         string `json:"role"`
	Epoch        uint64 `json:"epoch"`
	AlivePeers   []int  `json:"alive_peers"`
	AppliedIndex uint64 `json:"applied_index"`
	CommitIndex  uint64 `json:"commit_index"`
	LogLength    uint64 `json:"log_length"`
	StoreSize    int64  `json:"store_size"`
}

// Client communicates with the KV store over its HTTP API.
type Client struct {
	addr       string
	httpClient *http.Client
	clientID   string
	clientSeq  atomic.Uint64
}

// New creates a new Client that connects to the given address (host:port).
func New(addr string, timeout time.Duration) *Client {
	return &Client{
		addr: addr,
		httpClient: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 4,
				MaxIdleConns:        20,
				IdleConnTimeout:     60 * time.Second,
			},
		},
		clientID: fmt.Sprintf("client-%d", time.Now().UnixNano()),
	}
}

// NewClient is an alias for New.
func NewClient(addr string, timeout time.Duration) *Client {
	return New(addr, timeout)
}

// NewClientWithID creates a new Client with a specified client ID.
func NewClientWithID(addr string, timeout time.Duration, clientID string) *Client {
	c := New(addr, timeout)
	c.clientID = clientID
	return c
}

func (c *Client) nextSeq() uint64 {
	return c.clientSeq.Add(1)
}

// Get retrieves the value for the given key.
func (c *Client) Get(ctx context.Context, key types.Key) types.Result {
	reqBody := map[string]string{
		"key": key.String(),
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return types.Result{Status: types.StatusError, Error: err.Error()}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("http://%s/api/get", c.addr), bytes.NewReader(body))
	if err != nil {
		return types.Result{Status: types.StatusError, Error: err.Error()}
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return types.Result{Status: types.StatusError, Error: err.Error()}
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return types.Result{Status: types.StatusError, Error: err.Error()}
	}

	var result struct {
		Status string `json:"status"`
		Value  string `json:"value"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return types.Result{Status: types.StatusError, Error: err.Error()}
	}

	switch result.Status {
	case "found":
		val, err := base64.StdEncoding.DecodeString(result.Value)
		if err != nil {
			return types.Result{Status: types.StatusError, Error: err.Error()}
		}
		return types.Result{Status: types.StatusFound, Value: val}
	case "not_found":
		return types.Result{Status: types.StatusNotFound}
	case "timeout":
		return types.Result{Status: types.StatusTimeout}
	default:
		return types.Result{Status: types.StatusError, Error: fmt.Sprintf("server status: %s", result.Status)}
	}
}

// Put stores a key-value pair.
func (c *Client) Put(ctx context.Context, key types.Key, value []byte) types.Result {
	seq := c.nextSeq()
	reqBody := map[string]interface{}{
		"key":        key.String(),
		"value":      base64.StdEncoding.EncodeToString(value),
		"client_id":  c.clientID,
		"client_seq": seq,
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return types.Result{Status: types.StatusError, Error: err.Error()}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("http://%s/api/put", c.addr), bytes.NewReader(body))
	if err != nil {
		return types.Result{Status: types.StatusError, Error: err.Error()}
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return types.Result{Status: types.StatusError, Error: err.Error()}
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return types.Result{Status: types.StatusError, Error: err.Error()}
	}

	var result struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return types.Result{Status: types.StatusError, Error: err.Error()}
	}

	switch result.Status {
	case "ok":
		return types.Result{Status: types.StatusOK}
	case "timeout":
		return types.Result{Status: types.StatusTimeout}
	default:
		return types.Result{Status: types.StatusError, Error: fmt.Sprintf("server status: %s", result.Status)}
	}
}

// Delete removes a key from the store.
func (c *Client) Delete(ctx context.Context, key types.Key) types.Result {
	seq := c.nextSeq()
	reqBody := map[string]interface{}{
		"key":        key.String(),
		"client_id":  c.clientID,
		"client_seq": seq,
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return types.Result{Status: types.StatusError, Error: err.Error()}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("http://%s/api/delete", c.addr), bytes.NewReader(body))
	if err != nil {
		return types.Result{Status: types.StatusError, Error: err.Error()}
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return types.Result{Status: types.StatusError, Error: err.Error()}
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return types.Result{Status: types.StatusError, Error: err.Error()}
	}

	var result struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return types.Result{Status: types.StatusError, Error: err.Error()}
	}

	switch result.Status {
	case "ok":
		return types.Result{Status: types.StatusOK}
	case "timeout":
		return types.Result{Status: types.StatusTimeout}
	default:
		return types.Result{Status: types.StatusError, Error: fmt.Sprintf("server status: %s", result.Status)}
	}
}

// Status returns the node's current status.
func (c *Client) Status(ctx context.Context) (*StatusResp, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("http://%s/api/status", c.addr), nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var status StatusResp
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, err
	}

	return &status, nil
}

// Close releases resources held by the client.
func (c *Client) Close() error {
	c.httpClient.CloseIdleConnections()
	return nil
}
