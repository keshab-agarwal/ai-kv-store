package ycsb

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Client talks to the distributed KV store over HTTP.
// It handles 421 leader-redirect responses and fails over to other nodes.
type Client struct {
	nodes  []string
	client *http.Client

	mu     sync.RWMutex
	leader string // cached leader address, empty if unknown
}

// NewClient creates a Client from a comma-separated list of node addresses.
func NewClient(nodeList string) *Client {
	raw := strings.Split(nodeList, ",")
	nodes := make([]string, 0, len(raw))
	for _, n := range raw {
		n = strings.TrimSpace(n)
		if n != "" {
			nodes = append(nodes, n)
		}
	}
	return &Client{
		nodes: nodes,
		client: &http.Client{
			Timeout: 10 * time.Second,
			// Do not follow redirects automatically; we handle 421 ourselves.
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

func keyHex(key [16]byte) string {
	return hex.EncodeToString(key[:])
}

// Get retrieves the value for key.
// Returns (value, true, nil) on hit, (nil, false, nil) on 404,
// or (nil, false, err) on failure.
func (c *Client) Get(key [16]byte) ([]byte, bool, error) {
	path := "/kv/" + keyHex(key)
	body, status, err := c.doWithRetry("GET", path, nil)
	if err != nil {
		return nil, false, err
	}
	switch status {
	case http.StatusOK:
		return body, true, nil
	case http.StatusNotFound:
		return nil, false, nil
	default:
		return nil, false, fmt.Errorf("GET %s: unexpected status %d", path, status)
	}
}

// Put stores a value for the given key.
func (c *Client) Put(key [16]byte, value []byte) error {
	path := "/kv/" + keyHex(key)
	_, status, err := c.doWithRetry("PUT", path, value)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("PUT %s: unexpected status %d", path, status)
	}
	return nil
}

// Delete removes the given key.
func (c *Client) Delete(key [16]byte) error {
	path := "/kv/" + keyHex(key)
	_, status, err := c.doWithRetry("DELETE", path, nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("DELETE %s: unexpected status %d", path, status)
	}
	return nil
}

// doWithRetry sends the request, following 421 redirects and failing over to
// other nodes when a node is unreachable or returns 503.
func (c *Client) doWithRetry(method, path string, body []byte) ([]byte, int, error) {
	// Build a trial order: cached leader first, then the rest.
	order := c.trialOrder()

	const maxRedirects = 5
	var lastErr error

	for _, addr := range order {
		target := addr
		for redir := 0; redir < maxRedirects; redir++ {
			respBody, status, leaderHint, err := c.doOnce(method, target, path, body)
			if err != nil {
				lastErr = fmt.Errorf("node %s: %w", target, err)
				break // try next node
			}
			switch status {
			case http.StatusMisdirectedRequest: // 421 — not leader
				if leaderHint == "" {
					lastErr = fmt.Errorf("node %s: 421 without X-Leader", target)
					break // try next node
				}
				// Normalize: ":16001" → "localhost:16001"
				if strings.HasPrefix(leaderHint, ":") {
					leaderHint = "localhost" + leaderHint
				}
				c.setLeader(leaderHint)
				target = leaderHint
				continue // follow redirect
			case http.StatusServiceUnavailable: // 503
				lastErr = fmt.Errorf("node %s: 503 unavailable", target)
				break // try next node
			default:
				// Success or application-level error the caller should handle.
				if status == http.StatusOK {
					c.setLeader(target)
				}
				return respBody, status, nil
			}
			break // inner break for cases that set lastErr
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no nodes available")
	}
	return nil, 0, lastErr
}

func (c *Client) doOnce(method, addr, path string, body []byte) ([]byte, int, string, error) {
	url := "http://" + addr + path
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return nil, 0, "", err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, 0, "", err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, "", err
	}
	leaderHint := resp.Header.Get("X-Leader")
	return respBody, resp.StatusCode, leaderHint, nil
}

// trialOrder returns the list of node addresses to try, with the cached leader
// first (if known).
func (c *Client) trialOrder() []string {
	c.mu.RLock()
	leader := c.leader
	c.mu.RUnlock()

	if leader == "" {
		// Return a copy so callers cannot mutate our slice.
		out := make([]string, len(c.nodes))
		copy(out, c.nodes)
		return out
	}

	out := make([]string, 0, len(c.nodes))
	out = append(out, leader)
	for _, n := range c.nodes {
		if n != leader {
			out = append(out, n)
		}
	}
	return out
}

func (c *Client) setLeader(addr string) {
	c.mu.Lock()
	c.leader = addr
	c.mu.Unlock()
}
