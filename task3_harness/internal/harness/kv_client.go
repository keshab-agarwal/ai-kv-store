package harness

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"
)

type KVClient struct {
	mu        sync.Mutex
	endpoints []string
	leader    string
	client    *http.Client
	rng       *rand.Rand
}

func NewKVClient(endpoints []string, seed int64) *KVClient {
	clean := make([]string, 0, len(endpoints))
	for _, e := range endpoints {
		trim := strings.TrimRight(strings.TrimSpace(e), "/")
		if trim != "" {
			clean = append(clean, trim)
		}
	}
	leader := ""
	if len(clean) > 0 {
		leader = clean[0]
	}
	return &KVClient{
		endpoints: clean,
		leader:    leader,
		client:    &http.Client{Timeout: 1 * time.Second},
		rng:       rand.New(rand.NewSource(seed)),
	}
}

func (c *KVClient) pickOrder() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	order := make([]string, 0, len(c.endpoints)+1)
	if c.leader != "" {
		order = append(order, c.leader)
	}
	perm := c.rng.Perm(len(c.endpoints))
	seen := map[string]struct{}{}
	for _, e := range order {
		seen[e] = struct{}{}
	}
	for _, i := range perm {
		e := c.endpoints[i]
		if _, ok := seen[e]; ok {
			continue
		}
		order = append(order, e)
	}
	return order
}

func (c *KVClient) setLeader(addr string) {
	if addr == "" {
		return
	}
	c.mu.Lock()
	c.leader = strings.TrimRight(addr, "/")
	c.mu.Unlock()
}

func (c *KVClient) Get(ctx context.Context, key string) (OpResultStatus, []byte, string) {
	for _, base := range c.pickOrder() {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/kv/"+key, nil)
		resp, err := c.client.Do(req)
		if err != nil {
			continue
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusTemporaryRedirect {
			var st map[string]string
			_ = json.Unmarshal(body, &st)
			c.setLeader(st["leader"])
			continue
		}
		if resp.StatusCode != http.StatusOK {
			return StatusError, nil, fmt.Sprintf("http %d", resp.StatusCode)
		}
		var out struct {
			Status string `json:"status"`
			Value  string `json:"value"`
		}
		if err := json.Unmarshal(body, &out); err != nil {
			return StatusError, nil, "decode get"
		}
		if out.Status == string(StatusNotFound) {
			return StatusNotFound, nil, ""
		}
		if out.Status == string(StatusFound) {
			v, err := base64.StdEncoding.DecodeString(out.Value)
			if err != nil {
				return StatusError, nil, "decode value"
			}
			return StatusFound, v, ""
		}
		return StatusError, nil, "unexpected get status"
	}
	return StatusTimeout, nil, "get timeout"
}

func (c *KVClient) Put(ctx context.Context, key string, value []byte) (OpResultStatus, string) {
	payload, _ := json.Marshal(map[string]string{"value": base64.StdEncoding.EncodeToString(value)})
	for _, base := range c.pickOrder() {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPut, base+"/kv/"+key, bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.client.Do(req)
		if err != nil {
			continue
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusTemporaryRedirect {
			var st map[string]string
			_ = json.Unmarshal(body, &st)
			c.setLeader(st["leader"])
			continue
		}
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusServiceUnavailable {
			return StatusError, fmt.Sprintf("http %d", resp.StatusCode)
		}
		var out map[string]string
		_ = json.Unmarshal(body, &out)
		s := out["status"]
		switch s {
		case "OK":
			return StatusOK, ""
		case "TIMEOUT":
			return StatusTimeout, out["error"]
		default:
			return StatusError, out["error"]
		}
	}
	return StatusTimeout, "put timeout"
}

func (c *KVClient) Delete(ctx context.Context, key string) (OpResultStatus, string) {
	for _, base := range c.pickOrder() {
		req, _ := http.NewRequestWithContext(ctx, http.MethodDelete, base+"/kv/"+key, nil)
		resp, err := c.client.Do(req)
		if err != nil {
			continue
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusTemporaryRedirect {
			var st map[string]string
			_ = json.Unmarshal(body, &st)
			c.setLeader(st["leader"])
			continue
		}
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusServiceUnavailable {
			return StatusError, fmt.Sprintf("http %d", resp.StatusCode)
		}
		var out map[string]string
		_ = json.Unmarshal(body, &out)
		s := out["status"]
		switch s {
		case "OK":
			return StatusOK, ""
		case "TIMEOUT":
			return StatusTimeout, out["error"]
		default:
			return StatusError, out["error"]
		}
	}
	return StatusTimeout, "delete timeout"
}
