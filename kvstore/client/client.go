package client

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"ai-kv-store/kvstore"
)

type Client struct {
	nodes      []string
	clientID   string
	seqNum     atomic.Uint64
	httpClient *http.Client
	preferred  int
}

func New(nodes []string) *Client {
	id := make([]byte, 8)
	rand.Read(id)
	return &Client{
		nodes:    nodes,
		clientID: fmt.Sprintf("%x", id),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func NewWithID(nodes []string, clientID string) *Client {
	return &Client{
		nodes:    nodes,
		clientID: clientID,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *Client) Put(key kvstore.Key, value []byte) kvstore.OpResult {
	seq := c.seqNum.Add(1)
	body := map[string]any{
		"key":        key.Hex(),
		"value":      base64.StdEncoding.EncodeToString(value),
		"client_id":  c.clientID,
		"client_seq": seq,
	}
	return c.doRequest("/api/put", body)
}

func (c *Client) Get(key kvstore.Key) kvstore.OpResult {
	body := map[string]any{
		"key": key.Hex(),
	}
	return c.doRequest("/api/get", body)
}

func (c *Client) Delete(key kvstore.Key) kvstore.OpResult {
	seq := c.seqNum.Add(1)
	body := map[string]any{
		"key":        key.Hex(),
		"client_id":  c.clientID,
		"client_seq": seq,
	}
	return c.doRequest("/api/delete", body)
}

type apiResponse struct {
	Status string `json:"status"`
	Value  string `json:"value,omitempty"`
}

func (c *Client) doRequest(path string, body map[string]any) kvstore.OpResult {
	data, _ := json.Marshal(body)

	for attempt := 0; attempt < len(c.nodes); attempt++ {
		idx := (c.preferred + attempt) % len(c.nodes)
		url := fmt.Sprintf("http://%s%s", c.nodes[idx], path)

		resp, err := c.httpClient.Post(url, "application/json", bytes.NewReader(data))
		if err != nil {
			continue
		}
		respBody, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			continue
		}

		var apiResp apiResponse
		if err := json.Unmarshal(respBody, &apiResp); err != nil {
			continue
		}

		result := kvstore.OpResult{}
		switch apiResp.Status {
		case "OK":
			result.Status = kvstore.StatusOK
			c.preferred = idx
		case "FOUND":
			result.Status = kvstore.StatusFound
			if apiResp.Value != "" {
				val, err := base64.StdEncoding.DecodeString(apiResp.Value)
				if err == nil {
					result.Value = val
				}
			}
			c.preferred = idx
		case "NOT_FOUND":
			result.Status = kvstore.StatusNotFound
			c.preferred = idx
		case "TIMEOUT":
			result.Status = kvstore.StatusTimeout
		default:
			result.Status = kvstore.StatusError
		}
		return result
	}

	return kvstore.OpResult{Status: kvstore.StatusTimeout}
}

func (c *Client) ClientID() string { return c.clientID }
