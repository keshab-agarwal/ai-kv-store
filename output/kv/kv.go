package kv

import "sync"

// Client represents a client to a KV store.
type Client struct {
    address string
    store   map[string]string
    mu      sync.RWMutex
}

// NewClient creates a new KV client.
func NewClient(address string) *Client {
    return &Client{
        address: address,
        store:   make(map[string]string),
    }
}

// Put stores a key-value pair.
func (c *Client) Put(key, value string) {
    c.mu.Lock()
    defer c.mu.Unlock()
    c.store[key] = value
}

// Get retrieves the value for a key.
func (c *Client) Get(key string) string {
    c.mu.RLock()
    defer c.mu.RUnlock()
    return c.store[key]
}

// Delete removes a key-value pair.
func (c *Client) Delete(key string) {
    c.mu.Lock()
    defer c.mu.Unlock()
    delete(c.store, key)
}