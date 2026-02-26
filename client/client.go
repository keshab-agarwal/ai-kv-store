package client

import (
	"context"
	"kv-store/interfaces"
	"kv-store/internal/node"
	"kv-store/internal/version"
	"net/rpc"
	"sync"
)

type Client struct {
	addr  string
	conn  *rpc.Client
	ctxVV version.Vector
	mu    sync.Mutex
}

func New(addr string) (*Client, error) {
	conn, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{addr: addr, conn: conn}, nil
}

func (c *Client) Put(ctx context.Context, key interfaces.Key, value interfaces.Value) error {
	if len(value) > interfaces.MaxValueSize {
		return interfaces.ErrValueTooLarge
	}
	c.mu.Lock()
	clientVV := c.ctxVV
	c.mu.Unlock()
	req := node.PutReq{Key: key, Value: value, ClientVV: clientVV}
	var resp node.PutResp
	err := c.conn.Call("NodeRPC.Put", req, &resp)
	if err != nil {
		return interfaces.ErrNodeDown
	}
	if resp.Err != "" {
		switch resp.Err {
		case interfaces.ErrValueTooLarge.Error():
			return interfaces.ErrValueTooLarge
		case interfaces.ErrNodeDown.Error():
			return interfaces.ErrNodeDown
		default:
			return interfaces.ErrNodeDown
		}
	}
	c.mu.Lock()
	c.ctxVV = c.ctxVV.Merge(resp.VV)
	c.mu.Unlock()
	return nil
}

func (c *Client) Get(ctx context.Context, key interfaces.Key) (interfaces.Value, error) {
	c.mu.Lock()
	clientVV := c.ctxVV
	c.mu.Unlock()
	req := node.GetReq{Key: key, ClientVV: clientVV}
	var resp node.GetResp
	err := c.conn.Call("NodeRPC.Get", req, &resp)
	if err != nil {
		return nil, interfaces.ErrNodeDown
	}
	if resp.Err != "" {
		if resp.Err == interfaces.ErrKeyNotFound.Error() {
			return nil, interfaces.ErrKeyNotFound
		}
		return nil, interfaces.ErrNodeDown
	}
	c.mu.Lock()
	c.ctxVV = c.ctxVV.Merge(resp.VV)
	c.mu.Unlock()
	return resp.Value, nil
}

func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

var _ interfaces.Store = (*Client)(nil)
