package kvstore

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type transport struct {
	node      *Node
	listener  net.Listener
	server    *rpc.Server
	peerMu    sync.Mutex
	peerConns map[uint64]*rpc.Client
}

func newTransport(n *Node) *transport {
	return &transport{
		node:      n,
		peerConns: make(map[uint64]*rpc.Client),
	}
}

func (t *transport) start(addr string) error {
	svc := &NodeRPC{node: t.node}
	t.server = rpc.NewServer()
	if err := t.server.Register(svc); err != nil {
		return fmt.Errorf("register rpc: %w", err)
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	t.listener = ln

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go t.server.ServeConn(conn)
		}
	}()
	return nil
}

func (t *transport) stop() {
	if t.listener != nil {
		t.listener.Close()
	}
	t.peerMu.Lock()
	defer t.peerMu.Unlock()
	for id, c := range t.peerConns {
		c.Close()
		delete(t.peerConns, id)
	}
}

func (t *transport) call(peerID uint64, method string, args any, reply any, timeout time.Duration) error {
	client, err := t.getConn(peerID)
	if err != nil {
		return err
	}

	done := client.Go(method, args, reply, nil)
	select {
	case <-done.Done:
		if done.Error != nil {
			t.removeConn(peerID)
		}
		return done.Error
	case <-time.After(timeout):
		t.removeConn(peerID)
		return fmt.Errorf("rpc timeout to node %d", peerID)
	}
}

func (t *transport) getConn(peerID uint64) (*rpc.Client, error) {
	t.peerMu.Lock()
	defer t.peerMu.Unlock()

	if c, ok := t.peerConns[peerID]; ok {
		return c, nil
	}

	var addr string
	for _, p := range t.node.config.Peers {
		if p.ID == peerID {
			addr = p.RPCAddr
			break
		}
	}
	if addr == "" {
		return nil, fmt.Errorf("unknown peer %d", peerID)
	}

	conn, err := net.DialTimeout("tcp", addr, t.node.config.RPCTimeout)
	if err != nil {
		return nil, fmt.Errorf("dial peer %d at %s: %w", peerID, addr, err)
	}

	client := rpc.NewClient(conn)
	t.peerConns[peerID] = client
	return client, nil
}

func (t *transport) removeConn(peerID uint64) {
	t.peerMu.Lock()
	defer t.peerMu.Unlock()
	if c, ok := t.peerConns[peerID]; ok {
		c.Close()
		delete(t.peerConns, peerID)
	}
}
