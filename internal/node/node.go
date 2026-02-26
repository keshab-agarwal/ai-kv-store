package node

import (
	"fmt"
	"kv-store/interfaces"
	"kv-store/internal/store"
	"kv-store/internal/version"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)


const NumNodes = 3

type Node struct {
	ID            interfaces.NodeID
	store         *store.CausalStore
	dataDir       string
	peerAddrs     []string
	down          atomic.Bool
	listener      net.Listener
	adminListener net.Listener
	server        *rpc.Server
	wg            sync.WaitGroup
	blocked       map[string]bool
	blockMu       sync.RWMutex
	peerClients   [NumNodes]*rpc.Client
	peerMu        [NumNodes]sync.Mutex
	appendFile    *os.File
	appendMu      sync.Mutex
}

func New(id interfaces.NodeID, dataDir string, peerAddrs []string) (*Node, error) {
	if len(peerAddrs) != NumNodes {
		return nil, fmt.Errorf("need %d peer addrs", NumNodes)
	}
	s := store.NewCausalStore(id)
	n := &Node{
		ID:        id,
		store:     s,
		dataDir:   dataDir,
		peerAddrs: peerAddrs,
		blocked:   make(map[string]bool),
	}
	if err := s.Load(dataDir + "/data"); err != nil {
		return nil, err
	}
	return n, nil
}

func (n *Node) getPeerClient(peer int) (*rpc.Client, error) {
	n.peerMu[peer].Lock()
	defer n.peerMu[peer].Unlock()
	if n.peerClients[peer] != nil {
		return n.peerClients[peer], nil
	}
	if !n.canDial(n.peerAddrs[peer]) {
		return nil, fmt.Errorf("blocked")
	}
	client, err := rpc.Dial("tcp", n.peerAddrs[peer])
	if err != nil {
		return nil, err
	}
	n.peerClients[peer] = client
	return client, nil
}

func (n *Node) dropPeerClient(peer int) {
	n.peerMu[peer].Lock()
	c := n.peerClients[peer]
	n.peerClients[peer] = nil
	n.peerMu[peer].Unlock()
	if c != nil {
		_ = c.Close()
	}
}

func (n *Node) ensureAppendFile() error {
	if n.appendFile != nil {
		return nil
	}
	path := n.dataDir + "/data"
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	n.appendFile = f
	return nil
}

func (n *Node) appendToLog(key interfaces.Key, value interfaces.Value, vv version.Vector) error {
	n.appendMu.Lock()
	defer n.appendMu.Unlock()
	if err := n.ensureAppendFile(); err != nil {
		return err
	}
	return store.WriteRecord(n.appendFile, key, value, vv)
}

func (n *Node) Put(req PutReq, resp *PutResp) error {
	if len(req.Value) > interfaces.MaxValueSize {
		resp.Err = interfaces.ErrValueTooLarge.Error()
		return nil
	}
	owner := store.KeyOwner(req.Key, NumNodes)
	if owner != n.ID {
		if n.down.Load() {
			resp.Err = interfaces.ErrNodeDown.Error()
			return nil
		}
		if !n.canDial(n.peerAddrs[owner]) {
			resp.Err = interfaces.ErrNodeDown.Error()
			return nil
		}
		client, err := n.getPeerClient(int(owner))
		if err != nil {
			resp.Err = interfaces.ErrNodeDown.Error()
			return nil
		}
		err = client.Call("NodeRPC.Put", req, resp)
		if err != nil {
			n.dropPeerClient(int(owner))
			resp.Err = interfaces.ErrNodeDown.Error()
		}
		return nil
	}
	if n.down.Load() {
		resp.Err = interfaces.ErrNodeDown.Error()
		return nil
	}
	vv := n.store.Put(req.Key, req.Value, req.ClientVV)
	if err := n.appendToLog(req.Key, req.Value, vv); err != nil {
		resp.Err = err.Error()
		return nil
	}
	resp.VV = vv
	replicated := false
	for i := 0; i < NumNodes; i++ {
		if interfaces.NodeID(i) == n.ID {
			continue
		}
		if !n.canDial(n.peerAddrs[i]) {
			continue
		}
		client, err := n.getPeerClient(i)
		if err != nil {
			continue
		}
		var peerResp ReplicateResp
		err = client.Call("NodeRPC.Replicate", ReplicateReq{Key: req.Key, Value: req.Value, VV: vv}, &peerResp)
		if err != nil {
			n.dropPeerClient(i)
			continue
		}
		replicated = true
		break
	}
	if !replicated {
		resp.Err = interfaces.ErrNodeDown.Error()
		return nil
	}
	for i := 0; i < NumNodes; i++ {
		if interfaces.NodeID(i) == n.ID {
			continue
		}
		peer := i
		if !n.canDial(n.peerAddrs[peer]) {
			continue
		}
		go func() {
			client, err := n.getPeerClient(peer)
			if err != nil {
				return
			}
			var r ReplicateResp
			if client.Call("NodeRPC.Replicate", ReplicateReq{Key: req.Key, Value: req.Value, VV: vv}, &r) != nil {
				n.dropPeerClient(peer)
			}
		}()
	}
	return nil
}

func (n *Node) canDial(addr string) bool {
	n.blockMu.RLock()
	ok := !n.blocked[addr]
	n.blockMu.RUnlock()
	return ok
}

func (n *Node) BlockPeer(addr string) {
	for i := 0; i < NumNodes; i++ {
		if n.peerAddrs[i] == addr {
			n.dropPeerClient(i)
			break
		}
	}
	n.blockMu.Lock()
	n.blocked[addr] = true
	n.blockMu.Unlock()
}

func (n *Node) UnblockPeer(addr string) {
	n.blockMu.Lock()
	delete(n.blocked, addr)
	n.blockMu.Unlock()
}

func (n *Node) Get(req GetReq, resp *GetResp) error {
	if n.down.Load() {
		resp.Err = interfaces.ErrNodeDown.Error()
		return nil
	}
	value, vv, found := n.store.Get(req.Key, req.ClientVV)
	if found {
		resp.Value = value
		resp.VV = vv
		resp.Found = true
		return nil
	}
	owner := store.KeyOwner(req.Key, NumNodes)
	if owner == n.ID {
		resp.Err = interfaces.ErrKeyNotFound.Error()
		return nil
	}
	if !n.canDial(n.peerAddrs[owner]) {
		resp.Err = interfaces.ErrKeyNotFound.Error()
		return nil
	}
	client, err := n.getPeerClient(int(owner))
	if err != nil {
		resp.Err = interfaces.ErrKeyNotFound.Error()
		return nil
	}
	var peerResp GetResp
	if client.Call("NodeRPC.Get", req, &peerResp) != nil {
		n.dropPeerClient(int(owner))
		resp.Err = interfaces.ErrKeyNotFound.Error()
		return nil
	}
	if !peerResp.Found {
		resp.Err = interfaces.ErrKeyNotFound.Error()
		return nil
	}
	resp.Value = peerResp.Value
	resp.VV = peerResp.VV
	resp.Found = true
	return nil
}

func (n *Node) Replicate(req ReplicateReq, resp *ReplicateResp) error {
	if n.down.Load() {
		return nil
	}
	n.store.ApplyReplicated(req.Key, req.Value, req.VV)
	_ = n.appendToLog(req.Key, req.Value, req.VV)
	return nil
}

type NodeRPC struct{ node *Node }

func (r *NodeRPC) Put(req PutReq, resp *PutResp) error { return r.node.Put(req, resp) }
func (r *NodeRPC) Get(req GetReq, resp *GetResp) error { return r.node.Get(req, resp) }
func (r *NodeRPC) Replicate(req ReplicateReq, resp *ReplicateResp) error {
	return r.node.Replicate(req, resp)
}

func (n *Node) Listen(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	n.listener = ln
	n.server = rpc.NewServer()
	if err := n.server.Register(&NodeRPC{node: n}); err != nil {
		ln.Close()
		return err
	}
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go n.server.ServeConn(conn)
		}
	}()
	return nil
}

func (n *Node) ListenAdmin(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/partition", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		peer := r.URL.Query().Get("peer")
		if peer == "" {
			http.Error(w, "missing peer", http.StatusBadRequest)
			return
		}
		n.BlockPeer(peer)
	})
	mux.HandleFunc("/heal", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		peer := r.URL.Query().Get("peer")
		if peer == "" {
			http.Error(w, "missing peer", http.StatusBadRequest)
			return
		}
		n.UnblockPeer(peer)
	})
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	n.adminListener = ln
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		_ = http.Serve(ln, mux)
	}()
	return nil
}

func (n *Node) Close() error {
	n.appendMu.Lock()
	if n.appendFile != nil {
		_ = n.appendFile.Close()
		n.appendFile = nil
	}
	n.appendMu.Unlock()
	for i := 0; i < NumNodes; i++ {
		n.peerMu[i].Lock()
		if c := n.peerClients[i]; c != nil {
			_ = c.Close()
			n.peerClients[i] = nil
		}
		n.peerMu[i].Unlock()
	}
	if n.adminListener != nil {
		_ = n.adminListener.Close()
	}
	if n.listener != nil {
		return n.listener.Close()
	}
	return nil
}

func (n *Node) SetDown(down bool) { n.down.Store(down) }
