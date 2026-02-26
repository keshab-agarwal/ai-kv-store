package skeleton

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	"github.com/tensorkv/harness/interfaces"
	"kv-store/client"
	kvi "kv-store/interfaces"
)

type nodeStore struct {
	inner  *client.Client
	nodeID interfaces.NodeID
}

func (s *nodeStore) Put(ctx context.Context, key interfaces.Key, value interfaces.Value) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(value) < interfaces.MinValueSize {
		return interfaces.ErrValueTooSmall
	}
	if len(value) > interfaces.MaxValueSize {
		return interfaces.ErrValueTooLarge
	}
	if len(value) > kvi.MaxValueSize {
		return interfaces.ErrValueTooLarge
	}
	var kvKey kvi.Key
	copy(kvKey[:], key[:])
	err := s.inner.Put(ctx, kvKey, kvi.Value(value))
	return mapErr(err)
}

func (s *nodeStore) Get(ctx context.Context, key interfaces.Key) (interfaces.Value, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var kvKey kvi.Key
	copy(kvKey[:], key[:])
	val, err := s.inner.Get(ctx, kvKey)
	if err != nil {
		return nil, mapErr(err)
	}
	return interfaces.Value(val), nil
}

func mapErr(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, kvi.ErrKeyNotFound):
		return interfaces.ErrKeyNotFound
	case errors.Is(err, kvi.ErrValueTooSmall):
		return interfaces.ErrValueTooSmall
	case errors.Is(err, kvi.ErrValueTooLarge):
		return interfaces.ErrValueTooLarge
	case errors.Is(err, kvi.ErrNodeDown):
		return interfaces.ErrNodeDown
	}
	return err
}

type Cluster struct {
	n          int
	ports      []int
	adminPorts []int
	cmds       []*exec.Cmd
	nodeIDs    []interfaces.NodeID
	dataDirs   []string
	nodePath   string
	peerAddrs  []string
}

func NewCluster() *Cluster {
	return &Cluster{}
}

func (c *Cluster) Start(n int) error {
	if n <= 0 {
		return errors.New("node count must be > 0")
	}
	c.n = n
	c.ports = make([]int, n)
	c.adminPorts = make([]int, n)
	c.cmds = make([]*exec.Cmd, n)
	c.nodeIDs = make([]interfaces.NodeID, n)
	c.dataDirs = make([]string, n)
	c.peerAddrs = make([]string, n)

	for i := range n {
		p, err := freePort()
		if err != nil {
			return fmt.Errorf("node %d: allocate port: %w", i, err)
		}
		a, err := freePort()
		if err != nil {
			return fmt.Errorf("node %d: allocate admin port: %w", i, err)
		}
		c.ports[i] = p
		c.adminPorts[i] = a
		c.nodeIDs[i] = interfaces.NodeID(i)
		dir, err := os.MkdirTemp("", "kv-node-")
		if err != nil {
			return fmt.Errorf("node %d: mkdir: %w", i, err)
		}
		c.dataDirs[i] = dir
		c.peerAddrs[i] = fmt.Sprintf("127.0.0.1:%d", c.ports[i])
	}

	nodePath := os.Getenv("KV_NODE_BIN")
	if nodePath == "" {
		abs, _ := filepath.Abs(filepath.Join("..", "bin", "node"))
		nodePath = abs
	}
	c.nodePath = nodePath
	if _, err := os.Stat(nodePath); err != nil {
		return fmt.Errorf("node binary %s not found: %w", nodePath, err)
	}

	peersStr := ""
	for i := 0; i < n; i++ {
		if i > 0 {
			peersStr += ","
		}
		peersStr += c.peerAddrs[i]
	}

	for i := range n {
		cmd := exec.Command(nodePath,
			"-id", strconv.Itoa(i),
			"-port", strconv.Itoa(c.ports[i]),
			"-admin", strconv.Itoa(c.adminPorts[i]),
			"-data", c.dataDirs[i],
			"-peers", peersStr,
		)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			for j := 0; j < i; j++ {
				_ = c.cmds[j].Process.Kill()
			}
			return fmt.Errorf("node %d: start: %w", i, err)
		}
		c.cmds[i] = cmd
	}

	for i := 0; i < n; i++ {
		if err := waitReady("127.0.0.1", c.ports[i], 15*time.Second); err != nil {
			return fmt.Errorf("node %d: not ready: %w", i, err)
		}
	}
	return nil
}

func waitReady(host string, port int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	addr := fmt.Sprintf("%s:%d", host, port)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s", addr)
}

func (c *Cluster) Connect(nodeID interfaces.NodeID) (interfaces.Store, error) {
	idx := int(nodeID)
	if idx < 0 || idx >= c.n {
		return nil, fmt.Errorf("node %d not found", nodeID)
	}
	addr := c.peerAddrs[idx]
	cl, err := client.New(addr)
	if err != nil {
		return nil, err
	}
	return &nodeStore{inner: cl, nodeID: nodeID}, nil
}

func (c *Cluster) NodeIDs() []interfaces.NodeID {
	ids := make([]interfaces.NodeID, len(c.nodeIDs))
	copy(ids, c.nodeIDs)
	return ids
}

func (c *Cluster) KillNode(nodeID interfaces.NodeID) error {
	idx := int(nodeID)
	if idx < 0 || idx >= len(c.cmds) {
		return fmt.Errorf("node %d not found", nodeID)
	}
	cmd := c.cmds[idx]
	if cmd == nil || cmd.Process == nil {
		return fmt.Errorf("node %d: process not running", nodeID)
	}
	return cmd.Process.Kill()
}

func (c *Cluster) RestartNode(nodeID interfaces.NodeID) error {
	idx := int(nodeID)
	if idx < 0 || idx >= c.n {
		return fmt.Errorf("node %d not found", nodeID)
	}
	peersStr := ""
	for i := 0; i < c.n; i++ {
		if i > 0 {
			peersStr += ","
		}
		peersStr += c.peerAddrs[i]
	}
	cmd := exec.Command(c.nodePath,
		"-id", strconv.Itoa(idx),
		"-port", strconv.Itoa(c.ports[idx]),
		"-admin", strconv.Itoa(c.adminPorts[idx]),
		"-data", c.dataDirs[idx],
		"-peers", peersStr,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	c.cmds[idx] = cmd
	return waitReady("127.0.0.1", c.ports[idx], 15*time.Second)
}

func (c *Cluster) PartitionNodes(a, b interfaces.NodeID) error {
	addrA := c.peerAddrs[int(a)]
	addrB := c.peerAddrs[int(b)]
	if err := adminPost(c.adminPorts[int(a)], "partition", addrB); err != nil {
		return err
	}
	return adminPost(c.adminPorts[int(b)], "partition", addrA)
}

func (c *Cluster) HealPartition(a, b interfaces.NodeID) error {
	addrA := c.peerAddrs[int(a)]
	addrB := c.peerAddrs[int(b)]
	if err := adminPost(c.adminPorts[int(a)], "heal", addrB); err != nil {
		return err
	}
	return adminPost(c.adminPorts[int(b)], "heal", addrA)
}

func adminPost(adminPort int, path, peer string) error {
	url := fmt.Sprintf("http://127.0.0.1:%d/%s?peer=%s", adminPort, path, peer)
	resp, err := http.Post(url, "", nil)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("admin %s: status %d", path, resp.StatusCode)
	}
	return nil
}

func (c *Cluster) Shutdown() error {
	for i, cmd := range c.cmds {
		if cmd != nil && cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
		if i < len(c.dataDirs) && c.dataDirs[i] != "" {
			_ = os.RemoveAll(c.dataDirs[i])
		}
	}
	return nil
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
