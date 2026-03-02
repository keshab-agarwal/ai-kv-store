package correctness

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// NodeConfig describes how to start a node.
type NodeConfig struct {
	ID         int
	RPCPort    int    // e.g., 17101
	ClientPort int    // e.g., 16101
	DataDir    string
	BinPath    string // path to kvnode binary
}

// NodeProcess represents a running kvnode process.
type NodeProcess struct {
	cfg     NodeConfig
	cmd     *exec.Cmd
	mu      sync.Mutex
	alive   bool
	logFile *os.File
}

// Orchestrator manages a set of kvnode processes.
type Orchestrator struct {
	nodes    []*NodeProcess
	baseRPC  int
	baseHTTP int
	outDir   string
	binPath  string
}

// NewOrchestrator creates n nodes with:
//   - IDs: 1..n
//   - RPC ports:    baseRPC+i  (default base 17100, so node 1 -> 17101)
//   - Client ports: baseHTTP+i (default base 16100, so node 1 -> 16101)
//   - DataDir: outDir/data-i
func NewOrchestrator(binPath string, n int, outDir string) (*Orchestrator, error) {
	const baseRPC = 17100
	const baseHTTP = 16100

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir outDir %s: %w", outDir, err)
	}

	o := &Orchestrator{
		baseRPC:  baseRPC,
		baseHTTP: baseHTTP,
		outDir:   outDir,
		binPath:  binPath,
	}

	for i := 1; i <= n; i++ {
		cfg := NodeConfig{
			ID:         i,
			RPCPort:    baseRPC + i,
			ClientPort: baseHTTP + i,
			DataDir:    filepath.Join(outDir, fmt.Sprintf("data-%d", i)),
			BinPath:    binPath,
		}
		// Always start with a clean data directory so replayed WALs from a
		// previous run with the same outDir do not pollute the history.
		if err := os.RemoveAll(cfg.DataDir); err != nil {
			return nil, fmt.Errorf("clean data dir %s: %w", cfg.DataDir, err)
		}
		np := &NodeProcess{cfg: cfg}
		o.nodes = append(o.nodes, np)
	}
	return o, nil
}

// peersFlag builds the -peers flag value for node i (all other nodes).
// Format: "2=localhost:17102,3=localhost:17103" for node 1 with base 17100.
func (o *Orchestrator) peersFlag(nodeIdx int) string {
	var parts []string
	for j, np := range o.nodes {
		if j == nodeIdx {
			continue
		}
		parts = append(parts, fmt.Sprintf("%d=localhost:%d", np.cfg.ID, np.cfg.RPCPort))
	}
	return strings.Join(parts, ",")
}

// Start starts node with the given 1-based ID.
// Builds the -peers flag, starts the process, and waits until /api/status returns 200 (up to 5s).
func (o *Orchestrator) Start(id int) error {
	np, err := o.findNode(id)
	if err != nil {
		return err
	}

	np.mu.Lock()
	defer np.mu.Unlock()

	if np.alive {
		return nil // already running
	}

	nodeIdx := id - 1
	peers := o.peersFlag(nodeIdx)

	// Ensure data dir exists.
	if err := os.MkdirAll(np.cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("mkdir data dir %s: %w", np.cfg.DataDir, err)
	}

	args := []string{
		fmt.Sprintf("-id=%d", np.cfg.ID),
		fmt.Sprintf("-rpc=:%d", np.cfg.RPCPort),
		fmt.Sprintf("-client=:%d", np.cfg.ClientPort),
		fmt.Sprintf("-data=%s", np.cfg.DataDir),
	}
	if peers != "" {
		args = append(args, fmt.Sprintf("-peers=%s", peers))
	}

	cmd := exec.Command(np.cfg.BinPath, args...)

	// Redirect stdout/stderr to log files.
	logPath := filepath.Join(o.outDir, fmt.Sprintf("node-%d.log", np.cfg.ID))
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open log file %s: %w", logPath, err)
	}
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		logFile.Close()
		return fmt.Errorf("start node %d: %w", id, err)
	}

	np.cmd = cmd
	np.logFile = logFile
	np.alive = true

	// Wait for /api/status to return 200 (up to 5s).
	addr := fmt.Sprintf("localhost:%d", np.cfg.ClientPort)
	if err := waitForHTTP(addr, 5*time.Second); err != nil {
		// Try to kill the process if it didn't come up.
		_ = cmd.Process.Kill()
		np.alive = false
		return fmt.Errorf("node %d did not become ready: %w", id, err)
	}

	return nil
}

// waitForHTTP polls GET http://addr/api/status until it returns HTTP 200 or timeout.
func waitForHTTP(addr string, timeout time.Duration) error {
	client := &http.Client{Timeout: 500 * time.Millisecond}
	url := "http://" + addr + "/api/status"
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s", url)
}

// Stop gracefully stops node with the given ID (SIGTERM, wait up to 3s, then SIGKILL).
func (o *Orchestrator) Stop(id int) error {
	np, err := o.findNode(id)
	if err != nil {
		return err
	}

	np.mu.Lock()
	defer np.mu.Unlock()

	if !np.alive || np.cmd == nil {
		return nil
	}

	// Send SIGTERM.
	if err := np.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		// If SIGTERM fails, try SIGKILL.
		_ = np.cmd.Process.Kill()
	}

	// Wait up to 3s for the process to exit.
	done := make(chan error, 1)
	go func() { done <- np.cmd.Wait() }()

	select {
	case <-done:
		// Exited cleanly.
	case <-time.After(3 * time.Second):
		// Force kill.
		_ = np.cmd.Process.Kill()
		<-done
	}

	if np.logFile != nil {
		np.logFile.Close()
		np.logFile = nil
	}
	np.alive = false
	np.cmd = nil
	return nil
}

// Crash hard-kills node i (SIGKILL).
func (o *Orchestrator) Crash(id int) error {
	np, err := o.findNode(id)
	if err != nil {
		return err
	}

	np.mu.Lock()
	defer np.mu.Unlock()

	if !np.alive || np.cmd == nil {
		return nil
	}

	if err := np.cmd.Process.Kill(); err != nil {
		return fmt.Errorf("kill node %d: %w", id, err)
	}

	done := make(chan error, 1)
	go func() { done <- np.cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
	}

	if np.logFile != nil {
		np.logFile.Close()
		np.logFile = nil
	}
	np.alive = false
	np.cmd = nil
	return nil
}

// Recover restarts a previously crashed node.
// Does NOT clean the data directory (keeps WAL/snapshot for persistence test).
// Waits for /api/status to succeed (up to 30s).
func (o *Orchestrator) Recover(id int) error {
	np, err := o.findNode(id)
	if err != nil {
		return err
	}

	np.mu.Lock()
	if np.alive {
		np.mu.Unlock()
		return nil
	}
	np.mu.Unlock()

	nodeIdx := id - 1
	peers := o.peersFlag(nodeIdx)

	args := []string{
		fmt.Sprintf("-id=%d", np.cfg.ID),
		fmt.Sprintf("-rpc=:%d", np.cfg.RPCPort),
		fmt.Sprintf("-client=:%d", np.cfg.ClientPort),
		fmt.Sprintf("-data=%s", np.cfg.DataDir),
	}
	if peers != "" {
		args = append(args, fmt.Sprintf("-peers=%s", peers))
	}

	cmd := exec.Command(np.cfg.BinPath, args...)

	logPath := filepath.Join(o.outDir, fmt.Sprintf("node-%d.log", np.cfg.ID))
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open log file for recovery %s: %w", logPath, err)
	}
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		logFile.Close()
		return fmt.Errorf("recover node %d: %w", id, err)
	}

	np.mu.Lock()
	np.cmd = cmd
	np.logFile = logFile
	np.alive = true
	np.mu.Unlock()

	addr := fmt.Sprintf("localhost:%d", np.cfg.ClientPort)
	if err := waitForHTTP(addr, 30*time.Second); err != nil {
		_ = o.Crash(id)
		return fmt.Errorf("node %d did not recover in time: %w", id, err)
	}

	return nil
}

// StartAll starts all nodes and waits for leader election.
func (o *Orchestrator) StartAll() error {
	for _, np := range o.nodes {
		if err := o.Start(np.cfg.ID); err != nil {
			return fmt.Errorf("start node %d: %w", np.cfg.ID, err)
		}
	}
	return nil
}

// StopAll stops all running nodes.
func (o *Orchestrator) StopAll() {
	for _, np := range o.nodes {
		_ = o.Stop(np.cfg.ID)
	}
}

// WaitForLeader polls all nodes' /api/status every 100ms until one is primary.
// Returns that node's client HTTP address as "localhost:NNNNN".
func (o *Orchestrator) WaitForLeader(timeout time.Duration) (string, error) {
	client := &http.Client{Timeout: 500 * time.Millisecond}
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		for _, np := range o.nodes {
			np.mu.Lock()
			alive := np.alive
			port := np.cfg.ClientPort
			np.mu.Unlock()

			if !alive {
				continue
			}

			addr := fmt.Sprintf("localhost:%d", port)
			status, err := getNodeStatus(client, addr)
			if err != nil {
				continue
			}
			if status.Role == "primary" {
				// Return the leader's client address.
				lca := status.LeaderClientAddr
				if lca == "" {
					lca = addr
				}
				return lca, nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return "", fmt.Errorf("no leader elected within %s", timeout)
}

// ClientAddresses returns all client HTTP addresses: ["localhost:16101", "localhost:16102", ...]
func (o *Orchestrator) ClientAddresses() []string {
	addrs := make([]string, 0, len(o.nodes))
	for _, np := range o.nodes {
		addrs = append(addrs, fmt.Sprintf("localhost:%d", np.cfg.ClientPort))
	}
	return addrs
}

// NodeClientAddr returns the client address for node with the given ID.
func (o *Orchestrator) NodeClientAddr(id int) (string, error) {
	np, err := o.findNode(id)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("localhost:%d", np.cfg.ClientPort), nil
}

// findNode locates the NodeProcess for the given 1-based ID.
func (o *Orchestrator) findNode(id int) (*NodeProcess, error) {
	for _, np := range o.nodes {
		if np.cfg.ID == id {
			return np, nil
		}
	}
	return nil, fmt.Errorf("node %d not found", id)
}

// NodeCount returns the number of nodes in the orchestrator.
func (o *Orchestrator) NodeCount() int {
	return len(o.nodes)
}

// statusResponse is the minimal JSON shape from GET /api/status.
type statusResponse struct {
	NodeID           int    `json:"node_id"`
	Role             string `json:"role"`
	Epoch            uint64 `json:"epoch"`
	CommitIndex      uint64 `json:"commit_index"`
	LeaderID         int    `json:"leader_id"`
	LeaderClientAddr string `json:"leader_client_addr"`
	Keys             int    `json:"keys"`
}

// getNodeStatus fetches /api/status from a node.
func getNodeStatus(client *http.Client, addr string) (*statusResponse, error) {
	url := "http://" + addr + "/api/status"
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var s statusResponse
	if err := decodeJSON(resp, &s); err != nil {
		return nil, err
	}
	return &s, nil
}
