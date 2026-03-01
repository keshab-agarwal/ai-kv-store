// Package orchestrator manages KV node processes for the test harness.
package orchestrator

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// OrchestratorConfig holds the configuration for the orchestrator.
type OrchestratorConfig struct {
	BasePort   int
	NodeCount  int
	BinaryPath string
	DataDir    string
	LogDir     string
}

// nodeState tracks a single node process.
type nodeState struct {
	cmd     *exec.Cmd
	logFile *os.File
	alive   bool
}

// Orchestrator manages N KV node processes.
type Orchestrator struct {
	config OrchestratorConfig
	nodes  map[int]*nodeState
	mu     sync.Mutex
}

// NewOrchestrator creates a new Orchestrator with the given config.
func NewOrchestrator(config OrchestratorConfig) *Orchestrator {
	return &Orchestrator{
		config: config,
		nodes:  make(map[int]*nodeState),
	}
}

// nodePort returns the port for the given node ID.
func (o *Orchestrator) nodePort(id int) int {
	return o.config.BasePort + id
}

// NodeAddr returns the host:port string for the given node.
func (o *Orchestrator) NodeAddr(id int) string {
	return fmt.Sprintf("127.0.0.1:%d", o.nodePort(id))
}

// peersList builds the --peers flag value for a given node.
func (o *Orchestrator) peersList() string {
	var peers []string
	for i := 0; i < o.config.NodeCount; i++ {
		peers = append(peers, o.NodeAddr(i))
	}
	return strings.Join(peers, ",")
}

// StartAll starts all nodes in the cluster.
func (o *Orchestrator) StartAll() error {
	for i := 0; i < o.config.NodeCount; i++ {
		if err := o.StartNode(i); err != nil {
			// Stop any already-started nodes on failure.
			o.StopAll()
			return fmt.Errorf("failed to start node %d: %w", i, err)
		}
	}
	return nil
}

// StopAll gracefully stops all running nodes.
func (o *Orchestrator) StopAll() {
	o.mu.Lock()
	ids := make([]int, 0, len(o.nodes))
	for id := range o.nodes {
		ids = append(ids, id)
	}
	o.mu.Unlock()

	for _, id := range ids {
		_ = o.StopNode(id)
	}
}

// StartNode starts a single node by ID.
func (o *Orchestrator) StartNode(id int) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if ns, ok := o.nodes[id]; ok && ns.alive {
		return fmt.Errorf("node %d is already running", id)
	}

	// Ensure directories exist.
	if err := os.MkdirAll(o.config.LogDir, 0755); err != nil {
		return fmt.Errorf("creating log dir: %w", err)
	}
	nodeDataDir := filepath.Join(o.config.DataDir, fmt.Sprintf("node-%d", id))
	if err := os.MkdirAll(nodeDataDir, 0755); err != nil {
		return fmt.Errorf("creating data dir: %w", err)
	}

	logPath := filepath.Join(o.config.LogDir, fmt.Sprintf("node-%d.log", id))
	logFile, err := os.Create(logPath)
	if err != nil {
		return fmt.Errorf("creating log file: %w", err)
	}

	cmd := exec.Command(o.config.BinaryPath,
		fmt.Sprintf("--id=%d", id),
		fmt.Sprintf("--port=%d", o.nodePort(id)),
		fmt.Sprintf("--peers=%s", o.peersList()),
		fmt.Sprintf("--data-dir=%s", nodeDataDir),
	)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	// Set process group so we can kill the whole group if needed.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		logFile.Close()
		return fmt.Errorf("starting node %d: %w", id, err)
	}

	ns := &nodeState{
		cmd:     cmd,
		logFile: logFile,
		alive:   true,
	}
	o.nodes[id] = ns

	// Start a goroutine to track when the process exits.
	go func() {
		_ = cmd.Wait()
		o.mu.Lock()
		if n, ok := o.nodes[id]; ok && n == ns {
			ns.alive = false
		}
		o.mu.Unlock()
	}()

	return nil
}

// StopNode sends SIGTERM to the node and waits for it to exit.
func (o *Orchestrator) StopNode(id int) error {
	o.mu.Lock()
	ns, ok := o.nodes[id]
	if !ok || !ns.alive {
		o.mu.Unlock()
		return nil
	}
	o.mu.Unlock()

	if ns.cmd.Process != nil {
		_ = ns.cmd.Process.Signal(syscall.SIGTERM)
		// Wait up to 5 seconds for graceful shutdown.
		done := make(chan struct{})
		go func() {
			_ = ns.cmd.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = ns.cmd.Process.Kill()
			<-done
		}
	}

	o.mu.Lock()
	ns.alive = false
	if ns.logFile != nil {
		ns.logFile.Close()
	}
	o.mu.Unlock()

	return nil
}

// CrashNode sends SIGKILL to immediately terminate a node.
func (o *Orchestrator) CrashNode(id int) error {
	o.mu.Lock()
	ns, ok := o.nodes[id]
	if !ok || !ns.alive {
		o.mu.Unlock()
		return fmt.Errorf("node %d is not running", id)
	}
	o.mu.Unlock()

	if ns.cmd.Process != nil {
		err := ns.cmd.Process.Kill()
		if err != nil {
			return fmt.Errorf("killing node %d: %w", id, err)
		}
	}

	// Wait for the process to finish.
	_ = ns.cmd.Wait()

	o.mu.Lock()
	ns.alive = false
	if ns.logFile != nil {
		ns.logFile.Close()
	}
	o.mu.Unlock()

	return nil
}

// RecoverNode restarts a previously stopped or crashed node.
func (o *Orchestrator) RecoverNode(id int) error {
	o.mu.Lock()
	ns, ok := o.nodes[id]
	if ok && ns.alive {
		o.mu.Unlock()
		return fmt.Errorf("node %d is still running", id)
	}
	// Remove old state so StartNode can proceed.
	delete(o.nodes, id)
	o.mu.Unlock()

	return o.StartNode(id)
}

// IsAlive returns true if the node process is currently running.
func (o *Orchestrator) IsAlive(id int) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	ns, ok := o.nodes[id]
	return ok && ns.alive
}

// StatusResponse represents the /api/status response.
type StatusResponse struct {
	NodeID int    `json:"node_id"`
	Role   string `json:"role"`
	Epoch  int    `json:"epoch"`
}

// WaitForCluster polls all nodes until at least one reports role "coordinator".
// Returns the coordinator's node ID, or an error if the timeout is reached.
func (o *Orchestrator) WaitForCluster(timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 2 * time.Second}

	for time.Now().Before(deadline) {
		for i := 0; i < o.config.NodeCount; i++ {
			if !o.IsAlive(i) {
				continue
			}
			url := fmt.Sprintf("http://%s/api/status", o.NodeAddr(i))
			resp, err := client.Get(url)
			if err != nil {
				continue
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				continue
			}
			var status StatusResponse
			if err := json.Unmarshal(body, &status); err != nil {
				continue
			}
			if status.Role == "coordinator" {
				return i, nil
			}
		}
		time.Sleep(250 * time.Millisecond)
	}

	return -1, fmt.Errorf("no coordinator elected within %s", timeout)
}

// CopyLogs copies all node log files to the given destination directory.
func (o *Orchestrator) CopyLogs(destDir string) error {
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return err
	}
	for i := 0; i < o.config.NodeCount; i++ {
		src := filepath.Join(o.config.LogDir, fmt.Sprintf("node-%d.log", i))
		dst := filepath.Join(destDir, fmt.Sprintf("node-%d.log", i))
		if err := copyFile(src, dst); err != nil {
			// Log file might not exist if node was never started; skip.
			continue
		}
	}
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}
