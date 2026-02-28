package correctness

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

type NodeHandle struct {
	ID         uint64
	RPCPort    int
	ClientPort int
	Cmd        *exec.Cmd
	LogFile    *os.File
	Running    bool
}

type Orchestrator struct {
	nodes    []*NodeHandle
	binPath  string
	logDir   string
	baseRPC  int
	baseHTTP int
	logger   *log.Logger
}

func NewOrchestrator(nodeCount int, binPath string, logDir string, logger *log.Logger) *Orchestrator {
	o := &Orchestrator{
		binPath:  binPath,
		logDir:   logDir,
		baseRPC:  19001,
		baseHTTP: 18001,
		logger:   logger,
	}
	for i := 0; i < nodeCount; i++ {
		o.nodes = append(o.nodes, &NodeHandle{
			ID:         uint64(i + 1),
			RPCPort:    o.baseRPC + i,
			ClientPort: o.baseHTTP + i,
		})
	}
	return o
}

func (o *Orchestrator) NodeCount() int { return len(o.nodes) }

func (o *Orchestrator) ClientAddrs() []string {
	addrs := make([]string, len(o.nodes))
	for i, n := range o.nodes {
		addrs[i] = fmt.Sprintf("localhost:%d", n.ClientPort)
	}
	return addrs
}

func (o *Orchestrator) StartAll() error {
	for _, n := range o.nodes {
		if err := o.StartNode(n.ID); err != nil {
			return err
		}
	}
	time.Sleep(2 * time.Second)
	return nil
}

func (o *Orchestrator) StopAll() {
	for _, n := range o.nodes {
		o.StopNode(n.ID)
	}
}

func (o *Orchestrator) StartNode(id uint64) error {
	n := o.getNode(id)
	if n == nil {
		return fmt.Errorf("unknown node %d", id)
	}
	if n.Running {
		return nil
	}

	peers := o.buildPeersArg(id)
	args := []string{
		"-id", fmt.Sprintf("%d", id),
		"-rpc", fmt.Sprintf(":%d", n.RPCPort),
		"-client", fmt.Sprintf(":%d", n.ClientPort),
		"-peers", peers,
	}

	logPath := filepath.Join(o.logDir, fmt.Sprintf("node%d.log", id))
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open log %s: %w", logPath, err)
	}

	cmd := exec.Command(o.binPath, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		logFile.Close()
		return fmt.Errorf("start node %d: %w", id, err)
	}

	n.Cmd = cmd
	n.LogFile = logFile
	n.Running = true
	o.logger.Printf("started node %d (pid=%d)", id, cmd.Process.Pid)
	return nil
}

func (o *Orchestrator) StopNode(id uint64) {
	n := o.getNode(id)
	if n == nil || !n.Running {
		return
	}
	n.Cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- n.Cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		n.Cmd.Process.Kill()
		<-done
	}
	if n.LogFile != nil {
		n.LogFile.Close()
	}
	n.Running = false
	o.logger.Printf("stopped node %d", id)
}

func (o *Orchestrator) CrashNode(id uint64) {
	n := o.getNode(id)
	if n == nil || !n.Running {
		return
	}
	n.Cmd.Process.Kill()
	n.Cmd.Wait()
	if n.LogFile != nil {
		n.LogFile.Close()
	}
	n.Running = false
	o.logger.Printf("crashed node %d", id)
}

func (o *Orchestrator) RecoverNode(id uint64) error {
	o.logger.Printf("recovering node %d", id)
	return o.StartNode(id)
}

func (o *Orchestrator) IsRunning(id uint64) bool {
	n := o.getNode(id)
	return n != nil && n.Running
}

func (o *Orchestrator) getNode(id uint64) *NodeHandle {
	for _, n := range o.nodes {
		if n.ID == id {
			return n
		}
	}
	return nil
}

func (o *Orchestrator) buildPeersArg(excludeID uint64) string {
	s := ""
	for _, n := range o.nodes {
		if n.ID == excludeID {
			continue
		}
		if s != "" {
			s += ","
		}
		s += fmt.Sprintf("%d=localhost:%d", n.ID, n.RPCPort)
	}
	return s
}
