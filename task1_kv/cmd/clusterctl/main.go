package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type ClusterState struct {
	Peers map[int]string `json:"peers"`
	PIDs  map[int]int    `json:"pids"`
}

func main() {
	var (
		cmd      = flag.String("cmd", "", "start|stop|crash|recover|status")
		nodes    = flag.Int("nodes", 3, "number of nodes")
		node     = flag.Int("node", -1, "target node id for stop/crash/recover")
		basePort = flag.Int("base-port", 9001, "base port")
		stateDir = flag.String("state-dir", ".run/cluster", "state dir")
		nodeBin  = flag.String("node-bin", "./bin/kvnode", "node binary")
		httpWait = flag.Duration("wait", 3*time.Second, "startup wait")
	)
	flag.Parse()
	if *cmd == "" {
		fatal("-cmd required")
	}

	switch *cmd {
	case "start":
		must(startCluster(*nodes, *basePort, *stateDir, *nodeBin, *httpWait))
	case "stop":
		must(stopCluster(*stateDir))
	case "status":
		must(printStatus(*stateDir))
	case "crash":
		if *node < 0 {
			fatal("-node required")
		}
		must(killNode(*stateDir, *node, syscall.SIGKILL))
	case "recover":
		if *node < 0 {
			fatal("-node required")
		}
		must(recoverNode(*stateDir, *node, *nodeBin, *httpWait))
	default:
		fatal("unknown cmd")
	}
}

func startCluster(nodes, basePort int, stateDir, nodeBin string, wait time.Duration) error {
	if err := os.MkdirAll(filepath.Join(stateDir, "logs"), 0o755); err != nil {
		return err
	}
	st := ClusterState{Peers: map[int]string{}, PIDs: map[int]int{}}
	for i := 0; i < nodes; i++ {
		st.Peers[i] = fmt.Sprintf("http://127.0.0.1:%d", basePort+i)
	}
	peerStr := peersArg(st.Peers)
	for i := 0; i < nodes; i++ {
		logPath := filepath.Join(stateDir, "logs", fmt.Sprintf("node-%d.log", i))
		f, err := os.Create(logPath)
		if err != nil {
			return err
		}
		proc := exec.Command(nodeBin,
			"-node-id", strconv.Itoa(i),
			"-addr", st.Peers[i],
			"-listen", strings.TrimPrefix(st.Peers[i], "http://"),
			"-peers", peerStr,
		)
		proc.Stdout = f
		proc.Stderr = f
		if err := proc.Start(); err != nil {
			_ = f.Close()
			return err
		}
		st.PIDs[i] = proc.Process.Pid
		_ = f.Close()
	}
	if err := saveState(stateDir, st); err != nil {
		return err
	}
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		ok := true
		for i := 0; i < nodes; i++ {
			if !isHealthy(st.Peers[i]) {
				ok = false
				break
			}
		}
		if ok {
			fmt.Printf("cluster started with %d nodes\n", nodes)
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("cluster did not become healthy before timeout")
}

func stopCluster(stateDir string) error {
	st, err := loadState(stateDir)
	if err != nil {
		return err
	}
	for nodeID := range st.PIDs {
		_ = killNode(stateDir, nodeID, syscall.SIGTERM)
	}
	_ = os.Remove(filepath.Join(stateDir, "cluster_state.json"))
	fmt.Println("cluster stopped")
	return nil
}

func recoverNode(stateDir string, nodeID int, nodeBin string, wait time.Duration) error {
	st, err := loadState(stateDir)
	if err != nil {
		return err
	}
	if _, ok := st.Peers[nodeID]; !ok {
		return fmt.Errorf("unknown node %d", nodeID)
	}
	if pid, ok := st.PIDs[nodeID]; ok && processAlive(pid) {
		return fmt.Errorf("node %d already alive (pid %d)", nodeID, pid)
	}
	logPath := filepath.Join(stateDir, "logs", fmt.Sprintf("node-%d.log", nodeID))
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	peerStr := peersArg(st.Peers)
	proc := exec.Command(nodeBin,
		"-node-id", strconv.Itoa(nodeID),
		"-addr", st.Peers[nodeID],
		"-listen", strings.TrimPrefix(st.Peers[nodeID], "http://"),
		"-peers", peerStr,
	)
	proc.Stdout = f
	proc.Stderr = f
	if err := proc.Start(); err != nil {
		_ = f.Close()
		return err
	}
	_ = f.Close()
	st.PIDs[nodeID] = proc.Process.Pid
	if err := saveState(stateDir, st); err != nil {
		return err
	}
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		if isHealthy(st.Peers[nodeID]) {
			fmt.Printf("node %d recovered\n", nodeID)
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("node %d recover health timeout", nodeID)
}

func killNode(stateDir string, nodeID int, sig syscall.Signal) error {
	st, err := loadState(stateDir)
	if err != nil {
		return err
	}
	pid, ok := st.PIDs[nodeID]
	if !ok {
		return fmt.Errorf("node %d has no pid", nodeID)
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	_ = p.Signal(sig)
	delete(st.PIDs, nodeID)
	if err := saveState(stateDir, st); err != nil {
		return err
	}
	fmt.Printf("node %d sent %s\n", nodeID, sig.String())
	return nil
}

func printStatus(stateDir string) error {
	st, err := loadState(stateDir)
	if err != nil {
		return err
	}
	for id, addr := range st.Peers {
		pid := st.PIDs[id]
		fmt.Printf("node=%d addr=%s pid=%d alive=%t healthy=%t\n", id, addr, pid, processAlive(pid), isHealthy(addr))
	}
	return nil
}

func isHealthy(addr string) bool {
	resp, err := http.Get(addr + "/healthz")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil
}

func peersArg(peers map[int]string) string {
	parts := make([]string, 0, len(peers))
	for i := 0; i < len(peers); i++ {
		parts = append(parts, fmt.Sprintf("%d=%s", i, peers[i]))
	}
	return strings.Join(parts, ",")
}

func loadState(stateDir string) (ClusterState, error) {
	b, err := os.ReadFile(filepath.Join(stateDir, "cluster_state.json"))
	if err != nil {
		return ClusterState{}, err
	}
	var st ClusterState
	if err := json.Unmarshal(b, &st); err != nil {
		return ClusterState{}, err
	}
	if st.PIDs == nil {
		st.PIDs = map[int]int{}
	}
	return st, nil
}

func saveState(stateDir string, st ClusterState) error {
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(stateDir, "cluster_state.json"), b, 0o644)
}

func must(err error) {
	if err != nil {
		fatal(err.Error())
	}
}

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
