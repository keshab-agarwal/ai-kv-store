package kvstore

import (
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const maxValueSize = 1 << 20 // 1 MiB

// handleKV is the HTTP handler for /kv/{hexkey}.
func (n *Node) handleKV(w http.ResponseWriter, r *http.Request) {
	// Parse key from URL path
	path := strings.TrimPrefix(r.URL.Path, "/kv/")
	if path == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	keyBytes, err := hex.DecodeString(path)
	if err != nil || len(keyBytes) != 16 {
		http.Error(w, "key must be 32 hex chars (16 bytes)", http.StatusBadRequest)
		return
	}

	var key [16]byte
	copy(key[:], keyBytes)

	switch r.Method {
	case http.MethodGet:
		n.handleGet(w, key)
	case http.MethodPut:
		n.handlePut(w, r, key)
	case http.MethodDelete:
		n.handleDelete(w, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (n *Node) handleGet(w http.ResponseWriter, key [16]byte) {
	result := n.ClientGet(key)
	if err := writeResult(w, result, true); err != nil {
		return
	}
}

func (n *Node) handlePut(w http.ResponseWriter, r *http.Request, key [16]byte) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxValueSize+1))
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if len(body) > maxValueSize {
		http.Error(w, "value exceeds 1 MiB", http.StatusRequestEntityTooLarge)
		return
	}

	result := n.ClientPut(key, body)
	writeResult(w, result, false)
}

func (n *Node) handleDelete(w http.ResponseWriter, key [16]byte) {
	result := n.ClientDelete(key)
	writeResult(w, result, false)
}

func writeResult(w http.ResponseWriter, result OpResult, isGet bool) error {
	if result.Err != nil {
		errMsg := result.Err.Error()
		if strings.HasPrefix(errMsg, "redirect:") {
			leaderAddr := strings.TrimPrefix(errMsg, "redirect:")
			// Return 421 with leader hint
			w.Header().Set("X-Leader", leaderAddr)
			http.Error(w, fmt.Sprintf("not leader, try %s", leaderAddr), http.StatusMisdirectedRequest)
			return nil
		}
		if strings.Contains(errMsg, "no known leader") {
			http.Error(w, "no leader elected", http.StatusServiceUnavailable)
			return nil
		}
		http.Error(w, errMsg, http.StatusInternalServerError)
		return nil
	}
	if result.Timeout {
		http.Error(w, "TIMEOUT", http.StatusGatewayTimeout)
		return nil
	}
	if isGet {
		if !result.Found {
			http.Error(w, "NOT_FOUND", http.StatusNotFound)
			return nil
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		w.Write(result.Value)
		return nil
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
	return nil
}

// handleHealth returns node status for monitoring.
func (n *Node) handleHealth(w http.ResponseWriter, r *http.Request) {
	n.mu.Lock()
	role := n.role.String()
	phase := n.currentPhase
	leader := n.leaderID
	commit := n.commitIndex
	logLen := int64(len(n.log))
	keys := n.store.Len()
	n.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"id":%d,"role":"%s","phase":%d,"leader":%d,"commitIndex":%d,"logLength":%d,"keys":%d}`,
		n.id, role, phase, leader, commit, logLen, keys)
}
