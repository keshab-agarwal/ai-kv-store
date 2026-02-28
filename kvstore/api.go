package kvstore

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
)

type APIServer struct {
	node     *Node
	listener net.Listener
	server   *http.Server
}

type PutRequest struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	ClientID  string `json:"client_id,omitempty"`
	ClientSeq uint64 `json:"client_seq,omitempty"`
}

type GetRequest struct {
	Key string `json:"key"`
}

type DeleteRequest struct {
	Key       string `json:"key"`
	ClientID  string `json:"client_id,omitempty"`
	ClientSeq uint64 `json:"client_seq,omitempty"`
}

type APIResponse struct {
	Status string `json:"status"`
	Value  string `json:"value,omitempty"`
}

type StatusResponse struct {
	NodeID        uint64 `json:"node_id"`
	Role          string `json:"role"`
	Epoch         uint64 `json:"epoch"`
	CommitIndex   int64  `json:"commit_index"`
	CoordinatorID uint64 `json:"coordinator_id"`
	StoreSize     int    `json:"store_size"`
}

func NewAPIServer(node *Node) *APIServer {
	return &APIServer{node: node}
}

func (a *APIServer) Start(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/put", a.handlePut)
	mux.HandleFunc("/api/get", a.handleGet)
	mux.HandleFunc("/api/delete", a.handleDelete)
	mux.HandleFunc("/api/status", a.handleStatus)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("api listen %s: %w", addr, err)
	}
	a.listener = ln
	a.server = &http.Server{Handler: mux}
	go a.server.Serve(ln)
	return nil
}

func (a *APIServer) Stop() {
	if a.server != nil {
		a.server.Close()
	}
}

func (a *APIServer) Addr() string {
	if a.listener == nil {
		return ""
	}
	return a.listener.Addr().String()
}

func (a *APIServer) handlePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req PutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, APIResponse{Status: "ERROR"})
		return
	}

	key, err := KeyFromHex(req.Key)
	if err != nil {
		writeJSON(w, APIResponse{Status: "ERROR"})
		return
	}
	value, err := base64.StdEncoding.DecodeString(req.Value)
	if err != nil {
		writeJSON(w, APIResponse{Status: "ERROR"})
		return
	}
	if len(value) > a.node.config.MaxValueSize {
		writeJSON(w, APIResponse{Status: "ERROR"})
		return
	}

	result := a.node.HandleWrite(OpPut, key, value, req.ClientID, req.ClientSeq)
	writeJSON(w, APIResponse{Status: result.Status.String()})
}

func (a *APIServer) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req GetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, APIResponse{Status: "ERROR"})
		return
	}

	key, err := KeyFromHex(req.Key)
	if err != nil {
		writeJSON(w, APIResponse{Status: "ERROR"})
		return
	}

	result := a.node.HandleRead(key)
	resp := APIResponse{Status: result.Status.String()}
	if result.Status == StatusFound {
		resp.Value = base64.StdEncoding.EncodeToString(result.Value)
	}
	writeJSON(w, resp)
}

func (a *APIServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req DeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, APIResponse{Status: "ERROR"})
		return
	}

	key, err := KeyFromHex(req.Key)
	if err != nil {
		writeJSON(w, APIResponse{Status: "ERROR"})
		return
	}

	result := a.node.HandleWrite(OpDelete, key, nil, req.ClientID, req.ClientSeq)
	writeJSON(w, APIResponse{Status: result.Status.String()})
}

func (a *APIServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	role, epoch, coordID := a.node.RoleInfo()
	resp := StatusResponse{
		NodeID:        a.node.NodeID(),
		Role:          role.String(),
		Epoch:         epoch,
		CoordinatorID: coordID,
		StoreSize:     a.node.StoreRef().Len(),
	}
	a.node.mu.Lock()
	resp.CommitIndex = a.node.commitIndex
	a.node.mu.Unlock()
	writeJSON(w, resp)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
