package kvstore

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
)

// ---------------------------------------------------------------------------
// Client-facing HTTP API types
// ---------------------------------------------------------------------------

// GetRequest is the body for POST /api/get.
type GetRequest struct {
	Key string `json:"key"` // 32-char hex string
}

// GetResponse is the body returned from POST /api/get.
type GetResponse struct {
	Status string `json:"status"`
	Value  string `json:"value,omitempty"`  // base64-encoded
	Leader string `json:"leader,omitempty"` // only set on redirect
}

// PutRequest is the body for POST /api/put.
type PutRequest struct {
	Key       string `json:"key"`        // 32-char hex string
	Value     string `json:"value"`      // base64-encoded
	ClientID  string `json:"client_id"`
	ClientSeq uint64 `json:"client_seq"`
}

// PutResponse is the body returned from POST /api/put.
type PutResponse struct {
	Status string `json:"status"`
	Leader string `json:"leader,omitempty"`
}

// DeleteRequest is the body for POST /api/delete.
type DeleteRequest struct {
	Key       string `json:"key"`
	ClientID  string `json:"client_id"`
	ClientSeq uint64 `json:"client_seq"`
}

// DeleteResponse is the body returned from POST /api/delete.
type DeleteResponse struct {
	Status string `json:"status"`
	Leader string `json:"leader,omitempty"`
}

// StatusResponse is the body returned from GET /api/status.
type StatusResponse struct {
	NodeID           int    `json:"node_id"`
	Role             string `json:"role"`
	Epoch            uint64 `json:"epoch"`
	CommitIndex      uint64 `json:"commit_index"`
	LeaderID         int    `json:"leader_id"`
	LeaderClientAddr string `json:"leader_client_addr"`
	Keys             int    `json:"keys"`
}

// ---------------------------------------------------------------------------
// Inter-node RPC types
// ---------------------------------------------------------------------------

// VoteRequest is sent during an election by a Candidate.
type VoteRequest struct {
	Epoch               uint64 `json:"epoch"`
	CandidateID         int    `json:"candidate_id"`
	LastLogIndex        uint64 `json:"last_log_index"`
	LastLogEpoch        uint64 `json:"last_log_epoch"`
	CandidateClientAddr string `json:"candidate_client_addr"`
}

// VoteResponse is the reply to a VoteRequest.
type VoteResponse struct {
	Epoch   uint64 `json:"epoch"`
	Granted bool   `json:"granted"`
}

// PulseRequest is the PhotonPulse RPC payload (heartbeat + log replication combined).
type PulseRequest struct {
	Epoch            uint64     `json:"epoch"`
	LeaderID         int        `json:"leader_id"`
	LeaderClientAddr string     `json:"leader_client_addr"`
	LeaseExpiry      int64      `json:"lease_expiry"` // unix nano
	PrevLogIndex     uint64     `json:"prev_log_index"`
	PrevLogEpoch     uint64     `json:"prev_log_epoch"`
	Entries          []LogEntry `json:"entries"`
	CommitIndex      uint64     `json:"commit_index"`
}

// PulseResponse is the reply to a PulseRequest.
type PulseResponse struct {
	Epoch      uint64 `json:"epoch"`
	Success    bool   `json:"success"`
	MatchIndex uint64 `json:"match_index"`
	NodeID     int    `json:"node_id"`
}

// SnapshotData holds the key-value pairs from a snapshot.
// Values are stored as raw strings (the actual binary encoding happens at the
// HTTP boundary via base64 in the snapshot transfer path).
type SnapshotData struct {
	Keys map[string]string `json:"keys"` // key -> base64-encoded value
}

// SnapshotRequest2 is the full snapshot install RPC payload.
type SnapshotRequest2 struct {
	Epoch             uint64       `json:"epoch"`
	LeaderID          int          `json:"leader_id"`
	LastIncludedIndex uint64       `json:"last_included_index"`
	LastIncludedEpoch uint64       `json:"last_included_epoch"`
	Data              SnapshotData `json:"data"`
}

// SnapshotResponse is the reply to a SnapshotRequest2.
type SnapshotResponse struct {
	Epoch   uint64 `json:"epoch"`
	Success bool   `json:"success"`
}

// ---------------------------------------------------------------------------
// Client HTTP server
// ---------------------------------------------------------------------------

// SetupClientServer returns an http.Handler that serves the client-facing API.
func SetupClientServer(n *Node) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/get", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req GetRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
			return
		}
		status, val := n.HandleGet(req.Key)
		resp := GetResponse{Status: status}
		if strings.HasPrefix(status, "redirect") {
			// val contains the leader address.
			resp.Leader = string(val)
			resp.Status = "redirect"
		} else if val != nil {
			resp.Value = base64.StdEncoding.EncodeToString(val)
		}
		writeJSON(w, resp)
	})

	mux.HandleFunc("/api/put", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req PutRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
			return
		}
		rawVal, err := base64.StdEncoding.DecodeString(req.Value)
		if err != nil {
			http.Error(w, "bad request: value must be base64: "+err.Error(), http.StatusBadRequest)
			return
		}
		status, writeErr := n.HandlePut(req.Key, rawVal, req.ClientID, req.ClientSeq)
		resp := PutResponse{}
		if writeErr != nil {
			resp.Status = StatusError.String()
			writeJSON(w, resp)
			return
		}
		if strings.HasPrefix(status, "redirect:") {
			resp.Status = "redirect"
			resp.Leader = strings.TrimPrefix(status, "redirect:")
		} else {
			resp.Status = status
		}
		writeJSON(w, resp)
	})

	mux.HandleFunc("/api/delete", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req DeleteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
			return
		}
		status, writeErr := n.HandleDelete(req.Key, req.ClientID, req.ClientSeq)
		resp := DeleteResponse{}
		if writeErr != nil {
			resp.Status = StatusError.String()
			writeJSON(w, resp)
			return
		}
		if strings.HasPrefix(status, "redirect:") {
			resp.Status = "redirect"
			resp.Leader = strings.TrimPrefix(status, "redirect:")
		} else {
			resp.Status = status
		}
		writeJSON(w, resp)
	})

	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		writeJSON(w, n.StatusInfo())
	})

	return mux
}

// ---------------------------------------------------------------------------
// RPC HTTP server
// ---------------------------------------------------------------------------

// SetupRPCServer returns an http.Handler that serves the inter-node RPC API.
func SetupRPCServer(n *Node) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/rpc/vote", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req VoteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
			return
		}
		resp := n.HandleVote(req)
		writeJSON(w, resp)
	})

	mux.HandleFunc("/rpc/pulse", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req PulseRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
			return
		}
		resp := n.HandlePulse(req)
		writeJSON(w, resp)
	})

	mux.HandleFunc("/rpc/snapshot", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req SnapshotRequest2
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
			return
		}
		// Decode base64-encoded values in snapshot data.
		decoded := make(map[string]string, len(req.Data.Keys))
		for k, b64v := range req.Data.Keys {
			raw, err := base64.StdEncoding.DecodeString(b64v)
			if err != nil {
				// If not base64, treat as raw string.
				decoded[k] = b64v
			} else {
				decoded[k] = string(raw)
			}
		}
		req.Data.Keys = decoded
		resp := n.HandleSnapshot(req)
		writeJSON(w, resp)
	})

	return mux
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// writeJSON encodes v as JSON and writes it to w with Content-Type header.
// All errors are returned as JSON with status 200 so clients can always parse.
func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// Last-resort: the header is already sent so we can't change the status.
		_ = err
	}
}
