// Package correctness provides history recording, orchestration, workload
// generation, and fault injection for distributed KV store linearizability testing.
package correctness

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

// OpType identifies the operation type.
type OpType string

const (
	OpGet    OpType = "get"
	OpPut    OpType = "put"
	OpDelete OpType = "delete"
)

// HistoryEntry records one completed operation.
type HistoryEntry struct {
	OpID         int64  `json:"op_id"`
	ClientID     int    `json:"client_id"`
	CallTimeNs   int64  `json:"call_time_ns"`   // Unix nanoseconds
	ReturnTimeNs int64  `json:"return_time_ns"` // Unix nanoseconds
	OpType       OpType `json:"op_type"`
	Key          string `json:"key"`          // 32-hex string
	InputValue   string `json:"input_value"`  // base64 for put, "" for get/delete
	Status       string `json:"status"`       // "ok","found","not_found","error","timeout"
	OutputValue  string `json:"output_value"` // base64 for get/found, "" otherwise
}

// History manages the collection of operation records.
type History struct {
	mu      sync.Mutex
	entries []HistoryEntry
	nextID  int64

	// pending maps opID -> index into entries (for EndOp lookup)
	pending map[int64]int
}

// NewHistory creates a new empty History.
func NewHistory() *History {
	return &History{
		pending: make(map[int64]int),
	}
}

// BeginOp allocates an op_id and records the call time. Returns the op_id.
// The entry is added to the pending map so EndOp can complete it.
func (h *History) BeginOp(clientID int, opType OpType, key, inputValue string, callTimeNs int64) int64 {
	opID := atomic.AddInt64(&h.nextID, 1)

	entry := HistoryEntry{
		OpID:        opID,
		ClientID:    clientID,
		CallTimeNs:  callTimeNs,
		OpType:      opType,
		Key:         key,
		InputValue:  inputValue,
		ReturnTimeNs: 0, // filled by EndOp
	}

	h.mu.Lock()
	idx := len(h.entries)
	h.entries = append(h.entries, entry)
	h.pending[opID] = idx
	h.mu.Unlock()

	return opID
}

// EndOp records the return time and result for the given op_id.
func (h *History) EndOp(opID int64, returnTimeNs int64, status, outputValue string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	idx, ok := h.pending[opID]
	if !ok {
		return
	}
	h.entries[idx].ReturnTimeNs = returnTimeNs
	h.entries[idx].Status = status
	h.entries[idx].OutputValue = outputValue
	delete(h.pending, opID)
}

// Entries returns a copy of all recorded entries.
func (h *History) Entries() []HistoryEntry {
	h.mu.Lock()
	defer h.mu.Unlock()

	cp := make([]HistoryEntry, len(h.entries))
	copy(cp, h.entries)
	return cp
}

// WriteJSONL writes the history to a JSONL file (one JSON object per line).
func (h *History) WriteJSONL(path string) error {
	h.mu.Lock()
	entries := make([]HistoryEntry, len(h.entries))
	copy(entries, h.entries)
	h.mu.Unlock()

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create history file %s: %w", path, err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	for _, e := range entries {
		if err := enc.Encode(e); err != nil {
			return fmt.Errorf("encode history entry: %w", err)
		}
	}
	return w.Flush()
}

// LoadJSONL loads a history from a JSONL file.
func LoadJSONL(path string) ([]HistoryEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open history file %s: %w", path, err)
	}
	defer f.Close()

	var entries []HistoryEntry
	scanner := bufio.NewScanner(f)
	// Increase buffer size for potentially large JSON lines.
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var e HistoryEntry
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, fmt.Errorf("unmarshal history entry: %w", err)
		}
		entries = append(entries, e)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan history file: %w", err)
	}
	return entries, nil
}
