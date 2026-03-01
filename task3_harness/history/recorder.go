// Package history provides thread-safe operation recording for linearizability checking.
package history

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// HistoryEntry records a single operation invocation and completion.
type HistoryEntry struct {
	OpID        int64  `json:"op_id"`
	ClientID    string `json:"client_id"`
	CallTime    int64  `json:"call_time"`    // nanoseconds since epoch
	ReturnTime  int64  `json:"return_time"`  // nanoseconds since epoch
	OpType      string `json:"op_type"`      // get, put, delete
	Key         string `json:"key"`          // hex-encoded
	InputValue  string `json:"input_value"`  // base64, for put
	OutputValue string `json:"output_value"` // base64, for get found
	Status      string `json:"status"`       // ok, found, not_found, error, timeout
}

// Recorder is a thread-safe operation recorder.
type Recorder struct {
	mu      sync.Mutex
	entries map[int64]*HistoryEntry
	nextID  int64
}

// NewRecorder creates a new Recorder.
func NewRecorder() *Recorder {
	return &Recorder{
		entries: make(map[int64]*HistoryEntry),
	}
}

// RecordCall records the invocation of an operation and returns an opID.
func (r *Recorder) RecordCall(clientID, opType, key, inputValue string) int64 {
	opID := atomic.AddInt64(&r.nextID, 1)
	entry := &HistoryEntry{
		OpID:       opID,
		ClientID:   clientID,
		CallTime:   time.Now().UnixNano(),
		OpType:     opType,
		Key:        key,
		InputValue: inputValue,
	}
	r.mu.Lock()
	r.entries[opID] = entry
	r.mu.Unlock()
	return opID
}

// RecordReturn records the completion of an operation.
func (r *Recorder) RecordReturn(opID int64, outputValue, status string) {
	returnTime := time.Now().UnixNano()
	r.mu.Lock()
	if entry, ok := r.entries[opID]; ok {
		entry.ReturnTime = returnTime
		entry.OutputValue = outputValue
		entry.Status = status
	}
	r.mu.Unlock()
}

// Entries returns a copy of all recorded entries, sorted by OpID.
func (r *Recorder) Entries() []HistoryEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]HistoryEntry, 0, len(r.entries))
	for _, e := range r.entries {
		result = append(result, *e)
	}
	return result
}

// Save writes all entries to a JSONL file.
func (r *Recorder) Save(filepath string) error {
	entries := r.Entries()
	f, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("creating history file: %w", err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	for _, entry := range entries {
		if err := enc.Encode(entry); err != nil {
			return fmt.Errorf("encoding entry: %w", err)
		}
	}
	return w.Flush()
}

// Load reads entries from a JSONL file.
func Load(filepath string) ([]HistoryEntry, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("opening history file: %w", err)
	}
	defer f.Close()

	var entries []HistoryEntry
	scanner := bufio.NewScanner(f)
	// Increase buffer size for large lines.
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var entry HistoryEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			return nil, fmt.Errorf("parsing line %d: %w", lineNum, err)
		}
		entries = append(entries, entry)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading history file: %w", err)
	}
	return entries, nil
}
