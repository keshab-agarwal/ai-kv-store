// Package correctness provides history recording and linearizability checking
// for a distributed key-value store using the Porcupine checker.
package correctness

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

// HistoryEvent represents a single client operation against the KV store.
// Timestamps are nanoseconds from a monotonic-ish clock (time.Now().UnixNano()).
type HistoryEvent struct {
	OpID        int64  `json:"op_id"`
	ClientID    int    `json:"client_id"`
	CallTime    int64  `json:"call_time"`
	ReturnTime  int64  `json:"return_time"`
	OpType      string `json:"op_type"`
	Key         string `json:"key"`
	InputValue  string `json:"input_value,omitempty"`
	OutputValue string `json:"output_value,omitempty"`
	Status      string `json:"status"`
}

// HistoryRecorder collects HistoryEvents in a thread-safe manner.
type HistoryRecorder struct {
	mu     sync.Mutex
	events []HistoryEvent
	nextID int64
}

// NewHistoryRecorder creates a new empty HistoryRecorder.
func NewHistoryRecorder() *HistoryRecorder {
	return &HistoryRecorder{}
}

// NextOpID returns a unique, monotonically increasing operation ID.
func (hr *HistoryRecorder) NextOpID() int64 {
	return atomic.AddInt64(&hr.nextID, 1)
}

// Record appends a HistoryEvent to the recorder.
func (hr *HistoryRecorder) Record(ev HistoryEvent) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.events = append(hr.events, ev)
}

// Events returns a copy of all recorded events.
func (hr *HistoryRecorder) Events() []HistoryEvent {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	out := make([]HistoryEvent, len(hr.events))
	copy(out, hr.events)
	return out
}

// WriteJSONL writes all recorded events to a JSONL file (one JSON object per line).
func (hr *HistoryRecorder) WriteJSONL(path string) error {
	hr.mu.Lock()
	events := make([]HistoryEvent, len(hr.events))
	copy(events, hr.events)
	hr.mu.Unlock()

	return WriteHistoryJSONL(path, events)
}

// WriteHistoryJSONL writes a slice of HistoryEvents to a JSONL file.
func WriteHistoryJSONL(path string, events []HistoryEvent) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create history file: %w", err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	for _, ev := range events {
		if err := enc.Encode(ev); err != nil {
			return fmt.Errorf("encode event: %w", err)
		}
	}
	if err := w.Flush(); err != nil {
		return fmt.Errorf("flush history file: %w", err)
	}
	return nil
}

// ReadHistoryJSONL reads a JSONL history file and returns the events.
func ReadHistoryJSONL(path string) ([]HistoryEvent, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open history file: %w", err)
	}
	defer f.Close()

	var events []HistoryEvent
	scanner := bufio.NewScanner(f)
	// Allow lines up to 2 MiB (values can be up to 1 MiB hex-encoded).
	scanner.Buffer(make([]byte, 0, 4*1024*1024), 4*1024*1024)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var ev HistoryEvent
		if err := json.Unmarshal(line, &ev); err != nil {
			return nil, fmt.Errorf("parse line %d: %w", lineNum, err)
		}
		events = append(events, ev)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan history file: %w", err)
	}
	return events, nil
}
