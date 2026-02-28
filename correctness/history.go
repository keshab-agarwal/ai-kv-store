package correctness

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// HistoryEvent is one invocation or completion in the execution history.
// Each operation has two events: invoke (CallTime set, ReturnTime zero) and
// complete (both set). We flatten them into one record per operation.
type HistoryEvent struct {
	OpID       int64  `json:"op_id"`
	ClientID   string `json:"client_id"`
	CallTime   int64  `json:"call_time_ns"`
	ReturnTime int64  `json:"return_time_ns"`
	OpType     string `json:"op_type"` // "Get", "Put", "Delete"
	Key        string `json:"key"`     // hex-encoded
	InputValue string `json:"input_value,omitempty"`
	Output     string `json:"output"`        // "OK", "FOUND", "NOT_FOUND", "ERROR", "TIMEOUT"
	OutputValue string `json:"output_value,omitempty"`
}

// HistoryRecorder collects and writes operation history as JSONL.
type HistoryRecorder struct {
	mu      sync.Mutex
	file    *os.File
	writer  *bufio.Writer
	encoder *json.Encoder
}

func NewHistoryRecorder(path string) (*HistoryRecorder, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create history file: %w", err)
	}
	w := bufio.NewWriter(f)
	return &HistoryRecorder{
		file:    f,
		writer:  w,
		encoder: json.NewEncoder(w),
	}, nil
}

func (h *HistoryRecorder) Record(event HistoryEvent) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.encoder.Encode(event)
}

func (h *HistoryRecorder) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.writer.Flush()
	return h.file.Close()
}

func LoadHistory(path string) ([]HistoryEvent, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var events []HistoryEvent
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 4*1024*1024), 4*1024*1024)
	for scanner.Scan() {
		var ev HistoryEvent
		if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
			continue
		}
		events = append(events, ev)
	}
	return events, scanner.Err()
}

func NowNano() int64 {
	return time.Now().UnixNano()
}

// --- Summary report ---

type Summary struct {
	Pass         bool   `json:"pass"`
	TotalOps     int    `json:"total_ops"`
	Gets         int    `json:"gets"`
	Puts         int    `json:"puts"`
	Deletes      int    `json:"deletes"`
	Errors       int    `json:"errors"`
	Timeouts     int    `json:"timeouts"`
	ElapsedSec   float64 `json:"elapsed_sec"`
	FaultEvents  int    `json:"fault_events"`
	CheckerResult string `json:"checker_result"`
}

func WriteSummary(path string, s Summary) error {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}
