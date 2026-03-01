package harness

import "time"

type OperationType string

const (
	OpGet    OperationType = "get"
	OpPut    OperationType = "put"
	OpDelete OperationType = "delete"
)

type OpResultStatus string

const (
	StatusOK       OpResultStatus = "OK"
	StatusFound    OpResultStatus = "FOUND"
	StatusNotFound OpResultStatus = "NOT_FOUND"
	StatusError    OpResultStatus = "ERROR"
	StatusTimeout  OpResultStatus = "TIMEOUT"
)

type HistoryRecord struct {
	OpID        string         `json:"op_id"`
	ClientID    int            `json:"client_id"`
	CallTime    time.Time      `json:"call_time"`
	ReturnTime  *time.Time     `json:"return_time,omitempty"`
	OpType      OperationType  `json:"op_type"`
	Key         string         `json:"key"`
	InputValue  []byte         `json:"input_value,omitempty"`
	OutputValue []byte         `json:"output_value,omitempty"`
	Status      OpResultStatus `json:"status"`
	Error       string         `json:"error,omitempty"`
}

type RunSummary struct {
	Seed          int64         `json:"seed"`
	Duration      time.Duration `json:"duration"`
	Workers       int           `json:"workers"`
	Operations    int64         `json:"operations"`
	Gets          int64         `json:"gets"`
	Puts          int64         `json:"puts"`
	Deletes       int64         `json:"deletes"`
	Timeouts      int64         `json:"timeouts"`
	Errors        int64         `json:"errors"`
	CheckerResult string        `json:"checker_result"`
	CheckerNotes  string        `json:"checker_notes"`
}
