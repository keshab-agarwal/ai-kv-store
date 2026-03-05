package ycsb

import "fmt"

// Status represents the outcome of a database operation.
type Status int

const (
	StatusOK       Status = iota
	StatusError
	StatusNotFound
	StatusFound
)

// DB is the interface that database bindings must implement.
type DB interface {
	Read(key string) (Status, []byte, error)
	Insert(key string, value []byte) (Status, error)
	Update(key string, value []byte) (Status, error)
	Delete(key string) (Status, error)
	Scan(startKey string, count int) (Status, error)
}

// WorkloadConfig defines the parameters for a YCSB workload.
type WorkloadConfig struct {
	RecordCount      int64
	OperationCount   int64
	ReadProportion   float64
	UpdateProportion float64
	InsertProportion float64
	DeleteProportion float64
	ScanProportion   float64
	ZipfianTheta     float64
	ValueSize        int
	ThreadCount      int
}

// BuildKeyName maps a key index to a 128-bit hex-encoded string (32 chars).
func BuildKeyName(index int64) string {
	return fmt.Sprintf("%032x", index)
}
