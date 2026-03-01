// Package binding implements a YCSB-compatible binding for the KV store.
//
// It maps the standard YCSB DB interface operations to the KV store client:
//
//	Read   -> client.Get
//	Insert -> client.Put
//	Update -> client.Put
//	Delete -> client.Delete
//	Scan   -> unsupported (returns error)
package binding

import (
	"context"
	"fmt"
	"time"

	"ai-kv-store/shared/types"
	"ai-kv-store/task1_kv/client"
)

// Status represents the result status of a YCSB operation.
type Status int

const (
	StatusOK          Status = 0
	StatusError       Status = 1
	StatusNotFound    Status = 2
	StatusNotImpl     Status = 3
	StatusTimeout     Status = 4
)

func (s Status) String() string {
	switch s {
	case StatusOK:
		return "OK"
	case StatusError:
		return "ERROR"
	case StatusNotFound:
		return "NOT_FOUND"
	case StatusNotImpl:
		return "NOT_IMPLEMENTED"
	case StatusTimeout:
		return "TIMEOUT"
	default:
		return "UNKNOWN"
	}
}

// Binding implements the YCSB DB interface for the KV store.
type Binding struct {
	client *client.Client
}

// New creates a new Binding connected to the KV store at the given address.
func New(addr string, timeout time.Duration) *Binding {
	return &Binding{
		client: client.New(addr, timeout),
	}
}

// Read fetches the value for the given key. The table and fields parameters
// are accepted for YCSB interface compatibility but are ignored (our KV store
// is a single flat keyspace with opaque values).
func (b *Binding) Read(table string, key types.Key, fields []string) (map[string][]byte, Status) {
	result := b.client.Get(context.Background(), key)
	switch result.Status {
	case types.StatusFound:
		return map[string][]byte{"field0": result.Value}, StatusOK
	case types.StatusNotFound:
		return nil, StatusNotFound
	case types.StatusTimeout:
		return nil, StatusTimeout
	default:
		return nil, StatusError
	}
}

// Insert stores a new key-value pair. The table parameter is ignored.
func (b *Binding) Insert(table string, key types.Key, values map[string][]byte) Status {
	// Flatten fields into a single value (YCSB uses field0 for single-field workloads).
	var value []byte
	for _, v := range values {
		value = v
		break
	}
	result := b.client.Put(context.Background(), key, value)
	return mapWriteStatus(result)
}

// Update modifies the value for an existing key. Maps to Put since our KV
// store does not distinguish insert from update.
func (b *Binding) Update(table string, key types.Key, values map[string][]byte) Status {
	var value []byte
	for _, v := range values {
		value = v
		break
	}
	result := b.client.Put(context.Background(), key, value)
	return mapWriteStatus(result)
}

// Delete removes the given key. The table parameter is ignored.
func (b *Binding) Delete(table string, key types.Key) Status {
	result := b.client.Delete(context.Background(), key)
	return mapWriteStatus(result)
}

// Scan is not supported by this KV store. It returns StatusNotImpl.
func (b *Binding) Scan(table string, startKey types.Key, count int, fields []string) ([]map[string][]byte, Status) {
	return nil, StatusNotImpl
}

// Close releases resources held by the binding.
func (b *Binding) Close() error {
	return b.client.Close()
}

// String returns a human-readable description of the binding.
func (b *Binding) String() string {
	return fmt.Sprintf("KVStoreBinding")
}

func mapWriteStatus(result types.Result) Status {
	switch result.Status {
	case types.StatusOK:
		return StatusOK
	case types.StatusTimeout:
		return StatusTimeout
	default:
		return StatusError
	}
}
