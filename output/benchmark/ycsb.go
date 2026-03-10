package benchmark

import (
	"fmt"
	"math/rand"
)

// OperationType represents the type of operation in the YCSB workload.
type OperationType int

const (
	Read OperationType = iota
	Insert
	Update
	Delete
)

// Operation represents a single YCSB operation.
type Operation struct {
	Type  OperationType
	Key   string
	Value string
}

// WorkloadConfig holds the configuration for the YCSB workload.
type WorkloadConfig struct {
	ReadProportion   float64
	InsertProportion float64
	UpdateProportion float64
	DeleteProportion float64
	KeySpaceSize     int
}

// GenerateWorkload generates a list of operations based on the given configuration.
func GenerateWorkload(config WorkloadConfig, numOperations int) []Operation {
	operations := make([]Operation, numOperations)
	for i := 0; i < numOperations; i++ {
		opType := selectOperationType(config)
		key := generateKey(config.KeySpaceSize)
		value := generateValue()
		operations[i] = Operation{Type: opType, Key: key, Value: value}
	}
	return operations
}

func selectOperationType(config WorkloadConfig) OperationType {
	r := rand.Float64()
	switch {
	case r < config.ReadProportion:
		return Read
	case r < config.ReadProportion+config.InsertProportion:
		return Insert
	case r < config.ReadProportion+config.InsertProportion+config.UpdateProportion:
		return Update
	default:
		return Delete
	}
}

func generateKey(keySpaceSize int) string {
	return fmt.Sprintf("key-%d", rand.Intn(keySpaceSize))
}

func generateValue() string {
	return fmt.Sprintf("value-%d", rand.Int())
}