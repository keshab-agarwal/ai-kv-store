package main

import (
	"fmt"
	"kvstore/benchmark"
	"kvstore/kv"
	"time"
)

func main() {
	// Configuration for the YCSB workload
	config := benchmark.WorkloadConfig{
		ReadProportion:   0.5,
		InsertProportion: 0.2,
		UpdateProportion: 0.2,
		DeleteProportion: 0.1,
		KeySpaceSize:     1000,
	}

	// Generate a workload
	operations := benchmark.GenerateWorkload(config, 1000)

	// Create a client to interact with the KV store
	client := kv.NewClient(":8080")

	// Execute the operations
	start := time.Now()
	for _, op := range operations {
		switch op.Type {
		case benchmark.Read:
			client.Get(op.Key)
		case benchmark.Insert, benchmark.Update:
			client.Put(op.Key, op.Value)
		case benchmark.Delete:
			client.Delete(op.Key)
		}
	}
	duration := time.Since(start)

	// Output the benchmark results
	fmt.Printf("Executed %d operations in %v\n", len(operations), duration)
}
