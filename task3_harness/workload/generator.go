// Package workload generates random KV operations for testing.
package workload

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"ai-kv-store/shared/types"
	"ai-kv-store/task1_kv/client"
	"ai-kv-store/task3_harness/history"
	"ai-kv-store/task3_harness/orchestrator"
)

// GeneratorConfig holds the configuration for the workload generator.
type GeneratorConfig struct {
	NumClients   int
	NumOps       int
	ReadPct      float64 // e.g., 0.95
	WritePct     float64 // e.g., 0.04
	DeletePct    float64 // e.g., 0.01
	MaxValueSize int
	KeyPoolSize  int
	Seed         int64
	Timeout      time.Duration // per-request timeout
}

// DefaultGeneratorConfig returns a config with sensible defaults.
func DefaultGeneratorConfig() GeneratorConfig {
	return GeneratorConfig{
		NumClients:   10,
		NumOps:       1000,
		ReadPct:      0.95,
		WritePct:     0.04,
		DeletePct:    0.01,
		MaxValueSize: 1024,
		KeyPoolSize:  100,
		Seed:         time.Now().UnixNano(),
		Timeout:      10 * time.Second,
	}
}

// Generator runs a configurable workload against the KV cluster.
type Generator struct {
	config GeneratorConfig
	keys   []types.Key
}

// NewGenerator creates a new workload generator.
func NewGenerator(config GeneratorConfig) *Generator {
	// Generate deterministic key pool.
	rng := rand.New(rand.NewSource(config.Seed))
	keys := make([]types.Key, config.KeyPoolSize)
	for i := range keys {
		var k types.Key
		// Use deterministic random bytes for keys.
		for j := 0; j < 16; j++ {
			k[j] = byte(rng.Intn(256))
		}
		keys[i] = k
	}
	return &Generator{
		config: config,
		keys:   keys,
	}
}

// Run executes the workload against the cluster, recording operations.
func (g *Generator) Run(ctx context.Context, orch *orchestrator.Orchestrator, rec *history.Recorder) {
	var opsCompleted int64
	totalOps := int64(g.config.NumOps)

	var wg sync.WaitGroup
	opsPerClient := g.config.NumOps / g.config.NumClients
	remainder := g.config.NumOps % g.config.NumClients

	for i := 0; i < g.config.NumClients; i++ {
		wg.Add(1)
		clientOps := opsPerClient
		if i < remainder {
			clientOps++
		}
		// Each client gets its own deterministic RNG seeded from the main seed + client index.
		clientSeed := g.config.Seed + int64(i)*1000000
		clientID := i

		go func(cID int, numOps int, seed int64) {
			defer wg.Done()
			g.runClient(ctx, orch, rec, cID, numOps, seed, &opsCompleted, totalOps)
		}(clientID, clientOps, clientSeed)
	}

	wg.Wait()
}

// runClient runs operations for a single client goroutine.
func (g *Generator) runClient(
	ctx context.Context,
	orch *orchestrator.Orchestrator,
	rec *history.Recorder,
	clientID int,
	numOps int,
	seed int64,
	opsCompleted *int64,
	totalOps int64,
) {
	rng := rand.New(rand.NewSource(seed))

	// Pick a node to connect to. Round-robin across alive nodes.
	nodeID := clientID % g.config.NumClients
	addr := findAliveNode(orch, nodeID)
	c := client.New(addr, g.config.Timeout)
	defer c.Close()

	clientIDStr := fmt.Sprintf("harness-client-%d", clientID)

	for op := 0; op < numOps; op++ {
		// Check if we've exceeded total ops or context is cancelled.
		if atomic.LoadInt64(opsCompleted) >= totalOps {
			return
		}
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Select operation type based on percentages.
		r := rng.Float64()
		key := g.keys[rng.Intn(len(g.keys))]
		keyHex := key.String()

		var opType string
		var inputValue string

		if r < g.config.ReadPct {
			// GET
			opType = "get"
			opID := rec.RecordCall(clientIDStr, opType, keyHex, "")
			result := c.Get(ctx, key)
			outputValue := ""
			status := result.Status.String()
			if result.Status == types.StatusFound && result.Value != nil {
				outputValue = base64.StdEncoding.EncodeToString(result.Value)
			}
			rec.RecordReturn(opID, outputValue, status)
		} else if r < g.config.ReadPct+g.config.WritePct {
			// PUT
			opType = "put"
			valueSize := 100 + rng.Intn(g.config.MaxValueSize-100+1)
			value := make([]byte, valueSize)
			// Fill with deterministic random data.
			for j := 0; j+8 <= len(value); j += 8 {
				binary.LittleEndian.PutUint64(value[j:], rng.Uint64())
			}
			// Fill remaining bytes.
			remaining := len(value) % 8
			if remaining > 0 {
				tail := make([]byte, 8)
				binary.LittleEndian.PutUint64(tail, rng.Uint64())
				copy(value[len(value)-remaining:], tail[:remaining])
			}
			inputValue = base64.StdEncoding.EncodeToString(value)
			opID := rec.RecordCall(clientIDStr, opType, keyHex, inputValue)
			result := c.Put(ctx, key, value)
			status := result.Status.String()
			rec.RecordReturn(opID, "", status)
		} else {
			// DELETE
			opType = "delete"
			opID := rec.RecordCall(clientIDStr, opType, keyHex, "")
			result := c.Delete(ctx, key)
			status := result.Status.String()
			rec.RecordReturn(opID, "", status)
		}

		atomic.AddInt64(opsCompleted, 1)

		// If we got an error, try reconnecting to a different node.
		// This handles the case where the node we're talking to crashed.
		if !orch.IsAlive(nodeID) {
			c.Close()
			nodeID = (nodeID + 1) % 3 // simple rotation
			addr = findAliveNode(orch, nodeID)
			c = client.New(addr, g.config.Timeout)
		}
	}
}

// findAliveNode tries to find an alive node, starting from the given hint.
func findAliveNode(orch *orchestrator.Orchestrator, hint int) string {
	// Try the hint first.
	if orch.IsAlive(hint) {
		return orch.NodeAddr(hint)
	}
	// Try all nodes.
	for i := 0; i < 10; i++ {
		if orch.IsAlive(i) {
			return orch.NodeAddr(i)
		}
	}
	// Fall back to the hint address even if not alive; requests will fail
	// and be recorded as errors.
	log.Printf("WARNING: no alive nodes found, using node %d address", hint)
	return orch.NodeAddr(hint)
}
