# TensorKV Harness

Evaluation harness for AI-native distributed KV stores. Used in CS 294 (UC Berkeley, Prof. Ion Stoica) to verify LLM-generated distributed system implementations against correctness invariants and performance benchmarks.

## What this is

The harness defines typed interfaces, records operation histories, checks distributed systems invariants, injects faults, and benchmarks performance. A separate implementation must satisfy `interfaces.Cluster` and `interfaces.Store` — the harness verifies whatever it produces.

## Project layout

```
tensorkv-harness/
├── interfaces/        Typed contracts (Store, Cluster)
├── recorder/          Thread-safe history recording proxy
├── checkers/          Four independent invariant checkers
├── workload/          YCSB-style concurrent workload generator
├── faultinjector/     Time-based fault injection scheduler
├── benchmark/         Performance measurement and reporting
├── evaluator/         Top-level Evaluate() function
├── cmd/harness/       CLI entry point
└── internal/mock/     In-memory mock cluster for harness testing
```

## Invariant checkers

| Checker | What it verifies |
|---------|-----------------|
| `phantom` | Every successful Get returned a value that was actually Put for that key |
| `monotonic` | Per-client, per-key reads never go backwards in write order |
| `causal` | Per-key operation history is linearizable (Porcupine) |
| `durability` | Keys written before a node crash remain readable from surviving nodes |

## Scoring

`Evaluate()` returns a `(float64, string)` pair:

- **0.0** if any invariant fails (hard gate — no partial credit for correctness).
- **throughput / 1000.0** if all invariants pass across all three fault schedules.

## Quick start

```bash
# Install dependencies
go mod tidy

# Run all tests
go test ./...

# Run the harness against the in-memory mock
go run ./cmd/harness --mock

# Performance-only run (no invariant checking)
go run ./cmd/harness --mock --workload-only

# Vet
go vet ./...
```

## Implementing against the harness

Your implementation must satisfy both interfaces in `interfaces/store.go`:

```go
type Store interface {
    Put(ctx context.Context, key Key, value Value) error
    Get(ctx context.Context, key Key) (Value, error)
}

type Cluster interface {
    Start(nodes int) error
    Connect(node NodeID) (Store, error)
    NodeIDs() []NodeID
    KillNode(node NodeID) error
    RestartNode(node NodeID) error
    PartitionNodes(a, b NodeID) error
    HealPartition(a, b NodeID) error
    Shutdown() error
}
```

Values must be between 1 MB and 4 MB inclusive. The interfaces deliberately contain no algorithmic hints — choose any replication strategy you like.

## Dependencies

- [`github.com/anishathalye/porcupine`](https://github.com/anishathalye/porcupine) — linearizability checker used by the causal consistency checker.
- Go standard library only for everything else.
