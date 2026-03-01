# YCSB-Compatible KV Store Benchmark

A Go-based benchmark tool that generates YCSB-compatible workloads for the
distributed KV store. It implements the standard YCSB load/run phases with
configurable operation mix, key distribution, and concurrency.

## Quick Start

### Prerequisites

- Go 1.21+
- A running KV store instance (default: `localhost:9000`)

### Build and Run

```bash
# From the repository root:
cd task2_ycsb
./run_bench.sh
```

Or manually:

```bash
# Build
go build -o bench ./task2_ycsb/cmd/bench

# Run with defaults (1M keys, 10M ops, zipfian)
./bench --target localhost:9000

# Quick smoke test (100 keys, 1000 ops)
./bench --recordcount 100 --operationcount 1000

# Load phase only
./bench --phase load --recordcount 1000000

# Run phase only (keys must already be loaded)
./bench --phase run --recordcount 1000000 --operationcount 10000000
```

## Command-Line Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--target` | `localhost:9000` | KV store address (host:port) |
| `--threads` | `16` | Number of concurrent worker goroutines |
| `--recordcount` | `1000000` | Number of keys to load |
| `--operationcount` | `10000000` | Number of operations to run |
| `--distribution` | `zipfian` | Key distribution: `zipfian` or `uniform` |
| `--read-proportion` | `0.95` | Fraction of read operations |
| `--update-proportion` | `0.04` | Fraction of update operations |
| `--delete-proportion` | `0.01` | Fraction of delete operations |
| `--fieldlength` | `1024` | Value size in bytes |
| `--phase` | `both` | Phase to run: `load`, `run`, or `both` |
| `--output-dir` | `./ycsb-results` | Directory for result files |
| `--timeout` | `5s` | Per-request timeout |

## Scaling

### From 1M to 100M Keys

Simply change `--recordcount` and `--operationcount`:

```bash
# 100M keys, 1B ops, 64 threads
./run_bench.sh --recordcount 100000000 --operationcount 1000000000 --threads 64
```

For very large datasets, run phases separately to monitor each:

```bash
# Phase 1: Load 100M keys
./run_bench.sh --phase load --recordcount 100000000

# Phase 2: Run 1B operations
./run_bench.sh --phase run --recordcount 100000000 --operationcount 1000000000
```

Memory considerations for 100M keys:
- Each record is ~1 KB (16-byte key + 1024-byte value)
- 100M records = ~100 GB of data in the store
- Ensure the KV store and host have sufficient resources

## Output Format

### summary.json

```json
{
  "phase": "run",
  "start_time": "2026-03-01T12:00:00Z",
  "end_time": "2026-03-01T12:05:00Z",
  "duration_sec": 300.0,
  "total_ops": 10000000,
  "total_errors": 0,
  "throughput_ops_per_sec": 33333.3,
  "distribution": "zipfian",
  "threads": 16,
  "record_count": 1000000,
  "operation_count": 10000000,
  "field_length_bytes": 1024,
  "operations": [
    {
      "op_type": "READ",
      "count": 9500000,
      "errors": 0,
      "avg_latency_us": 150.0,
      "p50_latency_us": 120,
      "p95_latency_us": 280,
      "p99_latency_us": 450,
      "p999_latency_us": 800,
      "p9999_latency_us": 1500,
      "max_latency_us": 5000
    },
    ...
  ]
}
```

### timeseries.csv

```csv
timestamp,elapsed_sec,total_ops,throughput_ops_per_sec,avg_latency_us,p50_latency_us,p95_latency_us,p99_latency_us,p999_latency_us
2026-03-01T12:00:10Z,10.0,333000,33300.0,150.0,120,280,450,800
2026-03-01T12:00:20Z,20.0,666000,33300.0,148.0,118,275,448,790
...
```

### Terminal Output

Live stats are printed every 10 seconds during the run phase:

```
  [RUN] 333000 / 10000000 ops | interval: 33300 ops/sec | p50=120us p95=280us p99=450us p999=800us
```

At completion, a latency distribution chart is displayed:

```
--- LATENCY DISTRIBUTION (all ops) ---
     1 us [ 0.01%] #                                                  (100)
     2 us [ 0.05%] ##                                                 (500)
     4 us [ 0.10%] ###                                                (1000)
    64 us [15.00%] ########################                           (1500000)
   128 us [45.00%] ################################################## (4500000)
   256 us [30.00%] #################################                  (3000000)
   512 us [ 8.00%] #########                                          (800000)
   1.0 ms [ 1.50%] ##                                                 (150000)
   2.0 ms [ 0.30%] #                                                  (30000)
   4.0 ms [ 0.04%] #                                                  (4000)
```

## Interpreting Results

### Throughput

- Measured in operations per second (ops/sec)
- Higher is better
- Scales with `--threads` up to the server's capacity

### Latency Targets

| Percentile | Target | Description |
|------------|--------|-------------|
| P50 | < 200 us | Median latency |
| P95 | < 300 us | 95th percentile |
| P99 | < 500 us | 99th percentile - primary target |
| P999 | < 1000 us | 99.9th percentile - tail latency target |
| P9999 | < 5000 us | 99.99th percentile - extreme tail |

The benchmark prints a PASS/FAIL assessment against P99 < 500us and P999 < 1ms.

### Zipfian vs Uniform

- **Zipfian**: Realistic workload with hot keys. Benefits from caching. Lower
  average latency but potentially higher tail latency on popular keys.
- **Uniform**: All keys equally likely. No caching benefit. Stresses all
  storage partitions equally. Good for finding worst-case performance.

## YCSB Compatibility

### Workload Files

The `workloads/` directory contains standard YCSB `.properties` files that
are compatible with the Java YCSB framework:

- `workload_zipfian.properties` - Zipfian distribution (skewed access)
- `workload_uniform.properties` - Uniform distribution (even access)

These files use standard YCSB property names and can be loaded by the
Java YCSB tool with a custom Java binding. The Go benchmark tool implements
its own workload generation matching these parameters.

### Operation Mix Encoding

Standard YCSB does not have a native `deleteproportion`. The workload files
encode the mix as 95% read / 5% update. The Go benchmark splits the non-read
portion into 4% updates and 1% deletes using the `--update-proportion` and
`--delete-proportion` flags.

### Binding

The Go binding (`binding/binding.go`) implements the conceptual YCSB DB
interface:

| YCSB Method | KV Store Operation |
|-------------|-------------------|
| Read | GET |
| Insert | PUT |
| Update | PUT |
| Delete | DELETE |
| Scan | Not supported |
