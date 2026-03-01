# Redis Benchmark

Production-grade YCSB benchmark harness for Redis, designed as a direct performance
comparison baseline for the custom distributed KV store in this repository.

This benchmark uses the **exact same** YCSB workload engine (`ycsb/` package) — same
Zipfian key distribution, same operation mix (95/4/1), same reporter, same latency
statistics — so results are directly comparable.

## Prerequisites

- **Go 1.20+**
- **Redis 7+** (standalone mode; cluster mode supported via `-cluster` flag)

### Quick Redis setup

```bash
# Option A: Docker (recommended)
docker compose -f redis/docker-compose.yml up -d

# Option B: Local install (macOS)
brew install redis && redis-server --daemonize yes

# Option C: Local install (Linux)
sudo apt install redis-server && sudo systemctl start redis
```

## Building

```bash
go build -o bin/redisbench ./redis/cmd/redisbench/
```

## Running the benchmark

### Quick smoke test

```bash
./bin/redisbench \
    -addrs=localhost:6379 \
    -recordcount=10000 \
    -operationcount=50000 \
    -threads=8 \
    -phase=both \
    -outdir=redis-results
```

### Full benchmark (1M keys, 1M ops)

```bash
./bin/redisbench \
    -addrs=localhost:6379 \
    -recordcount=1000000 \
    -operationcount=1000000 \
    -threads=8 \
    -phase=both \
    -outdir=redis-results-full
```

### Scaling to 100M keys

```bash
./bin/redisbench \
    -addrs=localhost:6379 \
    -recordcount=100000000 \
    -operationcount=100000000 \
    -threads=16 \
    -poolsize=256 \
    -phase=both \
    -outdir=redis-results-100m
```

## Automated evaluation

```bash
chmod +x redis/evaluate.sh
./redis/evaluate.sh [run-name] [redis-addr]

# Examples:
./redis/evaluate.sh redis-v1              # default localhost:6379
./redis/evaluate.sh redis-v1 10.0.0.5:6379  # remote Redis
```

Results are written to `redis/runs/<run-name>/`.

## Comparing with the KV store

After running both benchmarks:

```bash
python3 redis/scripts/compare.py \
    evaluation/runs/<kv-run>/ycsb-results/run_report.json \
    redis/runs/<redis-run>/ycsb-results/run_report.json \
    comparison-output/
```

This produces:
- `comparison_report.json` — structured latency and throughput deltas
- `comparison_chart.png` — side-by-side bar charts (requires matplotlib)
- Tabular stdout output

## CLI flags

| Flag | Default | Description |
|------|---------|-------------|
| `-addrs` | `localhost:6379` | Comma-separated Redis addresses |
| `-password` | `""` | Redis AUTH password |
| `-db` | `0` | Redis database number |
| `-poolsize` | `128` | Connection pool size |
| `-cluster` | `false` | Enable Redis Cluster mode |
| `-read-timeout` | `3s` | Redis read timeout |
| `-write-timeout` | `3s` | Redis write timeout |
| `-op-timeout` | `5s` | Per-operation context timeout |
| `-recordcount` | `1000000` | Number of records to load |
| `-operationcount` | `1000000` | Number of run-phase operations |
| `-threads` | `8` | Concurrent client goroutines |
| `-valuesize` | `1024` | Value size in bytes |
| `-phase` | `both` | Phase: `load`, `run`, or `both` |
| `-outdir` | `redis-results` | Output directory |
| `-readproportion` | `0.95` | Read proportion |
| `-updateproportion` | `0.04` | Update proportion |
| `-deleteproportion` | `0.01` | Delete proportion |
| `-zipfian.theta` | `0.99` | Zipfian skew parameter |
| `-flush` | `true` | FLUSHDB before benchmark |
| `-info` | `false` | Print Redis server info |

## Output files

```
redis-results/
├── benchmark_meta.json       # Run parameters and configuration
├── load_report.json          # Load phase: throughput + latency stats
├── load_latencies.csv        # Per-operation latency samples
├── run_report.json           # Run phase: throughput + latency stats
├── run_latencies.csv         # Per-operation latency samples
├── redis_memory_info.txt     # Redis INFO memory snapshot
├── latency_histogram.png     # Histogram plot (if matplotlib available)
└── latency_cdf.png           # CDF plot (if matplotlib available)
```

## Latency plots

```bash
python3 redis/scripts/plot_latency.py redis-results/run_latencies.csv redis-results/
```

Requires: `pip install matplotlib numpy`

## Directory layout

```
redis/
├── binding.go                # Redis ↔ YCSB DB interface implementation
├── cmd/
│   └── redisbench/
│       └── main.go           # CLI entrypoint
├── docker-compose.yml        # One-command Redis server
├── evaluate.sh               # Automated evaluation harness
├── workloads/
│   ├── workload_load.properties
│   └── workload_run.properties
├── scripts/
│   ├── plot_latency.py       # Latency histogram + CDF
│   └── compare.py            # KV store vs Redis comparison
├── runs/                     # Evaluation output (gitignored)
└── README.md
```
