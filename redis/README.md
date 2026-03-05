# Redis Benchmark

Benchmarks Redis using a YCSB workload (95% read, 4% update, 1% delete).

## Quick Start

**1. Start Redis**

```bash
docker compose -f redis/docker-compose.yml up -d
```

**2. Build**

```bash
go build -o bin/redisbench ./redis/cmd/redisbench/
```

**3. Run**

```bash
./bin/redisbench -phase=both -outdir=redis-results
```

Results go to `redis-results/`. That's it.

## Automated Evaluation

Handles everything (start Redis, build, smoke test, full benchmark, plots):

```bash
./redis/evaluate.sh
```

## Tuning

Pass flags to `redisbench` to adjust the workload:

```bash
./bin/redisbench \
    -recordcount=100000 \
    -operationcount=500000 \
    -threads=16 \
    -phase=both \
    -outdir=redis-results
```

Run `./bin/redisbench -help` for all flags.
