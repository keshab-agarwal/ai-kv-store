# Task 2: YCSB Zipfian Harness

## Mapping
- `read()` -> KV `Get`
- `insert()` -> KV `Put`
- `update()` -> KV `Put`
- `delete()` -> KV `Delete`
- `scan()` -> `NOT_IMPLEMENTED` (and scans are disabled with `scanproportion=0.0`)

## Scale guidance
- 1M keys:
  - `recordcount=1000000`
  - `operationcount=5000000`
- 10M keys:
  - `recordcount=10000000`
  - `operationcount=20000000` to `50000000`
- 100M keys:
  - `recordcount=100000000`
  - `operationcount=100000000+`
  - increase client threads and pre-split runtime across multiple load drivers

Keep run-phase `insertproportion=0.0`; only load phase uses inserts.

## Latency + throughput output
YCSB status output (`-s`) includes:
- ops/sec overall throughput
- per-op average, min, max latency
- percentile histograms when enabled in YCSB config (`measurementtype=hdrhistogram`)

For p95/p99 graphs:
1. run YCSB with status output redirected to file
2. parse lines containing `95thPercentileLatency(us)` and `99thPercentileLatency(us)`
3. plot over time windows using existing plotting tooling or your own parser
