# Task 3: Correctness Test Harness

A complete correctness test harness for the distributed KV store with fault injection and Porcupine linearizability checking.

## Building

Build all binaries from the repository root:

```bash
# Build the KV node (if not already built)
go build -o bin/kvnode ./task1_kv/cmd/kvnode

# Build the test harness
go build -o bin/harness ./task3_harness/cmd/harness

# Build the standalone checker
go build -o bin/porcupine_check ./task3_harness/cmd/checker
```

## Usage

### Run Self-Test (no cluster needed)

Verifies the Porcupine checker integration with known-good and known-bad histories:

```bash
./bin/harness --self-test
```

### Run Harness (no faults)

Basic linearizability test without fault injection:

```bash
./bin/harness \
  --nodes=3 \
  --clients=10 \
  --ops=1000 \
  --seed=42 \
  --binary=./bin/kvnode \
  --output-dir=./test-run-basic
```

### Run Harness (with crash + recovery)

Test with a node crash and recovery during the workload:

```bash
./bin/harness \
  --nodes=3 \
  --clients=10 \
  --ops=1000 \
  --seed=42 \
  --crash \
  --crash-node=1 \
  --crash-after-ops=300 \
  --recover-after-ms=5000 \
  --binary=./bin/kvnode \
  --output-dir=./test-run-crash
```

### Rerun with Same Seed

To reproduce a test run exactly, use the same `--seed` value:

```bash
./bin/harness --seed=42 --ops=1000 --clients=10 --binary=./bin/kvnode
```

### Run Porcupine Checker on Saved History

Check a previously recorded history file:

```bash
./bin/porcupine_check --history=./test-run-basic/history.jsonl --output-dir=./check-results
```

### Start Cluster Manually

To start a 3-node cluster manually for interactive testing:

```bash
./bin/kvnode --id=0 --port=9100 --peers=127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102 &
./bin/kvnode --id=1 --port=9101 --peers=127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102 &
./bin/kvnode --id=2 --port=9102 --peers=127.0.0.1:9100,127.0.0.1:9101,127.0.0.1:9102 &
```

## History Format

The history is recorded as a JSONL file (one JSON object per line). Each line has the following schema:

```json
{
  "op_id": 1,
  "client_id": "harness-client-0",
  "call_time": 1700000000000000000,
  "return_time": 1700000000001000000,
  "op_type": "get",
  "key": "abcdef0123456789abcdef0123456789",
  "input_value": "",
  "output_value": "SGVsbG8=",
  "status": "found"
}
```

Fields:
- `op_id`: unique operation identifier
- `client_id`: identifies which client issued the operation
- `call_time`: invocation time in nanoseconds since Unix epoch
- `return_time`: completion time in nanoseconds since Unix epoch
- `op_type`: one of "get", "put", "delete"
- `key`: 32-character hex-encoded key
- `input_value`: base64-encoded value (only for put operations)
- `output_value`: base64-encoded value (only for get operations that return "found")
- `status`: one of "ok", "found", "not_found", "error", "timeout"

## TIMEOUT Handling Strategy

Timeout operations are treated as operations that could have linearized at any point after invocation. Specifically:

- **Put/Delete timeouts**: The return time is set to `MaxInt64/2`, and the operation is treated as if it succeeded ("ok"). This is sound because:
  - If the operation actually completed on the server, allowing it at any point after invocation is correct.
  - If the operation did not complete, treating it as potentially completing is conservative -- it only makes the check more permissive, never causing a false failure.

- **Get timeouts**: These are skipped entirely because we have no way to know what value the get would have returned.

This approach ensures that:
1. A passing check is a true positive: if the checker says the history is linearizable, it really is (modulo the timeout approximation).
2. A failing check is meaningful: timeouts are given maximum flexibility, so a failure indicates a real violation in the non-timeout operations.

## Interpreting Failures

When the checker reports FAIL (non-linearizable):

1. **Check the visualization**: Open `visualization.html` in a web browser. This shows the timeline of operations and highlights which operations could not be linearized.

2. **Look at the history**: Examine `history.jsonl` to find operations with conflicting results. Common violations include:
   - **Stale reads**: A get returns an old value after a newer put has completed.
   - **Lost writes**: A put is acknowledged but a subsequent get does not see it.
   - **Non-monotonic reads**: Two gets on the same key return values that are inconsistent with any sequential ordering.

3. **Check node logs**: Look at the node log files in the output directory for any errors, split-brain scenarios, or replication failures.

4. **Reproduce**: Rerun with the same `--seed` to reproduce the issue deterministically.

## Debugging Guide

1. **Start with the self-test** (`--self-test`) to verify the checker works correctly independent of the cluster.

2. **Run without faults first** to establish a baseline. If the check fails without faults, there is a fundamental correctness bug.

3. **Add fault injection** with `--crash` to test crash recovery. Use `--crash-after-ops` to control when the crash happens.

4. **Increase operations** (`--ops=5000`) to increase the chance of finding bugs.

5. **Use a small key pool** (`--key-pool-size=10`) to increase contention and the likelihood of detecting ordering violations.

6. **Check throughput**: If throughput is very low, the cluster may be unhealthy. Check node logs for leader election storms or replication errors.

7. **Seed for reproducibility**: Always note the seed from a failing run so you can reproduce it exactly.
