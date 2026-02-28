# Correctness Harness — Debugging Guide

## Architecture

The harness orchestrates N kvnode processes, generates concurrent client traffic
with a 95/4/1 Get/Put/Delete mix, records a full execution history (JSONL), then
verifies linearizability with Porcupine.

## History Format (JSONL)

Each line in `history.jsonl` is one completed operation:

```json
{
  "op_id": 42,
  "client_id": "w3",
  "call_time_ns": 1700000000000000,
  "return_time_ns": 1700000005000000,
  "op_type": "Get",
  "key": "aabbccdd11223344aabbccdd11223344",
  "input_value": "",
  "output": "FOUND",
  "output_value": "deadbeef"
}
```

Fields:
- `op_id`: monotonically increasing per-run operation ID
- `client_id`: which worker issued the operation
- `call_time_ns`: nanosecond timestamp when the operation was invoked
- `return_time_ns`: nanosecond timestamp when the response was received
- `op_type`: "Get", "Put", or "Delete"
- `key`: hex-encoded 128-bit key
- `input_value`: hex-encoded input value (Put only)
- `output`: status string — "OK", "FOUND", "NOT_FOUND", "ERROR", "TIMEOUT"
- `output_value`: hex-encoded output value (Get/FOUND only)

## TIMEOUT Strategy

Operations that return TIMEOUT are recorded with their actual call/return times.
The Porcupine checker sets their return time to `MaxInt64`, modeling them as
"possibly still pending." This is sound: the checker tries both including and
excluding the timed-out operation from the linearization. If a TIMEOUT op must
have taken effect for the history to be linearizable, Porcupine will find that
ordering. If it cannot have taken effect, Porcupine will find that too.

Implication: a TIMEOUT op will never cause a false FAIL. It may cause a false
PASS only if the write truly did not happen but the history is also consistent
without it — which is correct behavior (we don't know the outcome).

## Interpreting Failures

### PASS
All non-error operations form a linearizable history. The additional safety
property (reads never return values never written) also holds.

### FAIL — "history is NOT linearizable"
The recorded concurrent execution cannot be explained by any sequential ordering
that respects real-time precedence. This means the KV store violated
linearizability.

**Steps to diagnose:**
1. Open `checker_result.txt` for summary statistics.
2. Open `linearizability_violation.html` (generated on FAIL) in a browser.
   This interactive visualization shows the timeline of operations and
   highlights the conflicting ones.
3. Identify the violating operations (usually a stale read or lost write).
4. Check `logs/node*.log` for the relevant time window to see if an election,
   crash, or replication failure occurred.
5. Reproduce with the same seed: `--seed=<value>` printed in harness output.

### FAIL — "reads never return values never written"
A Get returned a value that no Put ever wrote. This is a data corruption bug.

### UNKNOWN — "checker timed out"
The history was too large for Porcupine to verify in the allotted time.
Reduce `--duration` or `--workers` to generate a smaller history, or increase
`--checktimeout`.

## Commands

```bash
# Build all binaries
go build -o bin/kvnode ./kvstore/cmd/kvnode/
go build -o bin/harness ./correctness/cmd/harness/
go build -o bin/checker ./correctness/cmd/checker/

# Self-test (validates checker pipeline, no cluster needed)
./bin/harness -selftest
./bin/checker -selftest

# Run without faults (30 seconds, 3 nodes, 10 workers)
./bin/harness -bin=./bin/kvnode -nodes=3 -duration=30s -workers=10

# Run with fault injection (crash + recover 1 node)
./bin/harness -bin=./bin/kvnode -nodes=3 -duration=30s -workers=10 -faults -seed=12345

# Reproduce a specific run
./bin/harness -bin=./bin/kvnode -nodes=3 -duration=30s -workers=10 -faults -seed=12345

# Run checker on saved history
./bin/checker -history=test-run-20240101-120000/history.jsonl -timeout=120s

# Output goes to test-run-YYYYMMDD-HHMMSS/ containing:
#   history.jsonl           — full operation history
#   summary.json            — pass/fail + counters
#   checker_result.txt      — detailed checker output
#   logs/node*.log          — per-node logs
#   linearizability_violation.html  — (on FAIL) interactive visualization
```
