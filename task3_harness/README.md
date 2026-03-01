# Task 3: Correctness Harness + Fault Injection + Porcupine

## TIMEOUT handling strategy
This harness keeps TIMEOUT operations as **pending** by recording only invocation (`call_time`) and omitting `return_time`.
The checker uses Porcupine event mode; pending operations are handled via completion semantics (`complete(H')`), preserving soundness.

## Stable history format (`history.jsonl`)
Each line has:
- `op_id`
- `client_id`
- `call_time` (RFC3339Nano)
- `return_time` (optional; missing for TIMEOUT)
- `op_type` (`get|put|delete`)
- `key` (32-hex, 128-bit)
- `input_value` (base64 bytes in JSON)
- `output_value` (base64 bytes in JSON)
- `status` (`OK|FOUND|NOT_FOUND|ERROR|TIMEOUT`)
- `error` (optional)

## Artifacts per run
- `history.jsonl`
- `harness.log`
- `node_logs/node-*.log`
- `checker.out`
- `summary.json`
- `linearizability.html` (from Porcupine visualize)

## Commands
Build binaries:
```bash
go build -o bin/kvnode ./task1_kv/cmd/node
go build -o bin/clusterctl ./task1_kv/cmd/clusterctl
go build -o bin/harness ./task3_harness/cmd/harness
go build -o bin/porcupine_check ./task3_harness/cmd/porcupine_check
```

No-fault run:
```bash
./bin/harness -run-dir artifacts/run_no_fault -seed 42 -duration 20s -workers 64 -inject-fault=false
```

Crash+recover run:
```bash
./bin/harness -run-dir artifacts/run_fault -seed 42 -duration 25s -workers 64 -inject-fault=true -crash-node 1 -crash-after 6s -recover-after 10s
```

Deterministic repro:
```bash
./bin/harness -run-dir artifacts/repro_seed_42 -seed 42 -duration 25s -workers 64 -inject-fault=true -crash-node 1 -crash-after 6s -recover-after 10s
```

Checker-only:
```bash
./bin/porcupine_check -history artifacts/run_fault/history.jsonl -out-dir artifacts/run_fault
```

Self-test:
```bash
./bin/porcupine_check -selftest -out-dir artifacts/selftest
```

## Debugging failures
1. Open `summary.json` and `checker.out` first.
2. If FAIL, inspect `linearizability.html` for conflicting operations and key timeline.
3. Locate exact records in `history.jsonl` via `op_id`.
4. Correlate with `node_logs/node-*.log` and fault timestamps in `harness.log`.
