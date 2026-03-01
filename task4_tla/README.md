# Task 4: TLA+ Linearizability Spec

## Files
- `KVLinearizable.tla`
- `KVLinearizable.cfg`
- `KVLinearizable_bug.cfg` (intentional bug mode)

## Run TLC
```bash
java -cp tlaplus/tla2tools.jar tlc2.TLC task4_tla/KVLinearizable.tla -config task4_tla/KVLinearizable.cfg -deadlock -depth 8
```

Bug mode (should produce counterexample):
```bash
java -cp tlaplus/tla2tools.jar tlc2.TLC task4_tla/KVLinearizable.tla -config task4_tla/KVLinearizable_bug.cfg -deadlock -depth 8
```

## Reading counterexamples
- TLC prints the violated invariant name (`NoPhantomRead` is expected in bug mode).
- Follow the state trace: each step shows action taken (`ClientInvoke`, `CompleteWrite`, `CompleteGet`, `CrashOne`, `RecoverOne`).
- Locate a `get` with `status="FOUND"` and `outVal` not present in `lastWriteSet`; that indicates a read of a value not established by legal writes.
