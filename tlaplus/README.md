# TLA+ Specification — Distributed Linearizable KV Store

## Overview

This specification models a distributed KV store with:
- **C clients** issuing Get/Put/Delete operations
- **N replicas** with one elected coordinator
- **Synchronous majority replication** for writes
- **Single-node crash and recovery** within bounded steps
- **Linearizability** as the core safety property

### State Representation

Unlike typical Raft/Paxos specs that use a replicated log, this spec uses a
**decision ledger** — a global sequence of committed operation slots. Each
replica tracks how many slots it has *witnessed* (an integer counter). The
coordinator assigns new slots and requires a majority of replicas to be caught
up before committing. This is a deliberately different state representation
per the task requirements.

## Files

| File | Purpose |
|------|---------|
| `KVStore.tla` | Main specification module |
| `KVStore.cfg` | Normal config — should produce **no counterexample** |
| `KVStoreBug.cfg` | Bug mode — should produce a **counterexample** |

## Running TLC

### Prerequisites

Download the TLA+ tools:
```bash
# Option 1: Use the tla2tools.jar from GitHub
wget https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar

# Option 2: Use the TLA+ Toolbox IDE (GUI)
```

### Normal Mode (expect PASS)

```bash
cd tlaplus/
java -jar tla2tools.jar -config KVStore.cfg KVStore.tla
```

Expected output: `Model checking completed. No error has been found.`

### Bug Mode (expect COUNTEREXAMPLE)

```bash
cd tlaplus/
java -jar tla2tools.jar -config KVStoreBug.cfg KVStore.tla
```

Expected output: TLC reports an invariant violation with a counterexample trace.

## Understanding Counterexamples

A TLC counterexample is a sequence of states from Init to the violating state.
Each state shows the values of all variables. Look for:

1. **The violated invariant**: TLC will say which of `Linearizability`,
   `ReadConsistency`, or `NoPhantomReads` failed.

2. **The trace**: Walk through the states to see:
   - Which client issued which operation
   - When the coordinator committed the operation
   - Which replica crashed and when
   - What the ledger and kvState look like

3. **In bug mode**: The counterexample typically shows:
   - A Put committed with only coordinator's ack (no majority)
   - Coordinator crashes
   - New coordinator elected from a replica that missed the Put
   - A subsequent Get returns NOT_FOUND or a stale value
   - This violates ReadConsistency or creates a phantom

## Safety Properties

### Linearizability
For any two committed operations where one's return time precedes the other's
call time, they appear in the same order in the committed sequence. Since the
coordinator serializes all operations, this is maintained by construction —
the invariant guards against implementation bugs.

### ReadConsistency
Every Get returns the value from the most recent preceding Put/Delete in the
committed sequence. This catches stale reads and lost writes.

### NoPhantomReads
A Get that returns a value (not NOT_FOUND) must have a matching Put in the
committed history with the same key and value. This catches data corruption.

## Tuning

To explore more states (deeper bugs), increase constants:
- `MaxOps = 5` or `6` (exponentially more states)
- `MaxCrashes = 2` (allows multiple crash/recovery cycles)
- Add more keys or values for broader coverage

Beware: state space grows exponentially. Start small.
