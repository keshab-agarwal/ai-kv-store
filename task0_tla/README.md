# LinearKV: A Linearizable Distributed Key-Value Store in TLA+

## Overview

This is a TLA+ specification of a linearizable distributed key-value store using an **epoch quorum** approach:

- **Multiple replicas** with one designated **coordinator** per epoch.
- The **coordinator** handles all writes by replicating to a majority quorum before acknowledging.
- **Reads** are served exclusively from the coordinator (which holds the latest committed state).
- On coordinator **crash**, a new epoch begins with a new coordinator elected from alive replicas.
- **Recovery** rebuilds replica state from the committed log.
- A **bug mode** (`BugMode=TRUE`) enables a `StaleRead` action where a follower serves reads directly from its (possibly stale) local state, which violates linearizability.

## Files

| File | Description |
|------|-------------|
| `LinearKV.tla` | Main TLA+ specification with all state variables, actions, and invariants |
| `LinearKV.cfg` | TLC config for normal mode (BugMode=FALSE) -- should pass all invariants |
| `LinearKV_Bug.cfg` | TLC config for bug mode (BugMode=TRUE) -- should find a linearizability violation |
| `run_tlc.sh` | Shell script to run TLC with either config |

## How to Run

### Prerequisites

- Java (JDK 11+)
- TLC jar at `../tlaplus/tla2tools.jar`

### Normal Mode (should pass)

```bash
cd task0_tla
./run_tlc.sh
```

Or manually:

```bash
java -XX:+UseParallelGC -jar ../tlaplus/tla2tools.jar \
    -config LinearKV.cfg -workers auto -metadir states LinearKV.tla
```

With `MaxOps=3`, 2 clients, 3 replicas, 2 keys, 2 values, TLC will explore millions of states. This may take 5-15 minutes depending on your machine. All states should satisfy both `TypeInvariant` and `Linearizability`.

### Bug Mode (should find counterexample)

```bash
./run_tlc.sh bug
```

Or manually:

```bash
java -XX:+UseParallelGC -jar ../tlaplus/tla2tools.jar \
    -config LinearKV_Bug.cfg -workers auto -metadir states LinearKV.tla
```

This finds a counterexample almost immediately (within seconds, at depth 3).

## Understanding the Counterexample

When you run bug mode, TLC produces a trace like this:

```
State 1: <Initial predicate>
  All replicas alive, coordinator is r1, stores are empty.

State 2: <ClientPut(c1, k1, v1)>
  Client c1 writes v1 to k1. The coordinator (r1) replicates to a quorum
  (e.g., r1 and r2), but r3 does NOT receive the write.

State 3: <StaleRead(c1, k1)>
  Client c1 reads k1 from follower r3 (which still has NoValue for k1).
  c1 gets back NoValue, but c1 previously wrote v1 to k1.
  This violates linearizability: in any legal sequential ordering,
  c1's read must come after c1's write, so it must return v1 (or a later value).
```

The key insight: **reading from a follower that missed a quorum write breaks linearizability** because the follower's state can be behind the coordinator's committed state.

## How Linearizability Is Checked

The invariant works by:

1. Collecting all completed operations across all clients into a set.
2. Checking that there exists some permutation (sequential ordering) of those operations such that:
   - **Legal execution**: Walking through the sequence with a simulated KV store, every Get returns the correct value given prior Puts/Deletes.
   - **Per-client order**: Operations by the same client appear in the same order as in that client's history.
   - **Real-time precedence**: If operation A completed before operation B started (A.end < B.start), then A appears before B in the sequence.

This is an exact check of linearizability for the bounded operation count.

## Design Notes

- `NoValue` is the string `"NoValue"`, distinct from all values in the `Values` constant set.
- Operations are atomic (modeled as single transitions) with monotonic IDs serving as timestamps.
- The `StoreFromLog` helper reconstructs the latest store state from the committed log (used during recovery and epoch changes).
- The quorum selection in writes uses `\E Q \in Quorum` to nondeterministically choose which quorum receives the write, modeling the fact that not all replicas may receive every write.
- `MaxOps` bounds the total number of client operations to keep the state space finite.

## Constants

| Constant | Normal | Bug | Description |
|----------|--------|-----|-------------|
| Clients | {c1, c2} | {c1, c2} | Client identifiers |
| Replicas | {r1, r2, r3} | {r1, r2, r3} | Replica identifiers |
| Keys | {k1, k2} | {k1, k2} | Key space |
| Values | {v1, v2} | {v1, v2} | Value space |
| MaxOps | 3 | 3 | Maximum total operations |
| BugMode | FALSE | TRUE | Enable stale read bug |
