# KVStore TLA+ Specification

## Overview

This directory contains a TLA+ specification for the Photon Quorum Protocol — the
distributed KV store's consensus and replication algorithm. The spec uses a physics
metaphor: nodes have "energy levels" (epochs), writes are "photon pulses" absorbed by
a quorum, and the primary holds a "coherence window" (lease) for serving reads without
quorum round-trips.

## Files

| File | Purpose |
|---|---|
| `KVStore.tla` | Complete TLA+ specification |
| `KVStore.cfg` | TLC configuration — normal mode, no bugs, expects no violations |
| `KVStoreBug.cfg` | TLC configuration — bug injection mode, expects `LinearizabilityInvariant` violation |

## What Is Modelled

- **N nodes** (default N=3) with roles: `Primary`, `Secondary`, `Candidate`
- **Election** via quorum vote: epoch bump, vote grant, win election
- **Replication**: primary appends entries, secondaries replicate one-by-one
- **Commit**: primary advances `commitIndex` when a quorum holds the entry
- **Lease reads**: primary serves Gets from local KV state only when `leaseValid=TRUE`
- **Crash & recovery**: `NodeCrash` resets volatile state (role, lease) but keeps durable log and epoch
- **Single key** `"k"`, values `{"v1","v2"}`, for TLC tractability

## Safety Properties Checked

| Invariant | Description |
|---|---|
| `TypeInvariant` | All state variables have the correct types |
| `AtMostOneLeaderPerEpoch` | At most one node is `Primary` in any given epoch |
| `CommittedEntriesNotLost` | Every primary's log contains all entries up to its `commitIndex` |
| `LinearizabilityInvariant` | Any primary serving reads with a valid lease has an up-to-date view of all committed writes |

## Bug Mode

When `INJECT_BUG = TRUE`, the `ClientGet` action allows a primary to serve reads even
when `leaseValid = FALSE`. This means the primary's KV state may not reflect the most
recently committed writes (which could have been committed by a different primary with
a higher epoch). TLC will find a counterexample where `LinearizabilityInvariant` is
violated: a primary returns a stale value for a key that was overwritten after the
primary's lease expired.

## Running with TLC

### Normal mode (should find no invariant violations):

```
java -jar tla2tools.jar -config KVStore.cfg KVStore.tla
```

### Bug mode (should find a `LinearizabilityInvariant` counterexample):

```
java -jar tla2tools.jar -config KVStoreBug.cfg KVStore.tla
```

### Recommended TLC options for faster model checking:

```
java -Xmx4g -jar tla2tools.jar \
    -workers auto \
    -config KVStore.cfg \
    KVStore.tla
```

## Constants and Bounds

The constants are deliberately small to keep TLC's state space tractable:

| Constant | Normal | Bug mode | Meaning |
|---|---|---|---|
| `Nodes` | `{1,2,3}` | `{1,2,3}` | Three-node cluster |
| `MaxEpoch` | `3` | `2` | Maximum epoch before stopping |
| `Keys` | `{"k"}` | `{"k"}` | Single key |
| `Values` | `{"v1","v2"}` | `{"v1","v2"}` | Two possible values |
| `MaxLogLen` | `5` | `3` | Maximum log entries per node |
| `INJECT_BUG` | `FALSE` | `TRUE` | Enable stale-read bug |

The `SYMMETRY Symmetry` directive tells TLC that node IDs are interchangeable, reducing
the state space by up to N! (6x for three nodes).

## Creative Framing

The spec uses an energy/physics metaphor throughout:

- **Energy levels** = epochs (higher epoch = higher energy, older leaders step down)
- **Photon pulses** = write operations propagated to replicas
- **Quorum absorption** = when enough replicas "absorb" the pulse, it collapses into committed state
- **Coherence window** = the lease: a time window during which the primary's local state is guaranteed to be consistent with the cluster
- **Quantum register** = the cluster's committed KV state — only "collapsed" (observable) when quorum agrees
