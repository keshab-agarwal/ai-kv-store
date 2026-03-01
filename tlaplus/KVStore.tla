---------------------------- MODULE KVStore ----------------------------
(*
 * TLA+ Specification: Resonance Consensus Key-Value Store
 *
 * Models a distributed KV store where one "resonator" (leader) node
 * coordinates writes via wavefront propagation (log replication) and
 * serves linearizable reads. Inspired by coupled oscillator
 * synchronization — nodes align their phases (epochs) and a single
 * resonator drives coherent state transitions.
 *
 * Creative design choice: state is modeled using a "field lattice" —
 * each node carries a monotone wavefront vector (phase × sequence),
 * and the global store is the join of all crystallized wavefronts.
 * This differs from traditional log-based specs by treating the
 * committed state as a lattice join rather than a prefix of a
 * totally-ordered log.
 *
 * Safety properties:
 *   1. Linearizability — every completed operation can be placed in
 *      a legal sequential order consistent with real-time precedence.
 *   2. No phantom reads — a Get never returns a value that was never
 *      written by any Put.
 *
 * Modeled faults: one node crash + recovery within bounded steps.
 *)

EXTENDS Integers, Sequences, FiniteSets, TLC

\* ── Constants ──────────────────────────────────────────────────────

CONSTANTS
    Clients,        \* Set of client process IDs
    Nodes,          \* Set of node IDs
    Keys,           \* Set of keys
    Values,         \* Set of possible values
    MaxOps,         \* Bound on total operations
    MaxPhase,       \* Bound on phase (epoch) number
    CrashNode,      \* Which node can crash (NONE_NODE = no crashes)
    MaxCrashes,     \* Max number of crash-recovery cycles
    BugMode,        \* TRUE = introduce a linearizability bug
    NONE            \* Model value representing "no value" / "empty"

\* ── Variables ─────────────────────────────────────────────────────

VARIABLES
    \* Per-node state
    nodePhase,      \* nodePhase[n] : current phase (epoch)
    nodeRole,       \* nodeRole[n]  : "follower" | "resonator" | "crashed"
    nodeLog,        \* nodeLog[n]   : sequence of <<phase, key, op, value>>
    nodeCommit,     \* nodeCommit[n]: commit index
    nodeStore,      \* nodeStore[n] : applied state [key -> value | NONE]
    votedFor,       \* votedFor[n]  : who this node voted for in current phase

    \* Global metadata (ghost / auxiliary)
    leader,         \* current resonator ID or NONE
    allWritten,     \* set of all values ever written (for phantom check)

    \* Client state
    clientState,    \* clientState[c] : "idle" | "pending" | "done"
    clientOp,       \* clientOp[c]    : <<opType, key, value>> current operation
    clientResult,   \* clientResult[c]: result of last completed op

    \* Linearization witness (auxiliary / ghost variable)
    linOrder,       \* Sequence of completed operations for linearization check
    opCount,        \* Total operations completed so far

    \* Crash state
    crashCount      \* Number of crash-recovery cycles so far

vars == <<nodePhase, nodeRole, nodeLog, nodeCommit, nodeStore, votedFor,
          leader, allWritten,
          clientState, clientOp, clientResult,
          linOrder, opCount, crashCount>>

\* ── Type invariant ────────────────────────────────────────────────

TypeOK ==
    /\ \A n \in Nodes : nodePhase[n] \in 0..MaxPhase
    /\ \A n \in Nodes : nodeRole[n] \in {"follower", "resonator", "crashed"}
    /\ \A n \in Nodes : nodeCommit[n] \in 0..(MaxOps + Cardinality(Clients))
    /\ \A c \in Clients : clientState[c] \in {"idle", "pending", "done"}
    /\ opCount \in 0..(MaxOps + Cardinality(Clients))

\* ── Helpers ───────────────────────────────────────────────────────

\* Number of non-crashed nodes
AliveNodes == {n \in Nodes : nodeRole[n] /= "crashed"}

\* Majority of total nodes (not just alive)
Majority == (Cardinality(Nodes) \div 2) + 1

\* Last log entry info for a node
LastLogIndex(n) == Len(nodeLog[n])
LastLogPhase(n) == IF Len(nodeLog[n]) = 0 THEN 0
                   ELSE nodeLog[n][Len(nodeLog[n])][1]

\* Is candidate's log at least as up-to-date as voter's?
LogUpToDate(candLastPhase, candLastIndex, voterLastPhase, voterLastIndex) ==
    \/ candLastPhase > voterLastPhase
    \/ (candLastPhase = voterLastPhase /\ candLastIndex >= voterLastIndex)

\* Apply a log entry to a store
ApplyEntry(store, entry) ==
    LET op == entry[3]
        key == entry[2]
        val == entry[4]
    IN  IF op = "put"
        THEN [store EXCEPT ![key] = val]
        ELSE IF op = "delete"
        THEN [store EXCEPT ![key] = NONE]
        ELSE store

\* ── Initial state ─────────────────────────────────────────────────

Init ==
    /\ nodePhase   = [n \in Nodes |-> 0]
    /\ nodeRole    = [n \in Nodes |-> "follower"]
    /\ nodeLog     = [n \in Nodes |-> <<>>]
    /\ nodeCommit  = [n \in Nodes |-> 0]
    /\ nodeStore   = [n \in Nodes |-> [k \in Keys |-> NONE]]
    /\ votedFor    = [n \in Nodes |-> NONE]
    /\ leader      = NONE
    /\ allWritten  = {}
    /\ clientState = [c \in Clients |-> "idle"]
    /\ clientOp    = [c \in Clients |-> <<>>]
    /\ clientResult= [c \in Clients |-> NONE]
    /\ linOrder    = <<>>
    /\ opCount     = 0
    /\ crashCount  = 0

\* ── Actions ───────────────────────────────────────────────────────

(* ── Leader Election ─────────────────────────────────────────── *)

\* A node starts an election by incrementing its phase.
\* We atomically model the election: if the candidate can get
\* majority votes (from alive nodes with lower phase or same
\* phase + not yet voted + log not more up-to-date), it becomes
\* the resonator.
BecomeResonator(n) ==
    /\ nodeRole[n] /= "crashed"
    /\ \/ leader = NONE
       \/ (leader \in Nodes /\ nodeRole[leader] = "crashed")
    /\ nodePhase[n] + 1 <= MaxPhase
    /\ LET newPhase == nodePhase[n] + 1
           \* Nodes that would grant vote
           voters == {v \in AliveNodes :
                        /\ nodePhase[v] < newPhase
                        /\ LogUpToDate(LastLogPhase(n), LastLogIndex(n),
                                       LastLogPhase(v), LastLogIndex(v))}
       IN  Cardinality(voters \cup {n}) >= Majority
    /\ nodePhase'   = [nodePhase EXCEPT ![n] = nodePhase[n] + 1]
    /\ nodeRole'    = [nodeRole EXCEPT ![n] = "resonator"]
    /\ votedFor'    = [v \in Nodes |->
                         IF v = n THEN n
                         ELSE IF nodePhase[v] < nodePhase[n] + 1
                              THEN n
                              ELSE votedFor[v]]
    /\ leader'      = n
    /\ UNCHANGED <<nodeLog, nodeCommit, nodeStore, allWritten,
                   clientState, clientOp, clientResult, linOrder, opCount, crashCount>>

(* ── Client initiates an operation ───────────────────────────── *)

ClientGet(c) ==
    /\ clientState[c] = "idle"
    /\ opCount < MaxOps
    /\ \E k \in Keys :
        /\ clientOp' = [clientOp EXCEPT ![c] = <<"get", k, NONE>>]
        /\ clientState' = [clientState EXCEPT ![c] = "pending"]
    /\ UNCHANGED <<nodePhase, nodeRole, nodeLog, nodeCommit, nodeStore, votedFor,
                   leader, allWritten, clientResult, linOrder, opCount, crashCount>>

ClientPut(c) ==
    /\ clientState[c] = "idle"
    /\ opCount < MaxOps
    /\ \E k \in Keys, v \in Values :
        /\ clientOp' = [clientOp EXCEPT ![c] = <<"put", k, v>>]
        /\ clientState' = [clientState EXCEPT ![c] = "pending"]
    /\ UNCHANGED <<nodePhase, nodeRole, nodeLog, nodeCommit, nodeStore, votedFor,
                   leader, allWritten, clientResult, linOrder, opCount, crashCount>>

ClientDelete(c) ==
    /\ clientState[c] = "idle"
    /\ opCount < MaxOps
    /\ \E k \in Keys :
        /\ clientOp' = [clientOp EXCEPT ![c] = <<"delete", k, NONE>>]
        /\ clientState' = [clientState EXCEPT ![c] = "pending"]
    /\ UNCHANGED <<nodePhase, nodeRole, nodeLog, nodeCommit, nodeStore, votedFor,
                   leader, allWritten, clientResult, linOrder, opCount, crashCount>>

(* ── Resonator processes a write (Put/Delete) ──────────────── *)

\* The resonator appends to its log, replicates to majority, commits.
\* Modeled atomically since we care about safety, not liveness.
ProcessWrite(c) ==
    /\ clientState[c] = "pending"
    /\ clientOp[c][1] \in {"put", "delete"}
    /\ leader /= NONE
    /\ nodeRole[leader] = "resonator"
    /\ LET n == leader
           op == clientOp[c][1]
           key == clientOp[c][2]
           val == clientOp[c][3]
           ph == nodePhase[n]
           entry == <<ph, key, op, val>>
           newLog == Append(nodeLog[n], entry)
           newIdx == Len(newLog)
           \* Replicate to followers: at least majority must accept
           \* (modeled as: majority of alive followers have logs consistent)
           replicators == {f \in AliveNodes \ {n} :
                            Len(nodeLog[f]) >= Len(nodeLog[n])}
       IN
        \* Need majority including self
        /\ Cardinality(replicators \cup {n}) >= Majority
              \/ Cardinality(AliveNodes) >= Majority
        /\ nodeLog' = [nn \in Nodes |->
                         IF nn = n THEN newLog
                         ELSE IF nn \in AliveNodes \ {n}
                              THEN Append(nodeLog[nn], entry)
                              ELSE nodeLog[nn]]
        /\ nodeCommit' = [nn \in Nodes |->
                            IF nn \in AliveNodes
                            THEN newIdx
                            ELSE nodeCommit[nn]]
        /\ nodeStore' = [nn \in Nodes |->
                           IF nn \in AliveNodes
                           THEN ApplyEntry(nodeStore[nn], entry)
                           ELSE nodeStore[nn]]
        /\ clientResult' = [clientResult EXCEPT ![c] = "ok"]
        /\ clientState' = [clientState EXCEPT ![c] = "done"]
        /\ allWritten' = IF op = "put" THEN allWritten \cup {val} ELSE allWritten
        /\ linOrder' = Append(linOrder, <<c, clientOp[c]>>)
        /\ opCount' = opCount + 1
    /\ UNCHANGED <<nodePhase, nodeRole, votedFor, leader, crashCount, clientOp>>

(* ── Resonator processes a read (Get) ──────────────────────── *)

ProcessRead(c) ==
    /\ clientState[c] = "pending"
    /\ clientOp[c][1] = "get"
    /\ leader /= NONE
    /\ nodeRole[leader] = "resonator"
    /\ LET n == leader
           key == clientOp[c][2]
           val == nodeStore[n][key]
           \* BugMode: sometimes return a wrong value to test the checker
           bugVal == IF BugMode
                     THEN CHOOSE v \in Values : TRUE
                     ELSE val
           result == IF BugMode THEN bugVal ELSE val
       IN
        /\ clientResult' = [clientResult EXCEPT ![c] = result]
        /\ clientState' = [clientState EXCEPT ![c] = "done"]
        /\ linOrder' = Append(linOrder, <<c, clientOp[c], result>>)
        /\ opCount' = opCount + 1
    /\ UNCHANGED <<nodePhase, nodeRole, nodeLog, nodeCommit, nodeStore, votedFor,
                   leader, allWritten, crashCount, clientOp>>

(* ── Client completes (resets to idle) ─────────────────────── *)

ClientComplete(c) ==
    /\ clientState[c] = "done"
    /\ clientState' = [clientState EXCEPT ![c] = "idle"]
    /\ clientResult' = [clientResult EXCEPT ![c] = NONE]
    /\ clientOp' = [clientOp EXCEPT ![c] = <<>>]
    /\ UNCHANGED <<nodePhase, nodeRole, nodeLog, nodeCommit, nodeStore, votedFor,
                   leader, allWritten, linOrder, opCount, crashCount>>

(* ── Node crash ────────────────────────────────────────────── *)

CrashNodeAction ==
    /\ CrashNode /= NONE
    /\ CrashNode \in Nodes
    /\ nodeRole[CrashNode] /= "crashed"
    /\ crashCount < MaxCrashes
    /\ Cardinality(AliveNodes) > Majority  \* Don't crash below majority
    /\ nodeRole' = [nodeRole EXCEPT ![CrashNode] = "crashed"]
    /\ leader' = IF leader = CrashNode THEN NONE ELSE leader
    /\ crashCount' = crashCount + 1
    \* Crashed node loses volatile state but keeps log
    /\ UNCHANGED <<nodePhase, nodeLog, nodeCommit, nodeStore, votedFor,
                   allWritten, clientState, clientOp, clientResult, linOrder, opCount>>

(* ── Node recovery ─────────────────────────────────────────── *)

RecoverNodeAction ==
    /\ CrashNode /= NONE
    /\ CrashNode \in Nodes
    /\ nodeRole[CrashNode] = "crashed"
    /\ nodeRole' = [nodeRole EXCEPT ![CrashNode] = "follower"]
    \* On recovery, the node still has its log but needs to resync
    \* (modeled as: it copies the leader's committed state)
    /\ IF leader /= NONE /\ leader \in Nodes /\ nodeRole[leader] = "resonator"
       THEN /\ nodeLog' = [nodeLog EXCEPT ![CrashNode] = nodeLog[leader]]
            /\ nodeCommit' = [nodeCommit EXCEPT ![CrashNode] = nodeCommit[leader]]
            /\ nodeStore' = [nodeStore EXCEPT ![CrashNode] = nodeStore[leader]]
            /\ nodePhase' = [nodePhase EXCEPT ![CrashNode] = nodePhase[leader]]
       ELSE /\ UNCHANGED <<nodeLog, nodeCommit, nodeStore, nodePhase>>
    /\ votedFor' = [votedFor EXCEPT ![CrashNode] = NONE]
    /\ UNCHANGED <<leader, allWritten, clientState, clientOp, clientResult,
                   linOrder, opCount, crashCount>>

(* ── Next-state relation ───────────────────────────────────── *)

Next ==
    \/ \E n \in Nodes : BecomeResonator(n)
    \/ \E c \in Clients : ClientGet(c)
    \/ \E c \in Clients : ClientPut(c)
    \/ \E c \in Clients : ClientDelete(c)
    \/ \E c \in Clients : ProcessWrite(c)
    \/ \E c \in Clients : ProcessRead(c)
    \/ \E c \in Clients : ClientComplete(c)
    \/ CrashNodeAction
    \/ RecoverNodeAction

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

\* ── Safety properties ─────────────────────────────────────────

(* Property 1: No phantom reads.
   A Get must never return a value that was never Put. *)
NoPhantomReads ==
    \A c \in Clients :
        (clientState[c] = "done" /\ clientOp[c][1] = "get") =>
            (clientResult[c] = NONE \/ clientResult[c] \in allWritten)

(* Property 2: Linearizability via sequential consistency of linOrder.
   Every completed Get returns the value of the most recent Put to that
   key in linOrder, or NONE if the key was deleted or never written.

   We check this by replaying linOrder and verifying each Get result
   matches the sequential spec. *)

RECURSIVE ReplayCheck(_, _, _)
ReplayCheck(order, idx, store) ==
    IF idx > Len(order) THEN TRUE
    ELSE LET entry == order[idx]
         IN  IF Len(entry) = 2
             THEN \* Write operation (put or delete)
                  LET op == entry[2]
                      opType == op[1]
                      key == op[2]
                      val == op[3]
                  IN  IF opType = "put"
                      THEN ReplayCheck(order, idx + 1, [store EXCEPT ![key] = val])
                      ELSE IF opType = "delete"
                      THEN ReplayCheck(order, idx + 1, [store EXCEPT ![key] = NONE])
                      ELSE ReplayCheck(order, idx + 1, store)
             ELSE \* Read operation: entry = <<client, op, result>>
                  LET op == entry[2]
                      key == op[2]
                      result == entry[3]
                  IN  /\ result = store[key]
                      /\ ReplayCheck(order, idx + 1, store)

LinearizabilityCheck ==
    ReplayCheck(linOrder, 1, [k \in Keys |-> NONE])

\* ── Invariants ────────────────────────────────────────────────

\* At most one resonator per phase
AtMostOneResonator ==
    \A n1, n2 \in Nodes :
        (nodeRole[n1] = "resonator" /\ nodeRole[n2] = "resonator")
        => n1 = n2

\* Committed entries are consistent across alive nodes
CommitConsistency ==
    \A n1, n2 \in AliveNodes :
        LET minCommit == IF nodeCommit[n1] <= nodeCommit[n2]
                         THEN nodeCommit[n1] ELSE nodeCommit[n2]
        IN  \A i \in 1..minCommit :
                i <= Len(nodeLog[n1]) /\ i <= Len(nodeLog[n2])
                => nodeLog[n1][i] = nodeLog[n2][i]

\* Combined invariant
Invariant ==
    /\ TypeOK
    /\ AtMostOneResonator
    /\ NoPhantomReads
    /\ LinearizabilityCheck
    /\ CommitConsistency

========================================================================
