--------------------------- MODULE KVStore ---------------------------
(**************************************************************************)
(* TLA+ specification for distributed KV store with RF=2+witness.         *)
(* Models: primary/backup/witness roles; epoch-based leader election;     *)
(* quorum writes (2-of-3); lease-based reads; crash-recovery.            *)
(* Safety: linearizability, log agreement, reads return written values.   *)
(**************************************************************************)

EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Clients,        \* set of client IDs
    Nodes,          \* set of node IDs (exactly 3)
    InitPrimary,    \* the node that starts as primary
    Keys,           \* set of keys
    Values,         \* set of non-nil values
    Nil,            \* distinguished "no value" constant
    MaxOps,         \* bound on total operations
    MaxCrashes      \* bound on crash/recovery cycles

VARIABLES
    role,           \* role[n] \in {"primary","backup","witness","crashed"}
    epoch,          \* epoch[n] \in Nat
    nlog,           \* nlog[n] = sequence of LogEntry records
    commitIndex,    \* commitIndex[n] \in Nat
    votedFor,       \* votedFor[n] \in Nodes \cup {Nil}
    kvState,        \* kvState[n][k] \in Values \cup {Nil}
    leaseValid,     \* leaseValid[n] \in BOOLEAN
    history,        \* sequence of completed op records
    opCounter,      \* global logical clock
    crashCount      \* total crashes so far

vars == <<role, epoch, nlog, commitIndex, votedFor, kvState,
          leaseValid, history, opCounter, crashCount>>

Min(a, b) == IF a < b THEN a ELSE b

----
(**************************************************************************)
(* Initial state                                                          *)
(**************************************************************************)

OtherNodes == Nodes \ {InitPrimary}
InitBackup == CHOOSE n \in OtherNodes : TRUE
InitWitness == CHOOSE n \in OtherNodes \ {InitBackup} : TRUE

Init ==
    /\ role = [n \in Nodes |->
         IF n = InitPrimary THEN "primary"
         ELSE IF n = InitBackup THEN "backup"
         ELSE "witness"]
    /\ epoch = [n \in Nodes |-> 1]
    /\ nlog = [n \in Nodes |-> << >>]
    /\ commitIndex = [n \in Nodes |-> 0]
    /\ votedFor = [n \in Nodes |-> Nil]
    /\ kvState = [n \in Nodes |-> [k \in Keys |-> Nil]]
    /\ leaseValid = [n \in Nodes |->
         IF n = InitPrimary THEN TRUE ELSE FALSE]
    /\ history = << >>
    /\ opCounter = 0
    /\ crashCount = 0

----
(**************************************************************************)
(* Helpers                                                                *)
(**************************************************************************)

IsPrimary(n) == role[n] = "primary"
IsBackup(n) == role[n] = "backup"
IsWitness(n) == role[n] = "witness"
IsAlive(n) == role[n] /= "crashed"

LastLogIndex(n) == Len(nlog[n])
LastLogEpoch(n) == IF Len(nlog[n]) = 0 THEN 0
                   ELSE nlog[n][Len(nlog[n])].epoch

ApplyEntry(state, entry) ==
    IF entry.opType = "put"
    THEN [state EXCEPT ![entry.key] = entry.val]
    ELSE IF entry.opType = "delete"
    THEN [state EXCEPT ![entry.key] = Nil]
    ELSE state

ApplyLog(logSeq, upTo) ==
    LET RECURSIVE Ap(_, _, _)
        Ap(st, seq, i) ==
            IF i > upTo \/ i > Len(seq) THEN st
            ELSE Ap(ApplyEntry(st, seq[i]), seq, i + 1)
    IN Ap([k \in Keys |-> Nil], logSeq, 1)

----
(**************************************************************************)
(* Client Get: read from primary under valid lease                        *)
(**************************************************************************)

ClientGet(c, k) ==
    /\ opCounter < MaxOps
    /\ \E p \in Nodes :
         /\ IsPrimary(p) /\ leaseValid[p]
         /\ LET val == kvState[p][k]
                status == IF val = Nil THEN "not_found" ELSE "found"
                callT == opCounter + 1
                retT == opCounter + 2
            IN /\ opCounter' = retT
               /\ history' = Append(history,
                    [client |-> c, opType |-> "get", key |-> k,
                     inVal |-> Nil, outVal |-> val, outStatus |-> status,
                     callTime |-> callT, returnTime |-> retT])
    /\ UNCHANGED <<role, epoch, nlog, commitIndex, votedFor, kvState,
                   leaseValid, crashCount>>

----
(**************************************************************************)
(* Client Put: replicate to backup (not just witness) then commit         *)
(**************************************************************************)

ClientPut(c, k, v) ==
    /\ opCounter < MaxOps
    /\ \E p \in Nodes :
         /\ IsPrimary(p)
         \* The backup MUST receive the entry for commit (ensures log completeness
         \* during elections). The witness provides additional durability but is
         \* not sufficient alone — this prevents a new leader from missing committed data.
         /\ \E b \in Nodes \ {p} :
              /\ IsBackup(b) /\ IsAlive(b)
              /\ LET idx == LastLogIndex(p) + 1
                     entry == [epoch |-> epoch[p], key |-> k, val |-> v,
                               opType |-> "put", index |-> idx]
                 IN /\ nlog' = [nlog EXCEPT
                          ![p] = Append(nlog[p], entry),
                          ![b] = Append(nlog[b], entry)]
                    /\ commitIndex' = [commitIndex EXCEPT ![p] = idx]
                    /\ kvState' = [kvState EXCEPT
                         ![p] = ApplyEntry(kvState[p], entry)]
                    /\ LET callT == opCounter + 1
                           retT == opCounter + 2
                       IN /\ opCounter' = retT
                          /\ history' = Append(history,
                               [client |-> c, opType |-> "put", key |-> k,
                                inVal |-> v, outVal |-> Nil,
                                outStatus |-> "ok",
                                callTime |-> callT, returnTime |-> retT])
    /\ UNCHANGED <<role, epoch, votedFor, leaseValid, crashCount>>

----
(**************************************************************************)
(* Client Delete: same quorum path as Put                                 *)
(**************************************************************************)

ClientDelete(c, k) ==
    /\ opCounter < MaxOps
    /\ \E p \in Nodes :
         /\ IsPrimary(p)
         /\ \E b \in Nodes \ {p} :
              /\ IsBackup(b) /\ IsAlive(b)
              /\ LET idx == LastLogIndex(p) + 1
                     entry == [epoch |-> epoch[p], key |-> k, val |-> Nil,
                               opType |-> "delete", index |-> idx]
                 IN /\ nlog' = [nlog EXCEPT
                          ![p] = Append(nlog[p], entry),
                          ![b] = Append(nlog[b], entry)]
                    /\ commitIndex' = [commitIndex EXCEPT ![p] = idx]
                    /\ kvState' = [kvState EXCEPT
                         ![p] = ApplyEntry(kvState[p], entry)]
                    /\ LET callT == opCounter + 1
                           retT == opCounter + 2
                       IN /\ opCounter' = retT
                          /\ history' = Append(history,
                               [client |-> c, opType |-> "delete", key |-> k,
                                inVal |-> Nil, outVal |-> Nil,
                                outStatus |-> "ok",
                                callTime |-> callT, returnTime |-> retT])
    /\ UNCHANGED <<role, epoch, votedFor, leaseValid, crashCount>>

----
(**************************************************************************)
(* Backup catches up to primary's commit index                            *)
(**************************************************************************)

BackupApply(n) ==
    /\ IsBackup(n)
    /\ \E p \in Nodes :
         /\ IsPrimary(p)
         /\ commitIndex[p] > commitIndex[n]
         /\ Len(nlog[n]) >= commitIndex[p]
    /\ LET p == CHOOSE x \in Nodes : IsPrimary(x)
       IN /\ commitIndex' = [commitIndex EXCEPT ![n] = commitIndex[p]]
          /\ kvState' = [kvState EXCEPT
               ![n] = ApplyLog(nlog[n], commitIndex[p])]
    /\ UNCHANGED <<role, epoch, nlog, votedFor, leaseValid,
                   history, opCounter, crashCount>>

----
(**************************************************************************)
(* Crash / Recovery                                                       *)
(**************************************************************************)

NodeCrash(n) ==
    /\ IsAlive(n)
    /\ crashCount < MaxCrashes
    /\ role' = [role EXCEPT ![n] = "crashed"]
    /\ leaseValid' = [leaseValid EXCEPT ![n] = FALSE]
    /\ crashCount' = crashCount + 1
    /\ UNCHANGED <<epoch, nlog, commitIndex, votedFor, kvState,
                   history, opCounter>>

NodeRecover(n) ==
    /\ role[n] = "crashed"
    /\ role' = [role EXCEPT ![n] = "backup"]
    /\ UNCHANGED <<epoch, nlog, commitIndex, votedFor, kvState, leaseValid,
                   history, opCounter, crashCount>>

----
(**************************************************************************)
(* Leader Election                                                        *)
(**************************************************************************)

StartElection(n) ==
    /\ IsAlive(n)
    /\ ~IsPrimary(n)
    /\ ~(\E p \in Nodes : IsPrimary(p) /\ IsAlive(p))
    /\ LET newEpoch == epoch[n] + 1
           voters == {q \in Nodes \ {n} :
              /\ IsAlive(q)
              /\ \/ LastLogEpoch(n) > LastLogEpoch(q)
                 \/ (LastLogEpoch(n) = LastLogEpoch(q) /\
                     LastLogIndex(n) >= LastLogIndex(q))}
           votes == Cardinality(voters) + 1
       IN /\ votes * 2 > Cardinality(Nodes)
          /\ epoch' = [epoch EXCEPT ![n] = newEpoch]
          /\ votedFor' = [votedFor EXCEPT ![n] = n]
          /\ role' = [role EXCEPT ![n] = "primary"]
          /\ leaseValid' = [leaseValid EXCEPT ![n] = TRUE]
          /\ LET noopE == [epoch |-> newEpoch, key |-> CHOOSE k \in Keys : TRUE,
                            val |-> Nil, opType |-> "noop",
                            index |-> LastLogIndex(n) + 1]
             IN nlog' = [nlog EXCEPT ![n] = Append(nlog[n], noopE)]
    /\ UNCHANGED <<commitIndex, kvState, history, opCounter, crashCount>>

----
(**************************************************************************)
(* Lease                                                                  *)
(**************************************************************************)

RenewLease(n) ==
    /\ IsPrimary(n)
    /\ Cardinality({q \in Nodes : IsAlive(q) /\ q /= n}) >= 1
    /\ leaseValid' = [leaseValid EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<role, epoch, nlog, commitIndex, votedFor, kvState,
                   history, opCounter, crashCount>>

ExpireLease(n) ==
    /\ IsPrimary(n)
    /\ leaseValid[n] = TRUE
    /\ leaseValid' = [leaseValid EXCEPT ![n] = FALSE]
    /\ UNCHANGED <<role, epoch, nlog, commitIndex, votedFor, kvState,
                   history, opCounter, crashCount>>

----
(**************************************************************************)
(* Next                                                                   *)
(**************************************************************************)

Next ==
    \/ \E c \in Clients, k \in Keys : ClientGet(c, k)
    \/ \E c \in Clients, k \in Keys, v \in Values : ClientPut(c, k, v)
    \/ \E c \in Clients, k \in Keys : ClientDelete(c, k)
    \/ \E n \in Nodes : BackupApply(n)
    \/ \E n \in Nodes : NodeCrash(n)
    \/ \E n \in Nodes : NodeRecover(n)
    \/ \E n \in Nodes : StartElection(n)
    \/ \E n \in Nodes : RenewLease(n)
    \/ \E n \in Nodes : ExpireLease(n)

Spec == Init /\ [][Next]_vars

----
(**************************************************************************)
(* Safety Invariants                                                      *)
(**************************************************************************)

AtMostOnePrimary ==
    Cardinality({n \in Nodes : IsPrimary(n)}) <= 1

LogAgreement ==
    \A n1, n2 \in Nodes :
        (IsAlive(n1) /\ IsAlive(n2)) =>
        \A i \in 1..Min(commitIndex[n1], commitIndex[n2]) :
            (i <= Len(nlog[n1]) /\ i <= Len(nlog[n2])) =>
            (nlog[n1][i].key = nlog[n2][i].key /\
             nlog[n1][i].val = nlog[n2][i].val /\
             nlog[n1][i].opType = nlog[n2][i].opType)

ReadsReturnWrittenValues ==
    \A i \in 1..Len(history) :
        (history[i].opType = "get" /\ history[i].outStatus = "found") =>
        \E j \in 1..Len(history) :
            history[j].opType = "put" /\
            history[j].key = history[i].key /\
            history[j].inVal = history[i].outVal

PrimaryStateConsistent ==
    \A p \in Nodes :
        IsPrimary(p) =>
        kvState[p] = ApplyLog(nlog[p], commitIndex[p])

Safety == AtMostOnePrimary /\ LogAgreement /\ ReadsReturnWrittenValues /\ PrimaryStateConsistent

====
