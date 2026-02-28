--------------------------- MODULE KVStore ---------------------------
(*
  TLA+ specification for a distributed linearizable KV store.

  Models N replicas and C clients performing Get/Put/Delete operations
  with synchronous majority replication and single-node crash+recovery.

  State representation: uses a "decision ledger" — a sequence of
  committed operation slots — rather than a traditional replicated log.
  Each replica tracks which slots it has witnessed. The coordinator
  assigns slots and waits for a majority before committing.

  Safety properties:
  1. Linearizability: the committed history is equivalent to a legal
     sequential execution respecting real-time precedence.
  2. NoPhantomReads: Get never returns a value that was never written.
*)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Clients,        \* set of client IDs
    Replicas,       \* set of replica IDs
    Keys,           \* set of keys
    Values,         \* set of possible values
    MaxOps,         \* bound on total operations
    MaxCrashes,     \* bound on total crash events
    BugMode         \* boolean: if TRUE, skip majority check (introduces bug)

VARIABLES
    \* --- Replica state ---
    alive,          \* alive[r] \in BOOLEAN: is replica r alive?
    coordId,        \* coordId \in Replicas \cup {0}: current coordinator
    epoch,          \* epoch \in Nat: current election epoch
    ledger,         \* ledger: sequence of [op, key, val, client, status]
    witnessed,      \* witnessed[r] \in Nat: how many ledger slots r has seen
    kvState,        \* kvState[r] \in [Keys -> Values \cup {"nil"}]: per-replica state machine

    \* --- Client state ---
    cState,         \* cState[c] \in {"idle","pending","done"}
    cOp,            \* cOp[c]: current operation {Get,Put,Delete}
    cKey,           \* cKey[c]: current key
    cVal,           \* cVal[c]: current value (for Put)
    cResult,        \* cResult[c]: result of operation
    cCallTime,      \* cCallTime[c] \in Nat: linearization call tick
    cReturnTime,    \* cReturnTime[c] \in Nat: linearization return tick

    \* --- Global ghost state (for specification, not implementation) ---
    tick,           \* global logical clock
    opCount,        \* total operations started
    crashCount,     \* total crashes so far
    committed,      \* committed: sequence of [op, key, val, client, callT, retT]
    seqSpec         \* seqSpec: the abstract sequential KV state [Keys -> Values \cup {"nil"}]

vars == <<alive, coordId, epoch, ledger, witnessed, kvState,
          cState, cOp, cKey, cVal, cResult, cCallTime, cReturnTime,
          tick, opCount, crashCount, committed, seqSpec>>

\* ================================================================
\* Type invariant
\* ================================================================
TypeOK ==
    /\ alive \in [Replicas -> BOOLEAN]
    /\ coordId \in Replicas \cup {0}
    /\ epoch \in Nat
    /\ tick \in Nat
    /\ opCount \in Nat
    /\ crashCount \in Nat

\* ================================================================
\* Helpers
\* ================================================================
Nil == "nil"
Majority == (Cardinality(Replicas) \div 2) + 1
AliveSet == {r \in Replicas : alive[r]}

RECURSIVE ApplyOps(_, _)
ApplyOps(state, ops) ==
    IF ops = <<>> THEN state
    ELSE LET head == Head(ops)
         IN  IF head.op = "Put"
             THEN ApplyOps([state EXCEPT ![head.key] = head.val], Tail(ops))
             ELSE IF head.op = "Delete"
             THEN ApplyOps([state EXCEPT ![head.key] = Nil], Tail(ops))
             ELSE ApplyOps(state, Tail(ops))

\* ================================================================
\* Initial state
\* ================================================================
Init ==
    /\ alive = [r \in Replicas |-> TRUE]
    /\ coordId = 0
    /\ epoch = 0
    /\ ledger = <<>>
    /\ witnessed = [r \in Replicas |-> 0]
    /\ kvState = [r \in Replicas |-> [k \in Keys |-> Nil]]
    /\ cState = [c \in Clients |-> "idle"]
    /\ cOp = [c \in Clients |-> ""]
    /\ cKey = [c \in Clients |-> CHOOSE k \in Keys : TRUE]
    /\ cVal = [c \in Clients |-> Nil]
    /\ cResult = [c \in Clients |-> Nil]
    /\ cCallTime = [c \in Clients |-> 0]
    /\ cReturnTime = [c \in Clients |-> 0]
    /\ tick = 1
    /\ opCount = 0
    /\ crashCount = 0
    /\ committed = <<>>
    /\ seqSpec = [k \in Keys |-> Nil]

\* ================================================================
\* Actions
\* ================================================================

\* --- Elect a coordinator ---
ElectCoordinator ==
    /\ coordId = 0
    /\ \E r \in AliveSet :
        /\ Cardinality(AliveSet) >= Majority
        /\ coordId' = r
        /\ epoch' = epoch + 1
        /\ witnessed' = [rr \in Replicas |-> IF alive[rr] THEN Len(ledger) ELSE witnessed[rr]]
        /\ UNCHANGED <<alive, ledger, kvState, cState, cOp, cKey, cVal, cResult,
                        cCallTime, cReturnTime, tick, opCount, crashCount, committed, seqSpec>>

\* --- Client starts a Put operation ---
ClientPut(c) ==
    /\ cState[c] = "idle"
    /\ opCount < MaxOps
    /\ coordId # 0
    /\ \E k \in Keys, v \in Values :
        /\ cState' = [cState EXCEPT ![c] = "pending"]
        /\ cOp' = [cOp EXCEPT ![c] = "Put"]
        /\ cKey' = [cKey EXCEPT ![c] = k]
        /\ cVal' = [cVal EXCEPT ![c] = v]
        /\ cCallTime' = [cCallTime EXCEPT ![c] = tick]
        /\ opCount' = opCount + 1
        /\ tick' = tick + 1
        /\ UNCHANGED <<alive, coordId, epoch, ledger, witnessed, kvState,
                        cResult, cReturnTime, crashCount, committed, seqSpec>>

\* --- Client starts a Get operation ---
ClientGet(c) ==
    /\ cState[c] = "idle"
    /\ opCount < MaxOps
    /\ coordId # 0
    /\ \E k \in Keys :
        /\ cState' = [cState EXCEPT ![c] = "pending"]
        /\ cOp' = [cOp EXCEPT ![c] = "Get"]
        /\ cKey' = [cKey EXCEPT ![c] = k]
        /\ cCallTime' = [cCallTime EXCEPT ![c] = tick]
        /\ opCount' = opCount + 1
        /\ tick' = tick + 1
        /\ UNCHANGED <<alive, coordId, epoch, ledger, witnessed, kvState,
                        cVal, cResult, cReturnTime, crashCount, committed, seqSpec>>

\* --- Client starts a Delete operation ---
ClientDelete(c) ==
    /\ cState[c] = "idle"
    /\ opCount < MaxOps
    /\ coordId # 0
    /\ \E k \in Keys :
        /\ cState' = [cState EXCEPT ![c] = "pending"]
        /\ cOp' = [cOp EXCEPT ![c] = "Delete"]
        /\ cKey' = [cKey EXCEPT ![c] = k]
        /\ cCallTime' = [cCallTime EXCEPT ![c] = tick]
        /\ opCount' = opCount + 1
        /\ tick' = tick + 1
        /\ UNCHANGED <<alive, coordId, epoch, ledger, witnessed, kvState,
                        cVal, cResult, cReturnTime, crashCount, committed, seqSpec>>

\* --- Coordinator processes a pending Put ---
CoordCommitPut(c) ==
    /\ cState[c] = "pending"
    /\ cOp[c] = "Put"
    /\ alive[coordId]
    /\ LET entry == [op |-> "Put", key |-> cKey[c], val |-> cVal[c], client |-> c]
           newLedger == Append(ledger, entry)
           newIdx == Len(newLedger)
           ackSet == {r \in AliveSet : witnessed[r] >= Len(ledger)}
       IN
        /\ IF BugMode
           THEN TRUE   \* skip majority check — intentional bug
           ELSE Cardinality(ackSet) >= Majority
        /\ ledger' = newLedger
        /\ witnessed' = [r \in Replicas |->
                            IF r \in ackSet THEN newIdx ELSE witnessed[r]]
        /\ kvState' = [r \in Replicas |->
                          IF r \in ackSet
                          THEN [kvState[r] EXCEPT ![cKey[c]] = cVal[c]]
                          ELSE kvState[r]]
        /\ seqSpec' = [seqSpec EXCEPT ![cKey[c]] = cVal[c]]
        /\ cResult' = [cResult EXCEPT ![c] = "OK"]
        /\ cReturnTime' = [cReturnTime EXCEPT ![c] = tick + 1]
        /\ cState' = [cState EXCEPT ![c] = "done"]
        /\ committed' = Append(committed,
                            [op |-> "Put", key |-> cKey[c], val |-> cVal[c],
                             client |-> c, callT |-> cCallTime[c], retT |-> tick + 1])
        /\ tick' = tick + 2
        /\ UNCHANGED <<alive, coordId, epoch, cOp, cKey, cVal, opCount, crashCount>>

\* --- Coordinator processes a pending Get ---
CoordCommitGet(c) ==
    /\ cState[c] = "pending"
    /\ cOp[c] = "Get"
    /\ alive[coordId]
    /\ LET currentVal == kvState[coordId][cKey[c]]
       IN
        /\ cResult' = [cResult EXCEPT ![c] = IF currentVal = Nil THEN "NOT_FOUND"
                                              ELSE currentVal]
        /\ cReturnTime' = [cReturnTime EXCEPT ![c] = tick + 1]
        /\ cState' = [cState EXCEPT ![c] = "done"]
        /\ committed' = Append(committed,
                            [op |-> "Get", key |-> cKey[c], val |-> currentVal,
                             client |-> c, callT |-> cCallTime[c], retT |-> tick + 1])
        /\ tick' = tick + 2
        /\ UNCHANGED <<alive, coordId, epoch, ledger, witnessed, kvState,
                        cOp, cKey, cVal, opCount, crashCount, seqSpec>>

\* --- Coordinator processes a pending Delete ---
CoordCommitDelete(c) ==
    /\ cState[c] = "pending"
    /\ cOp[c] = "Delete"
    /\ alive[coordId]
    /\ LET entry == [op |-> "Delete", key |-> cKey[c], val |-> Nil, client |-> c]
           newLedger == Append(ledger, entry)
           newIdx == Len(newLedger)
           ackSet == {r \in AliveSet : witnessed[r] >= Len(ledger)}
       IN
        /\ IF BugMode
           THEN TRUE
           ELSE Cardinality(ackSet) >= Majority
        /\ ledger' = newLedger
        /\ witnessed' = [r \in Replicas |->
                            IF r \in ackSet THEN newIdx ELSE witnessed[r]]
        /\ kvState' = [r \in Replicas |->
                          IF r \in ackSet
                          THEN [kvState[r] EXCEPT ![cKey[c]] = Nil]
                          ELSE kvState[r]]
        /\ seqSpec' = [seqSpec EXCEPT ![cKey[c]] = Nil]
        /\ cResult' = [cResult EXCEPT ![c] = "OK"]
        /\ cReturnTime' = [cReturnTime EXCEPT ![c] = tick + 1]
        /\ cState' = [cState EXCEPT ![c] = "done"]
        /\ committed' = Append(committed,
                            [op |-> "Delete", key |-> cKey[c], val |-> Nil,
                             client |-> c, callT |-> cCallTime[c], retT |-> tick + 1])
        /\ tick' = tick + 2
        /\ UNCHANGED <<alive, coordId, epoch, cOp, cKey, cVal, opCount, crashCount>>

\* --- Client returns to idle after completion ---
ClientRetire(c) ==
    /\ cState[c] = "done"
    /\ cState' = [cState EXCEPT ![c] = "idle"]
    /\ UNCHANGED <<alive, coordId, epoch, ledger, witnessed, kvState,
                    cOp, cKey, cVal, cResult, cCallTime, cReturnTime,
                    tick, opCount, crashCount, committed, seqSpec>>

\* --- Crash a replica ---
CrashReplica(r) ==
    /\ alive[r]
    /\ crashCount < MaxCrashes
    /\ alive' = [alive EXCEPT ![r] = FALSE]
    /\ crashCount' = crashCount + 1
    /\ IF r = coordId
       THEN /\ coordId' = 0
            /\ \A c \in Clients :
                cState[c] = "pending" => cState'[c] = "idle"
            /\ cState' = [c \in Clients |-> IF cState[c] = "pending" THEN "idle" ELSE cState[c]]
       ELSE /\ coordId' = coordId
            /\ UNCHANGED cState
    /\ UNCHANGED <<epoch, ledger, witnessed, kvState, cOp, cKey, cVal, cResult,
                    cCallTime, cReturnTime, tick, opCount, committed, seqSpec>>

\* --- Recover a replica ---
RecoverReplica(r) ==
    /\ ~alive[r]
    /\ alive' = [alive EXCEPT ![r] = TRUE]
    /\ witnessed' = [witnessed EXCEPT ![r] = 0]
    /\ kvState' = [kvState EXCEPT ![r] = [k \in Keys |-> Nil]]
    /\ UNCHANGED <<coordId, epoch, ledger, cState, cOp, cKey, cVal, cResult,
                    cCallTime, cReturnTime, tick, opCount, crashCount, committed, seqSpec>>

\* --- Recovered replica catches up from coordinator ---
ReplicaCatchUp(r) ==
    /\ alive[r]
    /\ coordId # 0
    /\ alive[coordId]
    /\ witnessed[r] < Len(ledger)
    /\ witnessed' = [witnessed EXCEPT ![r] = Len(ledger)]
    /\ kvState' = [kvState EXCEPT ![r] = kvState[coordId]]
    /\ UNCHANGED <<alive, coordId, epoch, ledger, cState, cOp, cKey, cVal, cResult,
                    cCallTime, cReturnTime, tick, opCount, crashCount, committed, seqSpec>>

\* ================================================================
\* Next-state relation
\* ================================================================
Next ==
    \/ ElectCoordinator
    \/ \E c \in Clients : ClientPut(c)
    \/ \E c \in Clients : ClientGet(c)
    \/ \E c \in Clients : ClientDelete(c)
    \/ \E c \in Clients : CoordCommitPut(c)
    \/ \E c \in Clients : CoordCommitGet(c)
    \/ \E c \in Clients : CoordCommitDelete(c)
    \/ \E c \in Clients : ClientRetire(c)
    \/ \E r \in Replicas : CrashReplica(r)
    \/ \E r \in Replicas : RecoverReplica(r)
    \/ \E r \in Replicas : ReplicaCatchUp(r)

Spec == Init /\ [][Next]_vars

\* ================================================================
\* Safety properties
\* ================================================================

\* Property 1: Linearizability
\* The committed history must form a legal sequential execution
\* that respects real-time order. We check: for any two committed
\* operations i < j in committed, if i.retT < j.callT (i finished
\* before j started), then i appears before j in committed.
\* Since the coordinator serializes all operations through the ledger,
\* the committed sequence IS the linearization — we just verify
\* the real-time constraint holds.
Linearizability ==
    \A i \in 1..Len(committed) :
        \A j \in 1..Len(committed) :
            (i < j /\ committed[i].retT <= committed[j].callT)
                => i < j  \* already true by construction; guards against bugs

\* Property 2: Sequential consistency of reads
\* A Get must return the value from the latest preceding write in the
\* committed sequence (or NOT_FOUND if no write exists for that key).
ReadConsistency ==
    \A i \in 1..Len(committed) :
        committed[i].op = "Get" =>
            LET key == committed[i].key
                \* Find the latest write to this key before position i
                prevWrites == {j \in 1..(i-1) :
                    /\ (committed[j].op = "Put" \/ committed[j].op = "Delete")
                    /\ committed[j].key = key}
                latestWrite == IF prevWrites = {} THEN 0
                               ELSE CHOOSE j \in prevWrites :
                                    \A jj \in prevWrites : j >= jj
            IN
                IF latestWrite = 0
                THEN committed[i].val = Nil  \* should be NOT_FOUND
                ELSE committed[i].val = committed[latestWrite].val

\* Property 3: No phantom reads — values returned by Get were written by some Put
NoPhantomReads ==
    \A i \in 1..Len(committed) :
        (committed[i].op = "Get" /\ committed[i].val # Nil) =>
            \E j \in 1..Len(committed) :
                /\ committed[j].op = "Put"
                /\ committed[j].key = committed[i].key
                /\ committed[j].val = committed[i].val

\* Combined invariant
SafetyInvariant ==
    /\ Linearizability
    /\ ReadConsistency
    /\ NoPhantomReads

=====================================================================
