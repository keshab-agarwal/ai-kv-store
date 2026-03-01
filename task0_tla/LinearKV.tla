--------------------------- MODULE LinearKV ---------------------------
(**************************************************************************)
(* A TLA+ specification for a linearizable distributed key-value store   *)
(* using an "epoch quorum" approach.                                     *)
(*                                                                       *)
(* Design:                                                               *)
(*  - Multiple replicas; one is coordinator per epoch.                   *)
(*  - Coordinator handles writes by replicating to a quorum.             *)
(*  - Reads served from coordinator (guarantees linearizability).        *)
(*  - On crash, a new epoch is started with a new coordinator.           *)
(*  - BugMode enables a stale-read action that violates linearizability. *)
(**************************************************************************)

EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS Clients,    \* Set of client identifiers
          Replicas,   \* Set of replica identifiers
          Keys,       \* Set of keys
          Values,     \* Set of values
          MaxOps,     \* Bound on total operations
          BugMode     \* Boolean: TRUE enables stale-read bug

\* We use a model value for "no value" - define it as a string constant
\* that is guaranteed not to be in Values
NoValue == "NoValue"

VARIABLES replicaState,  \* Function: replica -> record
          clientState,   \* Function: client -> record
          log,           \* Sequence of committed log entries
          epoch,         \* Current global epoch number
          opCounter      \* Monotonic operation ID counter

vars == <<replicaState, clientState, log, epoch, opCounter>>

---------------------------------------------------------------------
(* Helper: majority quorum *)
Quorum == {Q \in SUBSET Replicas : Cardinality(Q) * 2 > Cardinality(Replicas)}

(* Operation types *)
OpGet == "get"
OpPut == "put"
OpDel == "delete"

(* Replica roles *)
RoleCoord == "coordinator"
RoleFollow == "follower"
RoleNone == "none"

(* All possible stored values including NoValue *)
StoreValues == Values \cup {NoValue}

---------------------------------------------------------------------
(* Type invariant *)

TypeInvariant ==
    /\ \A r \in Replicas :
        /\ replicaState[r].store \in [Keys -> StoreValues]
        /\ replicaState[r].epoch \in Nat
        /\ replicaState[r].role \in {RoleCoord, RoleFollow, RoleNone}
        /\ replicaState[r].alive \in BOOLEAN
        /\ replicaState[r].appliedSeq \in Nat
    /\ \A c \in Clients :
        /\ clientState[c].pending \in {"none"}
        /\ \A i \in 1..Len(clientState[c].history) :
            /\ clientState[c].history[i].op \in {OpGet, OpPut, OpDel}
            /\ clientState[c].history[i].key \in Keys
            /\ clientState[c].history[i].inVal \in StoreValues
            /\ clientState[c].history[i].outVal \in StoreValues
            /\ clientState[c].history[i].id \in Nat
            /\ clientState[c].history[i].start \in Nat
            /\ clientState[c].history[i].end \in Nat
    /\ epoch \in Nat
    /\ opCounter \in Nat

---------------------------------------------------------------------
(* Initial state *)

EmptyStore == [k \in Keys |-> NoValue]

Init ==
    /\ epoch = 1
    /\ opCounter = 0
    /\ log = <<>>
    /\ LET coord == CHOOSE r \in Replicas : TRUE
       IN replicaState = [r \in Replicas |->
            [store      |-> EmptyStore,
             epoch      |-> 1,
             role       |-> IF r = coord THEN RoleCoord ELSE RoleFollow,
             alive      |-> TRUE,
             appliedSeq |-> 0]]
    /\ clientState = [c \in Clients |->
            [pending |-> "none",
             history |-> <<>>]]

---------------------------------------------------------------------
(* Helper: get the current coordinator *)
HasCoordinator ==
    \E r \in Replicas :
        /\ replicaState[r].role = RoleCoord
        /\ replicaState[r].alive
        /\ replicaState[r].epoch = epoch

Coordinator ==
    CHOOSE r \in Replicas :
        /\ replicaState[r].role = RoleCoord
        /\ replicaState[r].alive
        /\ replicaState[r].epoch = epoch

(* Helper: alive replicas *)
AliveReplicas == {r \in Replicas : replicaState[r].alive}

(* Recover store state from the log *)
StoreFromLog ==
    [k \in Keys |->
        IF \E i \in 1..Len(log) : log[i].key = k
        THEN LET maxIdx == CHOOSE i \in 1..Len(log) :
                    /\ log[i].key = k
                    /\ \A j \in 1..Len(log) : log[j].key = k => j <= i
             IN log[maxIdx].val
        ELSE NoValue]

---------------------------------------------------------------------
(* CLIENT ACTIONS *)

(* Client submits a Get request - served by coordinator *)
ClientGet(c, k) ==
    /\ clientState[c].pending = "none"
    /\ opCounter < MaxOps
    /\ HasCoordinator
    /\ LET coord  == Coordinator
           result == replicaState[coord].store[k]
           newId  == opCounter + 1
       IN
       /\ clientState' = [clientState EXCEPT
            ![c].history = Append(@, [op     |-> OpGet,
                                      key    |-> k,
                                      inVal  |-> NoValue,
                                      outVal |-> result,
                                      id     |-> newId,
                                      start  |-> newId,
                                      end    |-> newId])]
       /\ opCounter' = newId
    /\ UNCHANGED <<replicaState, log, epoch>>

(* Client submits a Put request - coordinator replicates to quorum *)
ClientPut(c, k, v) ==
    /\ clientState[c].pending = "none"
    /\ opCounter < MaxOps
    /\ HasCoordinator
    /\ LET coord  == Coordinator
           newId  == opCounter + 1
           newSeq == Len(log) + 1
           entry  == [key |-> k, val |-> v, epoch |-> epoch, seq |-> newSeq]
       IN
       \E Q \in Quorum :
           /\ Q \subseteq AliveReplicas
           /\ coord \in Q
           /\ log' = Append(log, entry)
           /\ replicaState' = [r \in Replicas |->
                IF r \in Q
                THEN [replicaState[r] EXCEPT
                        !.store = [@ EXCEPT ![k] = v],
                        !.appliedSeq = newSeq]
                ELSE replicaState[r]]
           /\ clientState' = [clientState EXCEPT
                ![c].history = Append(@, [op     |-> OpPut,
                                          key    |-> k,
                                          inVal  |-> v,
                                          outVal |-> v,
                                          id     |-> newId,
                                          start  |-> newId,
                                          end    |-> newId])]
           /\ opCounter' = newId
           /\ UNCHANGED <<epoch>>

(* Client submits a Delete request *)
ClientDelete(c, k) ==
    /\ clientState[c].pending = "none"
    /\ opCounter < MaxOps
    /\ HasCoordinator
    /\ LET coord  == Coordinator
           newId  == opCounter + 1
           newSeq == Len(log) + 1
           entry  == [key |-> k, val |-> NoValue, epoch |-> epoch, seq |-> newSeq]
       IN
       \E Q \in Quorum :
           /\ Q \subseteq AliveReplicas
           /\ coord \in Q
           /\ log' = Append(log, entry)
           /\ replicaState' = [r \in Replicas |->
                IF r \in Q
                THEN [replicaState[r] EXCEPT
                        !.store = [@ EXCEPT ![k] = NoValue],
                        !.appliedSeq = newSeq]
                ELSE replicaState[r]]
           /\ clientState' = [clientState EXCEPT
                ![c].history = Append(@, [op     |-> OpDel,
                                          key    |-> k,
                                          inVal  |-> NoValue,
                                          outVal |-> NoValue,
                                          id     |-> newId,
                                          start  |-> newId,
                                          end    |-> newId])]
           /\ opCounter' = newId
           /\ UNCHANGED <<epoch>>

---------------------------------------------------------------------
(* FAILURE AND RECOVERY ACTIONS *)

(* A replica crashes *)
NodeCrash(r) ==
    /\ replicaState[r].alive = TRUE
    /\ Cardinality(AliveReplicas \ {r}) * 2 > Cardinality(Replicas)
    /\ replicaState' = [replicaState EXCEPT
            ![r].alive = FALSE,
            ![r].role  = RoleNone]
    /\ UNCHANGED <<clientState, log, epoch, opCounter>>

(* A crashed replica recovers - rejoins as follower with state rebuilt from log *)
NodeRecover(r) ==
    /\ replicaState[r].alive = FALSE
    /\ replicaState' = [replicaState EXCEPT
            ![r].alive      = TRUE,
            ![r].role       = RoleFollow,
            ![r].epoch      = epoch,
            ![r].store      = StoreFromLog,
            ![r].appliedSeq = Len(log)]
    /\ UNCHANGED <<clientState, log, epoch, opCounter>>

(* Start a new epoch when coordinator has crashed *)
StartNewEpoch ==
    /\ ~HasCoordinator
    /\ \E newCoord \in AliveReplicas :
        LET newEpoch   == epoch + 1
            freshStore == StoreFromLog
        IN
        /\ epoch' = newEpoch
        /\ replicaState' = [r \in Replicas |->
             IF r = newCoord
             THEN [replicaState[r] EXCEPT
                     !.role       = RoleCoord,
                     !.epoch      = newEpoch,
                     !.store      = freshStore,
                     !.appliedSeq = Len(log)]
             ELSE IF replicaState[r].alive
                  THEN [replicaState[r] EXCEPT
                         !.role       = RoleFollow,
                         !.epoch      = newEpoch,
                         !.store      = freshStore,
                         !.appliedSeq = Len(log)]
                  ELSE replicaState[r]]
        /\ UNCHANGED <<clientState, log, opCounter>>

---------------------------------------------------------------------
(* BUG MODE: Stale Read *)
(* A follower serves a read from its local (possibly stale) state.   *)
(* This violates linearizability because a client can see an old     *)
(* value after another client already observed a newer write.        *)

StaleRead(c, k) ==
    /\ BugMode = TRUE
    /\ clientState[c].pending = "none"
    /\ opCounter < MaxOps
    /\ \E r \in Replicas :
        /\ replicaState[r].alive
        /\ replicaState[r].role = RoleFollow
        /\ LET result == replicaState[r].store[k]
               newId  == opCounter + 1
           IN
           /\ clientState' = [clientState EXCEPT
                ![c].history = Append(@, [op     |-> OpGet,
                                          key    |-> k,
                                          inVal  |-> NoValue,
                                          outVal |-> result,
                                          id     |-> newId,
                                          start  |-> newId,
                                          end    |-> newId])]
           /\ opCounter' = newId
    /\ UNCHANGED <<replicaState, log, epoch>>

---------------------------------------------------------------------
(* LINEARIZABILITY INVARIANT *)
(*                                                                       *)
(* For all completed operations, there must exist a legal sequential     *)
(* ordering S such that:                                                 *)
(*   1) S is equivalent (same per-client order and results)              *)
(*   2) Real-time precedence is preserved                                *)
(*   3) S is a legal execution of a sequential KV store                  *)
(**************************************************************************)

\* Collect all completed operations into a single set
AllOps == UNION {
    {clientState[c].history[i] : i \in 1..Len(clientState[c].history)}
    : c \in Clients}

\* Check whether a sequence of ops is a legal sequential KV-store execution
RECURSIVE IsLegalExec(_, _, _)
IsLegalExec(s, idx, st) ==
    IF idx > Len(s) THEN TRUE
    ELSE LET op == s[idx]
         IN IF op.op = OpGet
            THEN /\ op.outVal = st[op.key]
                 /\ IsLegalExec(s, idx + 1, st)
            ELSE IF op.op = OpPut
            THEN IsLegalExec(s, idx + 1, [st EXCEPT ![op.key] = op.inVal])
            ELSE \* OpDel
                 IsLegalExec(s, idx + 1, [st EXCEPT ![op.key] = NoValue])

IsLegalSeqExec(s) == IsLegalExec(s, 1, EmptyStore)

\* Check that a sequence respects per-client ordering
RespectsClientOrder(s) ==
    \A c \in Clients :
        \A i, j \in 1..Len(clientState[c].history) :
            i < j =>
                LET opi == clientState[c].history[i]
                    opj == clientState[c].history[j]
                IN \E si, sj \in 1..Len(s) :
                    /\ s[si] = opi
                    /\ s[sj] = opj
                    /\ si < sj

\* Check that a sequence respects real-time precedence
RespectsRealTime(s) ==
    \A si, sj \in 1..Len(s) :
        (s[si].end < s[sj].start) => si < sj

\* Generate all permutations of a set
RECURSIVE AllPermsOf(_)
AllPermsOf(S) ==
    IF S = {} THEN {<<>>}
    ELSE UNION {{<<x>> \o sq : sq \in AllPermsOf(S \ {x})} : x \in S}

Linearizability ==
    LET ops == AllOps
    IN IF ops = {} THEN TRUE
       ELSE \E s \in AllPermsOf(ops) :
                /\ IsLegalSeqExec(s)
                /\ RespectsClientOrder(s)
                /\ RespectsRealTime(s)

---------------------------------------------------------------------
(* SPECIFICATION *)

Next ==
    \/ \E c \in Clients, k \in Keys : ClientGet(c, k)
    \/ \E c \in Clients, k \in Keys, v \in Values : ClientPut(c, k, v)
    \/ \E c \in Clients, k \in Keys : ClientDelete(c, k)
    \/ \E r \in Replicas : NodeCrash(r)
    \/ \E r \in Replicas : NodeRecover(r)
    \/ StartNewEpoch
    \/ \E c \in Clients, k \in Keys : StaleRead(c, k)

Spec == Init /\ [][Next]_vars

=========================================================================
