------------------------------ MODULE KVLinearizable ------------------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC

(***************************************************************************
A fresh model using an operation-wave representation:
- clients create operation waves (invoke/return timestamps)
- leader commits write waves synchronously to all alive replicas
- one-node crash and bounded-step recovery are explicit
***************************************************************************)

CONSTANTS
  Clients,
  Replicas,
  Keys,
  Values,
  MaxSteps,
  RecoveryBound,
  BugMode

ASSUME Clients # {}
ASSUME Replicas # {}
ASSUME Keys # {}
ASSUME Values # {}
ASSUME RecoveryBound \in Nat
ASSUME MaxSteps \in Nat
ASSUME BugMode \in BOOLEAN

NullValue == "<NULL>"
NoOp == "noop"

OpTypes == {"get", "put", "delete"}
RespTypes == {"OK", "FOUND", "NOT_FOUND", "ERROR"}

Leader == CHOOSE r \in Replicas : TRUE

VARIABLES
  step,
  alive,
  recovering,
  store,
  nextOp,
  inflight,
  ops,
  linOrder,
  committed,
  lastWriteSet

vars == <<step, alive, recovering, store, nextOp, inflight, ops, linOrder, committed, lastWriteSet>>

Pos(seq, x) ==
  CHOOSE i \in 1..Len(seq) : seq[i] = x

OpRecord(id, c, t, k, v, inv) ==
  [ id        |-> id,
    client    |-> c,
    typ       |-> t,
    key       |-> k,
    val       |-> v,
    invStep   |-> inv,
    retStep   |-> 0,
    status    |-> "PENDING",
    outVal    |-> NullValue ]

InitStore == [r \in Replicas |-> [k \in Keys |-> NullValue]]

Init ==
  /\ step = 0
  /\ alive = Replicas
  /\ recovering = [r \in Replicas |-> 0]
  /\ store = InitStore
  /\ nextOp = 1
  /\ inflight = {}
  /\ ops = [i \in 1..(MaxSteps * Cardinality(Clients) + 5) |->
               [ id |-> i, client |-> CHOOSE c \in Clients : TRUE, typ |-> "get",
                 key |-> CHOOSE k \in Keys : TRUE, val |-> NullValue,
                 invStep |-> 0, retStep |-> 0, status |-> "UNUSED", outVal |-> NullValue ]]
  /\ linOrder = <<>>
  /\ committed = <<>>
  /\ lastWriteSet = {}

ClientInvoke ==
  /\ step < MaxSteps
  /\ \E c \in Clients:
      LET busy == \E id \in inflight : ops[id].client = c IN
      /\ ~busy
      /\ \E t \in OpTypes, k \in Keys, v \in Values:
          /\ step' = step + 1
          /\ nextOp' = nextOp + 1
          /\ inflight' = inflight \cup {nextOp}
          /\ ops' = [ops EXCEPT ![nextOp] = OpRecord(nextOp, c, t, k, v, step + 1)]
          /\ UNCHANGED <<alive, recovering, store, linOrder, committed, lastWriteSet>>

LatestValue(k) == store[Leader][k]

CompleteGet(id) ==
  /\ id \in inflight
  /\ ops[id].typ = "get"
  /\ Leader \in alive
  /\ step' = step + 1
  /\ inflight' = inflight \ {id}
  /\ LET v == LatestValue(ops[id].key) IN
     LET ghost == CHOOSE x \in Values : TRUE IN
     /\ ops' = [ops EXCEPT
                  ![id].retStep = step + 1,
                  ![id].status = IF v = NullValue /\ BugMode THEN "FOUND"
                                 ELSE IF v = NullValue THEN "NOT_FOUND" ELSE "FOUND",
                  ![id].outVal = IF v = NullValue /\ BugMode THEN ghost ELSE v]
  /\ linOrder' = Append(linOrder, id)
  /\ committed' = Append(committed,
        [id |-> id, typ |-> "get", key |-> ops[id].key, val |-> ops[id].outVal, status |-> ops'[id].status])
  /\ UNCHANGED <<alive, recovering, store, nextOp, lastWriteSet>>

ApplyWriteAll(op) ==
  [r \in Replicas |->
      [k \in Keys |->
         IF k # op.key THEN store[r][k]
         ELSE IF op.typ = "put" THEN op.val
         ELSE IF op.typ = "delete" THEN
               IF BugMode THEN store[r][k] ELSE NullValue
              ELSE store[r][k]]]

CompleteWrite(id) ==
  /\ id \in inflight
  /\ ops[id].typ \in {"put", "delete"}
  /\ Leader \in alive
  /\ Cardinality(alive) >= Cardinality(Replicas) - 1
  /\ step' = step + 1
  /\ inflight' = inflight \ {id}
  /\ ops' = [ops EXCEPT
              ![id].retStep = step + 1,
              ![id].status = "OK"]
  /\ store' = ApplyWriteAll(ops[id])
  /\ linOrder' = Append(linOrder, id)
  /\ committed' = Append(committed,
       [id |-> id, typ |-> ops[id].typ, key |-> ops[id].key, val |-> ops[id].val, status |-> "OK"])
  /\ lastWriteSet' = lastWriteSet \cup {ops[id].val}
  /\ UNCHANGED <<alive, recovering, nextOp>>

CrashOne ==
  /\ step < MaxSteps
  /\ Cardinality(alive) > Cardinality(Replicas) - 1
  /\ \E r \in alive \ {Leader}:
      /\ step' = step + 1
      /\ alive' = alive \ {r}
      /\ recovering' = [recovering EXCEPT ![r] = RecoveryBound]
      /\ UNCHANGED <<store, nextOp, inflight, ops, linOrder, committed, lastWriteSet>>

RecoverOne ==
  /\ step < MaxSteps
  /\ \E r \in Replicas \ alive:
      /\ recovering[r] = 0
      /\ step' = step + 1
      /\ alive' = alive \cup {r}
      /\ recovering' = recovering
      /\ store' = [store EXCEPT ![r] = store[Leader]]
      /\ UNCHANGED <<nextOp, inflight, ops, linOrder, committed, lastWriteSet>>

TickRecovery ==
  /\ step < MaxSteps
  /\ \E r \in Replicas : recovering[r] > 0
  /\ step' = step + 1
  /\ recovering' = [r \in Replicas |-> IF recovering[r] > 0 THEN recovering[r] - 1 ELSE 0]
  /\ UNCHANGED <<alive, store, nextOp, inflight, ops, linOrder, committed, lastWriteSet>>

NoOpStep ==
  /\ step < MaxSteps
  /\ step' = step + 1
  /\ UNCHANGED <<alive, recovering, store, nextOp, inflight, ops, linOrder, committed, lastWriteSet>>

Next ==
  \/ ClientInvoke
  \/ \E id \in inflight : CompleteGet(id)
  \/ \E id \in inflight : CompleteWrite(id)
  \/ CrashOne
  \/ RecoverOne
  \/ TickRecovery
  \/ NoOpStep

Spec == Init /\ [][Next]_vars

TypeInv ==
  /\ step \in Nat
  /\ alive \subseteq Replicas
  /\ recovering \in [Replicas -> Nat]
  /\ store \in [Replicas -> [Keys -> (Values \cup {NullValue})]]
  /\ nextOp \in Nat
  /\ inflight \subseteq 1..(MaxSteps * Cardinality(Clients) + 5)
  /\ linOrder \in Seq(1..(MaxSteps * Cardinality(Clients) + 5))

LinearizationRespectsRealTime ==
  \A i, j \in 1..Len(linOrder):
    LET a == linOrder[i] IN
    LET b == linOrder[j] IN
    LET oa == ops[a] IN
    LET ob == ops[b] IN
      (oa.retStep # 0 /\ ob.invStep # 0 /\ oa.retStep < ob.invStep) =>
      i < j

LegalSequentialSemantics ==
  \A i \in 1..Len(linOrder):
    LET id == linOrder[i] IN
    LET op == ops[id] IN
      IF op.typ = "put" THEN op.status = "OK"
      ELSE IF op.typ = "delete" THEN op.status = "OK"
      ELSE IF op.typ = "get" THEN op.status \in {"FOUND", "NOT_FOUND"}
      ELSE FALSE

NoPhantomRead ==
  \A i \in 1..Len(linOrder):
    LET id == linOrder[i] IN
    LET op == ops[id] IN
      (op.typ = "get" /\ op.status = "FOUND") =>
      op.outVal \in lastWriteSet

THEOREM Spec => []TypeInv

=============================================================================
