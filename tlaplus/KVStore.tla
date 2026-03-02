---------------------------- MODULE KVStore ----------------------------
(* Photon Quorum Protocol — Distributed KV Store                       *)
(*                                                                      *)
(* Creative framing: nodes maintain "energy coherence" for reads.      *)
(* Writes are "photon pulses" that collapse into committed state when   *)
(* absorbed by a quorum of replica nodes. The primary holds a          *)
(* "coherence window" (lease) during which reads are served locally.   *)
(*                                                                      *)
(* Model: N nodes, one shard, RF=N. Operations: Put, Get, Delete.      *)
(* Safety properties: linearizability, at-most-one-leader-per-epoch,   *)
(* and committed-entries-not-lost across leadership changes.           *)
(*                                                                      *)
(* Bug mode: INJECT_BUG=TRUE lets a primary serve reads with an        *)
(* expired lease, enabling stale reads that violate linearizability.   *)

EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Nodes,       \* set of node IDs, e.g. {1,2,3}
    MaxEpoch,    \* maximum epoch number
    Keys,        \* set of keys, e.g. {"k"}
    Values,      \* set of values, e.g. {"v1","v2"}
    MaxLogLen,   \* maximum log length bound for TLC
    Nil,         \* sentinel model value
    INJECT_BUG   \* TRUE to inject the stale-read bug

ASSUME
    /\ Nodes    # {}
    /\ MaxEpoch \in Nat \ {0}
    /\ Keys     # {}
    /\ Values   # {}
    /\ MaxLogLen \in Nat \ {0}
    /\ Nil \notin Values
    /\ Nil \notin Keys
    /\ Nil \notin Nodes
    /\ INJECT_BUG \in {TRUE, FALSE}

(* Symmetry set for TLC — nodes are interchangeable. *)
Symmetry == Permutations(Nodes)

------------------------------------------------------------------------
(* Quorum helpers                                                        *)
------------------------------------------------------------------------

Quorum(S) == {Q \in SUBSET S : 2 * Cardinality(Q) > Cardinality(S)}
IsQuorum(Q) == Q \in Quorum(Nodes)

------------------------------------------------------------------------
(* State variables                                                       *)
------------------------------------------------------------------------

VARIABLES
    role,          \* role[n] \in {"Primary","Secondary","Candidate"}
    epoch,         \* epoch[n] \in Nat  (energy level / term)
    nLog,          \* nLog[n] = sequence of log entries
    commitIdx,     \* commitIdx[n] \in Nat (0 = nothing committed)
    votedFor,      \* votedFor[n] \in Nodes \cup {Nil}
    votedEpoch,    \* votedEpoch[n] \in Nat
    leaseOk,       \* leaseOk[n] \in BOOLEAN (coherence window active)
    commitHist,    \* global append-only sequence of committed entries
    kvSt           \* kvSt[n][k] \in Values \cup {Nil}

vars == <<role, epoch, nLog, commitIdx, votedFor, votedEpoch,
          leaseOk, commitHist, kvSt>>

------------------------------------------------------------------------
(* Type invariant                                                        *)
------------------------------------------------------------------------

LogEntryOK(e) ==
    /\ e.epoch  \in Nat
    /\ e.index  \in Nat
    /\ e.op     \in {"put", "delete", "noop"}
    /\ e.key    \in Keys \cup {Nil}
    /\ e.value  \in Values \cup {Nil}

TypeInvariant ==
    /\ \A n \in Nodes :
        /\ role[n]       \in {"Primary", "Secondary", "Candidate"}
        /\ epoch[n]      \in Nat
        /\ commitIdx[n]  \in Nat
        /\ leaseOk[n]    \in BOOLEAN
        /\ votedFor[n]   \in Nodes \cup {Nil}
        /\ votedEpoch[n] \in Nat
        /\ \A i \in DOMAIN nLog[n] : LogEntryOK(nLog[n][i])
        /\ \A k \in Keys : kvSt[n][k] \in Values \cup {Nil}
    /\ \A i \in DOMAIN commitHist :
        /\ commitHist[i].key   \in Keys \cup {Nil}
        /\ commitHist[i].op    \in {"put", "delete", "noop"}
        /\ commitHist[i].value \in Values \cup {Nil}

------------------------------------------------------------------------
(* Initial state                                                         *)
------------------------------------------------------------------------

Init ==
    /\ role      = [n \in Nodes |-> "Secondary"]
    /\ epoch     = [n \in Nodes |-> 0]
    /\ nLog      = [n \in Nodes |-> <<>>]
    /\ commitIdx = [n \in Nodes |-> 0]
    /\ votedFor  = [n \in Nodes |-> Nil]
    /\ votedEpoch = [n \in Nodes |-> 0]
    /\ leaseOk   = [n \in Nodes |-> FALSE]
    /\ commitHist = <<>>
    /\ kvSt      = [n \in Nodes |-> [k \in Keys |-> Nil]]

------------------------------------------------------------------------
(* Helper operators (no RECURSIVE — uses set-based CHOOSE)              *)
------------------------------------------------------------------------

(* Last log index for node n. *)
LastIdx(n) ==
    IF nLog[n] = <<>> THEN 0 ELSE nLog[n][Len(nLog[n])].index

(* Last log epoch for node n. *)
LastEp(n) ==
    IF nLog[n] = <<>> THEN 0 ELSE nLog[n][Len(nLog[n])].epoch

(* Is candidate at least as up-to-date as n? *)
CandUpToDate(cLastEp, cLastIdx, n) ==
    \/ cLastEp > LastEp(n)
    \/ /\ cLastEp = LastEp(n)
       /\ cLastIdx >= LastIdx(n)

(* Apply one entry to a KV function. *)
ApplyOne(kv, e) ==
    IF      e.op = "put"    THEN [kv EXCEPT ![e.key] = e.value]
    ELSE IF e.op = "delete" THEN [kv EXCEPT ![e.key] = Nil]
    ELSE    kv

(* Compute the KV state from a sequence of committed entries (commitHist).
   Uses set-based CHOOSE: for each key, find the LAST entry that touches it.
   Avoids RECURSIVE entirely; TLC handles this well on small bounded sets. *)
CommittedKV ==
    [k \in Keys |->
        IF \E i \in DOMAIN commitHist :
               commitHist[i].key = k /\ commitHist[i].op \in {"put", "delete"}
        THEN
            LET last == CHOOSE i \in DOMAIN commitHist :
                            /\ commitHist[i].key = k
                            /\ commitHist[i].op \in {"put", "delete"}
                            /\ \A j \in DOMAIN commitHist :
                                (commitHist[j].key = k /\
                                 commitHist[j].op \in {"put", "delete"})
                                => j <= i
            IN IF commitHist[last].op = "delete" THEN Nil
               ELSE commitHist[last].value
        ELSE Nil]

------------------------------------------------------------------------
(* Election: Candidate wins quorum of votes                             *)
------------------------------------------------------------------------

BecomeCandidate(n) ==
    /\ role[n]   = "Secondary"
    /\ epoch[n]  < MaxEpoch
    /\ epoch'     = [epoch     EXCEPT ![n] = epoch[n] + 1]
    /\ role'      = [role      EXCEPT ![n] = "Candidate"]
    /\ votedFor'  = [votedFor  EXCEPT ![n] = n]
    /\ votedEpoch' = [votedEpoch EXCEPT ![n] = epoch[n] + 1]
    /\ UNCHANGED <<nLog, commitIdx, leaseOk, commitHist, kvSt>>

GrantVote(voter, cand) ==
    /\ role[cand]  = "Candidate"
    /\ voter # cand
    /\ epoch[voter] <= epoch[cand]
    /\ \/ votedEpoch[voter] < epoch[cand]
       \/ /\ votedEpoch[voter] = epoch[cand]
          /\ votedFor[voter]   = cand
    /\ CandUpToDate(LastEp(cand), LastIdx(cand), voter)
    /\ votedFor'   = [votedFor   EXCEPT ![voter] = cand]
    /\ votedEpoch' = [votedEpoch EXCEPT ![voter] = epoch[cand]]
    /\ epoch'      = [epoch      EXCEPT ![voter] = epoch[cand]]
    /\ role'       = [role       EXCEPT ![voter] = "Secondary"]
    /\ UNCHANGED <<nLog, commitIdx, leaseOk, commitHist, kvSt>>

WinElection(n) ==
    /\ role[n] = "Candidate"
    /\ IsQuorum({v \in Nodes : votedFor[v] = n /\ votedEpoch[v] = epoch[n]})
    /\ Len(nLog[n]) < MaxLogLen
    /\ LET noopE == [epoch |-> epoch[n], index |-> LastIdx(n) + 1,
                     op |-> "noop", key |-> Nil, value |-> Nil]
       IN /\ nLog'      = [nLog      EXCEPT ![n] = Append(nLog[n], noopE)]
          /\ commitIdx' = [commitIdx EXCEPT ![n] = Len(commitHist)]
          /\ kvSt'      = [kvSt      EXCEPT ![n] = CommittedKV]
    /\ role'    = [role EXCEPT ![n] = "Primary"]
    (* In non-bug mode, winning a new election models the real-time property
       that the old primary's lease expires before the new one can win.
       In bug mode the old lease is left alive — enabling the stale read bug. *)
    /\ leaseOk' = [m \in Nodes |->
                    IF m = n THEN FALSE
                    ELSE IF ~INJECT_BUG /\ role[m] = "Primary" /\ epoch[m] < epoch[n]
                    THEN FALSE
                    ELSE leaseOk[m]]
    /\ UNCHANGED <<epoch, votedFor, votedEpoch, commitHist>>

StepDown(n, hi) ==
    /\ hi > epoch[n]
    /\ hi <= MaxEpoch
    /\ role'    = [role    EXCEPT ![n] = "Secondary"]
    /\ epoch'   = [epoch   EXCEPT ![n] = hi]
    /\ votedFor' = [votedFor EXCEPT ![n] = Nil]
    /\ leaseOk' = [leaseOk EXCEPT ![n] = FALSE]
    /\ UNCHANGED <<nLog, commitIdx, votedEpoch, commitHist, kvSt>>

------------------------------------------------------------------------
(* Write path: Primary appends + replicates                             *)
------------------------------------------------------------------------

ClientPut(p, k, v) ==
    /\ role[p] = "Primary"
    /\ k \in Keys
    /\ v \in Values
    /\ Len(nLog[p]) < MaxLogLen
    /\ LET e == [epoch |-> epoch[p], index |-> LastIdx(p) + 1,
                 op |-> "put", key |-> k, value |-> v]
       IN nLog' = [nLog EXCEPT ![p] = Append(nLog[p], e)]
    /\ UNCHANGED <<role, epoch, commitIdx, votedFor, votedEpoch,
                   leaseOk, commitHist, kvSt>>

ClientDelete(p, k) ==
    /\ role[p] = "Primary"
    /\ k \in Keys
    /\ Len(nLog[p]) < MaxLogLen
    /\ LET e == [epoch |-> epoch[p], index |-> LastIdx(p) + 1,
                 op |-> "delete", key |-> k, value |-> Nil]
       IN nLog' = [nLog EXCEPT ![p] = Append(nLog[p], e)]
    /\ UNCHANGED <<role, epoch, commitIdx, votedFor, votedEpoch,
                   leaseOk, commitHist, kvSt>>

ReplicateEntry(s, p, idx) ==
    /\ role[s] = "Secondary"
    /\ role[p] = "Primary"
    /\ s # p
    /\ epoch[s] = epoch[p]
    /\ idx \in 1..Len(nLog[p])
    /\ Len(nLog[s]) = idx - 1
    /\ nLog' = [nLog EXCEPT ![s] = Append(nLog[s], nLog[p][idx])]
    /\ UNCHANGED <<role, epoch, commitIdx, votedFor, votedEpoch,
                   leaseOk, commitHist, kvSt>>

AdvanceCommit(p) ==
    /\ role[p] = "Primary"
    /\ LET ni == commitIdx[p] + 1
       IN /\ ni <= Len(nLog[p])
          /\ nLog[p][ni].epoch = epoch[p]
          /\ IsQuorum({n \in Nodes : Len(nLog[n]) >= ni /\ epoch[n] = epoch[p]})
          /\ commitIdx'  = [commitIdx  EXCEPT ![p] = ni]
          /\ commitHist' = Append(commitHist, nLog[p][ni])
          /\ kvSt' = [kvSt EXCEPT ![p] = ApplyOne(kvSt[p], nLog[p][ni])]
    /\ UNCHANGED <<role, epoch, nLog, votedFor, votedEpoch, leaseOk>>

SecondaryCommit(s, p) ==
    /\ role[s] = "Secondary"
    /\ role[p] = "Primary"
    /\ s # p
    /\ epoch[s] = epoch[p]
    /\ LET ni == commitIdx[s] + 1
       IN /\ ni <= commitIdx[p]
          /\ ni <= Len(nLog[s])
          /\ commitIdx' = [commitIdx EXCEPT ![s] = ni]
          /\ kvSt' = [kvSt EXCEPT ![s] = ApplyOne(kvSt[s], nLog[s][ni])]
    /\ UNCHANGED <<role, epoch, nLog, votedFor, votedEpoch, leaseOk, commitHist>>

------------------------------------------------------------------------
(* Lease                                                                 *)
------------------------------------------------------------------------

RenewLease(p) ==
    /\ role[p] = "Primary"
    /\ IsQuorum({n \in Nodes : epoch[n] = epoch[p]
                             /\ (n = p \/ role[n] = "Secondary")})
    /\ leaseOk' = [leaseOk EXCEPT ![p] = TRUE]
    /\ UNCHANGED <<role, epoch, nLog, commitIdx, votedFor, votedEpoch,
                   commitHist, kvSt>>

(* Lease expiry: suppressed in bug mode — primary keeps stale coherence window. *)
LeaseExpire(p) ==
    /\ role[p] = "Primary"
    /\ leaseOk[p] = TRUE
    /\ ~INJECT_BUG
    /\ leaseOk' = [leaseOk EXCEPT ![p] = FALSE]
    /\ UNCHANGED <<role, epoch, nLog, commitIdx, votedFor, votedEpoch,
                   commitHist, kvSt>>

------------------------------------------------------------------------
(* Read path                                                             *)
------------------------------------------------------------------------

(* Normal: lease required. Bug mode: no check — stale read possible. *)
ClientGet(p, k) ==
    /\ role[p] = "Primary"
    /\ k \in Keys
    /\ IF INJECT_BUG THEN TRUE ELSE leaseOk[p] = TRUE
    /\ UNCHANGED vars

------------------------------------------------------------------------
(* Crash and recovery                                                    *)
------------------------------------------------------------------------

NodeCrash(n) ==
    /\ role'    = [role    EXCEPT ![n] = "Secondary"]
    /\ leaseOk' = [leaseOk EXCEPT ![n] = FALSE]
    /\ votedFor' = [votedFor EXCEPT ![n] = Nil]
    /\ UNCHANGED <<epoch, nLog, commitIdx, votedEpoch, commitHist, kvSt>>

------------------------------------------------------------------------
(* Next-state relation                                                   *)
------------------------------------------------------------------------

Next ==
    \/ \E n \in Nodes                            : BecomeCandidate(n)
    \/ \E voter, cand \in Nodes                  : GrantVote(voter, cand)
    \/ \E n \in Nodes                            : WinElection(n)
    \/ \E p \in Nodes, k \in Keys, v \in Values  : ClientPut(p, k, v)
    \/ \E p \in Nodes, k \in Keys                : ClientDelete(p, k)
    \/ \E s, p \in Nodes, idx \in 1..MaxLogLen   : ReplicateEntry(s, p, idx)
    \/ \E p \in Nodes                            : AdvanceCommit(p)
    \/ \E s, p \in Nodes                         : SecondaryCommit(s, p)
    \/ \E p \in Nodes                            : RenewLease(p)
    \/ \E p \in Nodes                            : LeaseExpire(p)
    \/ \E p \in Nodes, k \in Keys                : ClientGet(p, k)
    \/ \E n \in Nodes                            : NodeCrash(n)
    \/ \E n \in Nodes, e \in 1..MaxEpoch         : StepDown(n, e)

------------------------------------------------------------------------
(* Specification                                                         *)
------------------------------------------------------------------------

Fairness ==
    /\ WF_vars(\E p \in Nodes : AdvanceCommit(p))
    /\ WF_vars(\E s, p \in Nodes : SecondaryCommit(s, p))

Spec == Init /\ [][Next]_vars /\ Fairness

------------------------------------------------------------------------
(* Safety invariants                                                     *)
------------------------------------------------------------------------

AtMostOneLeaderPerEpoch ==
    \A n, m \in Nodes :
        (role[n] = "Primary" /\ role[m] = "Primary" /\ n # m)
        => epoch[n] # epoch[m]

CommittedEntriesNotLost ==
    \A n \in Nodes :
        role[n] = "Primary" =>
            \A i \in 1..commitIdx[n] : i <= Len(nLog[n])

(* A primary with a valid lease must see CommittedKV. In bug mode,
   LeaseExpire is suppressed: a demoted primary keeps leaseOk=TRUE after
   a new primary commits writes, so its stale kvSt diverges from CommittedKV. *)
LinearizabilityInvariant ==
    \A n \in Nodes :
        (role[n] = "Primary" /\ leaseOk[n]) =>
            \A k \in Keys : kvSt[n][k] = CommittedKV[k]

------------------------------------------------------------------------
(* Liveness (documentation only)                                        *)
------------------------------------------------------------------------

EventuallyLeader == <> (\E n \in Nodes : role[n] = "Primary")

========================================================================
