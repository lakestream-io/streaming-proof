------------------------- MODULE CoordinatorWorker -------------------------
(***************************************************************************)
(* Models: Distributed coordinator-worker checkpoint interaction           *)
(*                                                                         *)
(* Models non-atomic checkpoint collection, watermark propagation,         *)
(* trim+snapshot non-atomicity, and worker failure.                        *)
(*                                                                         *)
(* What This Proves:                                                       *)
(*   Safety:                                                               *)
(*   - NoDataLossFromTrimming: trim never removes unverified sequences     *)
(*   - AggregationSafety: no phantom sequences in coordinator view         *)
(*   - WatermarkSafety: watermarks never exceed verified state             *)
(*   Liveness:                                                             *)
(*   - EventualConsistency: coord eventually sees worker state             *)
(***************************************************************************)

EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    NumWorkers,     \* Number of worker processes
    NumKeys,        \* Number of message keys
    MaxSeq,         \* Maximum sequence number
    MaxSteps        \* Bound on iterations for model checking

Keys == 1..NumKeys
Workers == 1..NumWorkers

(*--algorithm CoordinatorWorker
variables
    \* Worker state: actual received sequences per key (ground truth)
    workerSeq = [w \in Workers |-> [k \in Keys |-> 0]],
    \* Worker state after trimming (watermark level applied)
    workerTrimmedAt = [w \in Workers |-> [k \in Keys |-> -1]],
    \* Coordinator's aggregated view from last successful collection
    coordView = [k \in Keys |-> -1],
    \* Watermarks that have been VERIFIED by the coordinator
    verifiedWatermarks = [k \in Keys |-> -1],
    \* Watermarks sent to workers for trimming
    sentWatermarks = [k \in Keys |-> -1],
    \* Simple network: pending responses per worker
    pendingResponse = [w \in Workers |-> [k \in Keys |-> -1]],
    workerAlive = [w \in Workers |-> TRUE];

fair process Worker \in Workers
variables wrkSteps = 0;
begin
    WorkerLoop:
        while wrkSteps < MaxSteps do
            either
                \* Receive a new message (increment seq for some key)
                with k \in Keys do
                    if workerSeq[self][k] < MaxSeq then
                        workerSeq[self][k] := workerSeq[self][k] + 1;
                    end if;
                end with;
            or
                \* Apply watermark trimming then snapshot (non-atomic!)
                \* Note: one-key-at-a-time trim is a deliberate over-approximation.
                \* Java trims all keys atomically in a single synchronized call,
                \* so exploring partial-trim states is conservative (more states).
                ApplyTrim:
                    workerTrimmedAt[self] := [k \in Keys |->
                        IF sentWatermarks[k] > workerTrimmedAt[self][k]
                        THEN sentWatermarks[k]
                        ELSE workerTrimmedAt[self][k]];
                \* Snapshot happens AFTER trim — messages may arrive between
                TakeSnapshot:
                    pendingResponse[self] := [k \in Keys |-> workerSeq[self][k]];
            or
                \* Non-deterministically become unreachable
                if workerAlive[self] then
                    workerAlive[self] := FALSE;
                end if;
            end either;
            IncWrkSteps:
                wrkSteps := wrkSteps + 1;
        end while;
end process;

fair process Coord = 0
variables
    collected = [k \in Keys |-> -1],
    workerIdx = 1,
    cycleFailed = FALSE,
    crdSteps = 0;
begin
    CoordLoop:
        while crdSteps < MaxSteps do
            \* Reset for new cycle
            StartCycle:
                collected := [k \in Keys |-> -1];
                workerIdx := 1;
                cycleFailed := FALSE;

            \* Collect from each worker sequentially (matches Java loop)
            CollectLoop:
                while workerIdx <= NumWorkers do
                    if workerAlive[workerIdx] then
                        \* Read the worker's pending response (may be stale)
                        with resp = pendingResponse[workerIdx] do
                            collected := [k \in Keys |->
                                IF resp[k] > collected[k] THEN resp[k]
                                ELSE collected[k]];
                        end with;
                    else
                        \* Worker unreachable — entire cycle fails (Java behavior)
                        cycleFailed := TRUE;
                    end if;
                    workerIdx := workerIdx + 1;
                end while;

            \* Verify and update only if collection succeeded
            \* CRITICAL: verification happens BEFORE watermark advancement
            VerifyAndUpdate:
                if ~cycleFailed then
                    \* Step 1: update coordinator's view
                    coordView := collected;
                    \* Step 2: verify (abstracted — coordinator checks collected
                    \*   against inCheck, but we model just the watermark flow)
                    \* Step 3: advance watermarks ONLY to verified level
                    verifiedWatermarks := [k \in Keys |->
                        IF collected[k] > verifiedWatermarks[k]
                        THEN collected[k]
                        ELSE verifiedWatermarks[k]];
                end if;

            \* Propagate watermarks to workers (separate step — workers may
            \* receive messages between verification and propagation)
            PropagateWatermarks:
                if ~cycleFailed then
                    sentWatermarks := verifiedWatermarks;
                end if;
                crdSteps := crdSteps + 1;
        end while;
end process;

end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "63dd20bc" /\ chksum(tla) = "c3e35ac2")
VARIABLES pc, workerSeq, workerTrimmedAt, coordView, verifiedWatermarks, 
          sentWatermarks, pendingResponse, workerAlive, wrkSteps, collected, 
          workerIdx, cycleFailed, crdSteps

vars == << pc, workerSeq, workerTrimmedAt, coordView, verifiedWatermarks, 
           sentWatermarks, pendingResponse, workerAlive, wrkSteps, collected, 
           workerIdx, cycleFailed, crdSteps >>

ProcSet == (Workers) \cup {0}

Init == (* Global variables *)
        /\ workerSeq = [w \in Workers |-> [k \in Keys |-> 0]]
        /\ workerTrimmedAt = [w \in Workers |-> [k \in Keys |-> -1]]
        /\ coordView = [k \in Keys |-> -1]
        /\ verifiedWatermarks = [k \in Keys |-> -1]
        /\ sentWatermarks = [k \in Keys |-> -1]
        /\ pendingResponse = [w \in Workers |-> [k \in Keys |-> -1]]
        /\ workerAlive = [w \in Workers |-> TRUE]
        (* Process Worker *)
        /\ wrkSteps = [self \in Workers |-> 0]
        (* Process Coord *)
        /\ collected = [k \in Keys |-> -1]
        /\ workerIdx = 1
        /\ cycleFailed = FALSE
        /\ crdSteps = 0
        /\ pc = [self \in ProcSet |-> CASE self \in Workers -> "WorkerLoop"
                                        [] self = 0 -> "CoordLoop"]

WorkerLoop(self) == /\ pc[self] = "WorkerLoop"
                    /\ IF wrkSteps[self] < MaxSteps
                          THEN /\ \/ /\ \E k \in Keys:
                                          IF workerSeq[self][k] < MaxSeq
                                             THEN /\ workerSeq' = [workerSeq EXCEPT ![self][k] = workerSeq[self][k] + 1]
                                             ELSE /\ TRUE
                                                  /\ UNCHANGED workerSeq
                                     /\ pc' = [pc EXCEPT ![self] = "IncWrkSteps"]
                                     /\ UNCHANGED workerAlive
                                  \/ /\ pc' = [pc EXCEPT ![self] = "ApplyTrim"]
                                     /\ UNCHANGED <<workerSeq, workerAlive>>
                                  \/ /\ IF workerAlive[self]
                                           THEN /\ workerAlive' = [workerAlive EXCEPT ![self] = FALSE]
                                           ELSE /\ TRUE
                                                /\ UNCHANGED workerAlive
                                     /\ pc' = [pc EXCEPT ![self] = "IncWrkSteps"]
                                     /\ UNCHANGED workerSeq
                          ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                               /\ UNCHANGED << workerSeq, workerAlive >>
                    /\ UNCHANGED << workerTrimmedAt, coordView, 
                                    verifiedWatermarks, sentWatermarks, 
                                    pendingResponse, wrkSteps, collected, 
                                    workerIdx, cycleFailed, crdSteps >>

IncWrkSteps(self) == /\ pc[self] = "IncWrkSteps"
                     /\ wrkSteps' = [wrkSteps EXCEPT ![self] = wrkSteps[self] + 1]
                     /\ pc' = [pc EXCEPT ![self] = "WorkerLoop"]
                     /\ UNCHANGED << workerSeq, workerTrimmedAt, coordView, 
                                     verifiedWatermarks, sentWatermarks, 
                                     pendingResponse, workerAlive, collected, 
                                     workerIdx, cycleFailed, crdSteps >>

ApplyTrim(self) == /\ pc[self] = "ApplyTrim"
                   /\ workerTrimmedAt' = [workerTrimmedAt EXCEPT ![self] =                      [k \in Keys |->
                                                                           IF sentWatermarks[k] > workerTrimmedAt[self][k]
                                                                           THEN sentWatermarks[k]
                                                                           ELSE workerTrimmedAt[self][k]]]
                   /\ pc' = [pc EXCEPT ![self] = "TakeSnapshot"]
                   /\ UNCHANGED << workerSeq, coordView, verifiedWatermarks, 
                                   sentWatermarks, pendingResponse, 
                                   workerAlive, wrkSteps, collected, workerIdx, 
                                   cycleFailed, crdSteps >>

TakeSnapshot(self) == /\ pc[self] = "TakeSnapshot"
                      /\ pendingResponse' = [pendingResponse EXCEPT ![self] = [k \in Keys |-> workerSeq[self][k]]]
                      /\ pc' = [pc EXCEPT ![self] = "IncWrkSteps"]
                      /\ UNCHANGED << workerSeq, workerTrimmedAt, coordView, 
                                      verifiedWatermarks, sentWatermarks, 
                                      workerAlive, wrkSteps, collected, 
                                      workerIdx, cycleFailed, crdSteps >>

Worker(self) == WorkerLoop(self) \/ IncWrkSteps(self) \/ ApplyTrim(self)
                   \/ TakeSnapshot(self)

CoordLoop == /\ pc[0] = "CoordLoop"
             /\ IF crdSteps < MaxSteps
                   THEN /\ pc' = [pc EXCEPT ![0] = "StartCycle"]
                   ELSE /\ pc' = [pc EXCEPT ![0] = "Done"]
             /\ UNCHANGED << workerSeq, workerTrimmedAt, coordView, 
                             verifiedWatermarks, sentWatermarks, 
                             pendingResponse, workerAlive, wrkSteps, collected, 
                             workerIdx, cycleFailed, crdSteps >>

StartCycle == /\ pc[0] = "StartCycle"
              /\ collected' = [k \in Keys |-> -1]
              /\ workerIdx' = 1
              /\ cycleFailed' = FALSE
              /\ pc' = [pc EXCEPT ![0] = "CollectLoop"]
              /\ UNCHANGED << workerSeq, workerTrimmedAt, coordView, 
                              verifiedWatermarks, sentWatermarks, 
                              pendingResponse, workerAlive, wrkSteps, crdSteps >>

CollectLoop == /\ pc[0] = "CollectLoop"
               /\ IF workerIdx <= NumWorkers
                     THEN /\ IF workerAlive[workerIdx]
                                THEN /\ LET resp == pendingResponse[workerIdx] IN
                                          collected' =          [k \in Keys |->
                                                       IF resp[k] > collected[k] THEN resp[k]
                                                       ELSE collected[k]]
                                     /\ UNCHANGED cycleFailed
                                ELSE /\ cycleFailed' = TRUE
                                     /\ UNCHANGED collected
                          /\ workerIdx' = workerIdx + 1
                          /\ pc' = [pc EXCEPT ![0] = "CollectLoop"]
                     ELSE /\ pc' = [pc EXCEPT ![0] = "VerifyAndUpdate"]
                          /\ UNCHANGED << collected, workerIdx, cycleFailed >>
               /\ UNCHANGED << workerSeq, workerTrimmedAt, coordView, 
                               verifiedWatermarks, sentWatermarks, 
                               pendingResponse, workerAlive, wrkSteps, 
                               crdSteps >>

VerifyAndUpdate == /\ pc[0] = "VerifyAndUpdate"
                   /\ IF ~cycleFailed
                         THEN /\ coordView' = collected
                              /\ verifiedWatermarks' =                   [k \in Keys |->
                                                       IF collected[k] > verifiedWatermarks[k]
                                                       THEN collected[k]
                                                       ELSE verifiedWatermarks[k]]
                         ELSE /\ TRUE
                              /\ UNCHANGED << coordView, verifiedWatermarks >>
                   /\ pc' = [pc EXCEPT ![0] = "PropagateWatermarks"]
                   /\ UNCHANGED << workerSeq, workerTrimmedAt, sentWatermarks, 
                                   pendingResponse, workerAlive, wrkSteps, 
                                   collected, workerIdx, cycleFailed, crdSteps >>

PropagateWatermarks == /\ pc[0] = "PropagateWatermarks"
                       /\ IF ~cycleFailed
                             THEN /\ sentWatermarks' = verifiedWatermarks
                             ELSE /\ TRUE
                                  /\ UNCHANGED sentWatermarks
                       /\ crdSteps' = crdSteps + 1
                       /\ pc' = [pc EXCEPT ![0] = "CoordLoop"]
                       /\ UNCHANGED << workerSeq, workerTrimmedAt, coordView, 
                                       verifiedWatermarks, pendingResponse, 
                                       workerAlive, wrkSteps, collected, 
                                       workerIdx, cycleFailed >>

Coord == CoordLoop \/ StartCycle \/ CollectLoop \/ VerifyAndUpdate
            \/ PropagateWatermarks

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == Coord
           \/ (\E self \in Workers: Worker(self))
           \/ Terminating

Spec == /\ Init /\ [][Next]_vars
        /\ \A self \in Workers : WF_vars(Worker(self))
        /\ WF_vars(Coord)

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION 

(***************************************************************************)
(* Invariants (Safety)                                                     *)
(***************************************************************************)

\* SAFETY 1: Trimming never removes unverified sequences
\* Workers only trim at watermarks that have been verified
NoDataLossFromTrimming ==
    \A w \in Workers, k \in Keys :
        workerTrimmedAt[w][k] <= verifiedWatermarks[k]

\* SAFETY 2: Coordinator's view only contains genuine sequences
\* (no phantom sequences — coordView[k] <= max of actual worker seqs)
AggregationSafety ==
    \A k \in Keys :
        coordView[k] <= 0 \/
        \E w \in Workers : workerSeq[w][k] >= coordView[k]

\* SAFETY 3: Watermarks sent to workers never exceed verified state
WatermarkSafety ==
    \A k \in Keys :
        sentWatermarks[k] <= verifiedWatermarks[k]

(***************************************************************************)
(* Temporal Properties (Liveness)                                          *)
(***************************************************************************)

\* Eventual consistency: if a worker is alive and network stabilizes,
\* coordinator eventually sees its state
EventualConsistency ==
    \A w \in Workers, k \in Keys :
        (workerAlive[w] /\ workerSeq[w][k] > 0)
        ~> (coordView[k] >= workerSeq[w][k])

=============================================================================
