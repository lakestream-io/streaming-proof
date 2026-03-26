---------------------- MODULE CheckpointVerification -----------------------
(***************************************************************************)
(* Models: ProofTask.scheduleCheckpoint() verification state machine       *)
(*                                                                         *)
(* Models the coordinator's verification cycle. Producers publish,         *)
(* consumers receive, coordinator periodically checks if consumers have    *)
(* caught up. Algorithms (trim, watermark) are abstracted as correct.      *)
(*                                                                         *)
(* Mode is a CONSTANT — run separate model configs for standard/shared.    *)
(*                                                                         *)
(* What This Proves:                                                       *)
(*   Safety:                                                               *)
(*   - NoFalsePositive: lastVerified only advances on genuine catch-up     *)
(*   - LastVerifiedNonDecreasing: lastVerified never decreases             *)
(*   - WatermarkNonDecreasing: watermarks never decrease                   *)
(*   Liveness:                                                             *)
(*   - Progress: if producers stop, verification eventually succeeds       *)
(***************************************************************************)

EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    NumConsumers,   \* Number of consumer processes
    NumKeys,        \* Number of message keys
    MaxSeq,         \* Maximum sequence number
    TimeoutTicks,   \* Ticks before timeout declaration
    MaxSteps,       \* Bound on consumer/coordinator iterations for model checking
    Mode            \* "standard" or "shared" — run separate configs

ASSUME Mode \in {"standard", "shared"}

Keys == 1..NumKeys
Consumers == 1..NumConsumers

(*--algorithm CheckpointVerification
variables
    \* Single producer (multiple producers don't interact at protocol level)
    producerSeq = [k \in Keys |-> 0],
    \* Consumer state: latest received seq per key (abstraction of ranges)
    consumerSeq = [c \in Consumers |-> [k \in Keys |-> -1]],
    \* Coordinator state — inCheck starts unset (mirrors Java empty published map)
    inCheckSet = FALSE,
    inCheck = [k \in Keys |-> 0],
    lastVerified = [k \in Keys |-> -1],
    highWatermarks = [k \in Keys |-> -1],
    ticksSinceCheck = 0,
    timeouts = 0,
    failed = FALSE,
    \* Control: producer can stop publishing
    producerDone = FALSE;

fair process Producer = NumConsumers + 1
begin
    Publish:
        while ~producerDone do
            either
                \* Publish next message for a non-deterministic key
                with k \in Keys do
                    if producerSeq[k] < MaxSeq then
                        producerSeq[k] := producerSeq[k] + 1;
                    end if;
                end with;
            or
                \* Non-deterministically decide to stop
                producerDone := TRUE;
            end either;
        end while;
end process;

fair process Consumer \in Consumers
variables consSteps = 0;
begin
    Consume:
        while consSteps < MaxSteps do
            \* Non-deterministically receive a published message
            with k \in Keys do
                if consumerSeq[self][k] < producerSeq[k] then
                    \* Receive next sequence (abstraction: always in order)
                    consumerSeq[self][k] := consumerSeq[self][k] + 1;
                end if;
            end with;
            consSteps := consSteps + 1;
        end while;
end process;

fair process Coordinator = 0
variables
    snapshotProd = [k \in Keys |-> 0],
    snapshotCons = [k \in Keys |-> -1],
    coordSteps = 0;
begin
    VerifyLoop:
        while coordSteps < MaxSteps do
            \* Step 1: Snapshot producer state
            SnapshotProd:
                snapshotProd := producerSeq;

            \* Step 2: Snapshot consumer state (aggregate: max across consumers)
            SnapshotCons:
                snapshotCons := [k \in Keys |->
                    LET maxC == CHOOSE c \in Consumers :
                        \A d \in Consumers : consumerSeq[c][k] >= consumerSeq[d][k]
                    IN consumerSeq[maxC][k]];

            \* Step 3: Set inCheck on first cycle
            \* Java: empty published map means nothing to verify — trivially passes.
            \* We model this by deferring inCheck until first snapshot.
            SetInCheck:
                if ~inCheckSet then
                    inCheck := snapshotProd;
                    inCheckSet := TRUE;
                end if;

            \* Step 4a: Shared mode — advance watermarks unconditionally
            UpdateWatermarks:
                if Mode = "shared" then
                    highWatermarks := [k \in Keys |->
                        IF snapshotCons[k] > highWatermarks[k]
                        THEN snapshotCons[k] ELSE highWatermarks[k]];
                end if;

            \* Step 4b: Check fulfillment
            CheckFulfilled:
                if \A k \in Keys : snapshotCons[k] >= inCheck[k] then
                    lastVerified := inCheck;
                    if Mode = "standard" then
                        highWatermarks := snapshotCons;
                    end if;
                    inCheck := snapshotProd;
                    ticksSinceCheck := 0;
                    failed := FALSE;
                else
                    ticksSinceCheck := ticksSinceCheck + 1;
                end if;

            \* Step 4c: Timeout check (separate label to avoid nested if issue)
            CheckTimeout:
                if ticksSinceCheck > TimeoutTicks then
                    failed := TRUE;
                    timeouts := timeouts + 1;
                    ticksSinceCheck := 0;
                end if;
                coordSteps := coordSteps + 1;
        end while;
end process;

end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "254149e5" /\ chksum(tla) = "a102fcdc")
VARIABLES pc, producerSeq, consumerSeq, inCheckSet, inCheck, lastVerified, 
          highWatermarks, ticksSinceCheck, timeouts, failed, producerDone, 
          consSteps, snapshotProd, snapshotCons, coordSteps

vars == << pc, producerSeq, consumerSeq, inCheckSet, inCheck, lastVerified, 
           highWatermarks, ticksSinceCheck, timeouts, failed, producerDone, 
           consSteps, snapshotProd, snapshotCons, coordSteps >>

ProcSet == {NumConsumers + 1} \cup (Consumers) \cup {0}

Init == (* Global variables *)
        /\ producerSeq = [k \in Keys |-> 0]
        /\ consumerSeq = [c \in Consumers |-> [k \in Keys |-> -1]]
        /\ inCheckSet = FALSE
        /\ inCheck = [k \in Keys |-> 0]
        /\ lastVerified = [k \in Keys |-> -1]
        /\ highWatermarks = [k \in Keys |-> -1]
        /\ ticksSinceCheck = 0
        /\ timeouts = 0
        /\ failed = FALSE
        /\ producerDone = FALSE
        (* Process Consumer *)
        /\ consSteps = [self \in Consumers |-> 0]
        (* Process Coordinator *)
        /\ snapshotProd = [k \in Keys |-> 0]
        /\ snapshotCons = [k \in Keys |-> -1]
        /\ coordSteps = 0
        /\ pc = [self \in ProcSet |-> CASE self = NumConsumers + 1 -> "Publish"
                                        [] self \in Consumers -> "Consume"
                                        [] self = 0 -> "VerifyLoop"]

Publish == /\ pc[NumConsumers + 1] = "Publish"
           /\ IF ~producerDone
                 THEN /\ \/ /\ \E k \in Keys:
                                 IF producerSeq[k] < MaxSeq
                                    THEN /\ producerSeq' = [producerSeq EXCEPT ![k] = producerSeq[k] + 1]
                                    ELSE /\ TRUE
                                         /\ UNCHANGED producerSeq
                            /\ UNCHANGED producerDone
                         \/ /\ producerDone' = TRUE
                            /\ UNCHANGED producerSeq
                      /\ pc' = [pc EXCEPT ![NumConsumers + 1] = "Publish"]
                 ELSE /\ pc' = [pc EXCEPT ![NumConsumers + 1] = "Done"]
                      /\ UNCHANGED << producerSeq, producerDone >>
           /\ UNCHANGED << consumerSeq, inCheckSet, inCheck, lastVerified, 
                           highWatermarks, ticksSinceCheck, timeouts, failed, 
                           consSteps, snapshotProd, snapshotCons, coordSteps >>

Producer == Publish

Consume(self) == /\ pc[self] = "Consume"
                 /\ IF consSteps[self] < MaxSteps
                       THEN /\ \E k \in Keys:
                                 IF consumerSeq[self][k] < producerSeq[k]
                                    THEN /\ consumerSeq' = [consumerSeq EXCEPT ![self][k] = consumerSeq[self][k] + 1]
                                    ELSE /\ TRUE
                                         /\ UNCHANGED consumerSeq
                            /\ consSteps' = [consSteps EXCEPT ![self] = consSteps[self] + 1]
                            /\ pc' = [pc EXCEPT ![self] = "Consume"]
                       ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                            /\ UNCHANGED << consumerSeq, consSteps >>
                 /\ UNCHANGED << producerSeq, inCheckSet, inCheck, 
                                 lastVerified, highWatermarks, ticksSinceCheck, 
                                 timeouts, failed, producerDone, snapshotProd, 
                                 snapshotCons, coordSteps >>

Consumer(self) == Consume(self)

VerifyLoop == /\ pc[0] = "VerifyLoop"
              /\ IF coordSteps < MaxSteps
                    THEN /\ pc' = [pc EXCEPT ![0] = "SnapshotProd"]
                    ELSE /\ pc' = [pc EXCEPT ![0] = "Done"]
              /\ UNCHANGED << producerSeq, consumerSeq, inCheckSet, inCheck, 
                              lastVerified, highWatermarks, ticksSinceCheck, 
                              timeouts, failed, producerDone, consSteps, 
                              snapshotProd, snapshotCons, coordSteps >>

SnapshotProd == /\ pc[0] = "SnapshotProd"
                /\ snapshotProd' = producerSeq
                /\ pc' = [pc EXCEPT ![0] = "SnapshotCons"]
                /\ UNCHANGED << producerSeq, consumerSeq, inCheckSet, inCheck, 
                                lastVerified, highWatermarks, ticksSinceCheck, 
                                timeouts, failed, producerDone, consSteps, 
                                snapshotCons, coordSteps >>

SnapshotCons == /\ pc[0] = "SnapshotCons"
                /\ snapshotCons' =             [k \in Keys |->
                                   LET maxC == CHOOSE c \in Consumers :
                                       \A d \in Consumers : consumerSeq[c][k] >= consumerSeq[d][k]
                                   IN consumerSeq[maxC][k]]
                /\ pc' = [pc EXCEPT ![0] = "SetInCheck"]
                /\ UNCHANGED << producerSeq, consumerSeq, inCheckSet, inCheck, 
                                lastVerified, highWatermarks, ticksSinceCheck, 
                                timeouts, failed, producerDone, consSteps, 
                                snapshotProd, coordSteps >>

SetInCheck == /\ pc[0] = "SetInCheck"
              /\ IF ~inCheckSet
                    THEN /\ inCheck' = snapshotProd
                         /\ inCheckSet' = TRUE
                    ELSE /\ TRUE
                         /\ UNCHANGED << inCheckSet, inCheck >>
              /\ pc' = [pc EXCEPT ![0] = "UpdateWatermarks"]
              /\ UNCHANGED << producerSeq, consumerSeq, lastVerified, 
                              highWatermarks, ticksSinceCheck, timeouts, 
                              failed, producerDone, consSteps, snapshotProd, 
                              snapshotCons, coordSteps >>

UpdateWatermarks == /\ pc[0] = "UpdateWatermarks"
                    /\ IF Mode = "shared"
                          THEN /\ highWatermarks' =               [k \in Keys |->
                                                    IF snapshotCons[k] > highWatermarks[k]
                                                    THEN snapshotCons[k] ELSE highWatermarks[k]]
                          ELSE /\ TRUE
                               /\ UNCHANGED highWatermarks
                    /\ pc' = [pc EXCEPT ![0] = "CheckFulfilled"]
                    /\ UNCHANGED << producerSeq, consumerSeq, inCheckSet, 
                                    inCheck, lastVerified, ticksSinceCheck, 
                                    timeouts, failed, producerDone, consSteps, 
                                    snapshotProd, snapshotCons, coordSteps >>

CheckFulfilled == /\ pc[0] = "CheckFulfilled"
                  /\ IF \A k \in Keys : snapshotCons[k] >= inCheck[k]
                        THEN /\ lastVerified' = inCheck
                             /\ IF Mode = "standard"
                                   THEN /\ highWatermarks' = snapshotCons
                                   ELSE /\ TRUE
                                        /\ UNCHANGED highWatermarks
                             /\ inCheck' = snapshotProd
                             /\ ticksSinceCheck' = 0
                             /\ failed' = FALSE
                        ELSE /\ ticksSinceCheck' = ticksSinceCheck + 1
                             /\ UNCHANGED << inCheck, lastVerified, 
                                             highWatermarks, failed >>
                  /\ pc' = [pc EXCEPT ![0] = "CheckTimeout"]
                  /\ UNCHANGED << producerSeq, consumerSeq, inCheckSet, 
                                  timeouts, producerDone, consSteps, 
                                  snapshotProd, snapshotCons, coordSteps >>

CheckTimeout == /\ pc[0] = "CheckTimeout"
                /\ IF ticksSinceCheck > TimeoutTicks
                      THEN /\ failed' = TRUE
                           /\ timeouts' = timeouts + 1
                           /\ ticksSinceCheck' = 0
                      ELSE /\ TRUE
                           /\ UNCHANGED << ticksSinceCheck, timeouts, failed >>
                /\ coordSteps' = coordSteps + 1
                /\ pc' = [pc EXCEPT ![0] = "VerifyLoop"]
                /\ UNCHANGED << producerSeq, consumerSeq, inCheckSet, inCheck, 
                                lastVerified, highWatermarks, producerDone, 
                                consSteps, snapshotProd, snapshotCons >>

Coordinator == VerifyLoop \/ SnapshotProd \/ SnapshotCons \/ SetInCheck
                  \/ UpdateWatermarks \/ CheckFulfilled \/ CheckTimeout

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == Producer \/ Coordinator
           \/ (\E self \in Consumers: Consumer(self))
           \/ Terminating

Spec == /\ Init /\ [][Next]_vars
        /\ WF_vars(Producer)
        /\ \A self \in Consumers : WF_vars(Consumer(self))
        /\ WF_vars(Coordinator)

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION 

(***************************************************************************)
(* Invariants (Safety)                                                     *)
(***************************************************************************)

\* SAFETY 1: lastVerified only advances when consumers have genuinely caught up
NoFalsePositive ==
    \A k \in Keys :
        lastVerified[k] > -1 =>
            \E c \in Consumers : consumerSeq[c][k] >= lastVerified[k]

(***************************************************************************)
(* Temporal Properties                                                     *)
(***************************************************************************)

\* lastVerified never decreases across state transitions
LastVerifiedNonDecreasing ==
    [][\A k \in Keys : lastVerified'[k] >= lastVerified[k]]_lastVerified

\* Watermarks never decrease across state transitions
WatermarkNonDecreasing ==
    [][\A k \in Keys : highWatermarks'[k] >= highWatermarks[k]]_highWatermarks

\* Progress: if producer stops, eventually some verification succeeds
\* (requires fair processes — all processes declared with 'fair')
Progress ==
    producerDone ~> (\E k \in Keys : lastVerified[k] >= 0)

=============================================================================
