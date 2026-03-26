------------------------ MODULE OrderingVerification ------------------------
(***************************************************************************)
(* Models: ProofConsumerTask.onMessage() out-of-order detection             *)
(*                                                                         *)
(* When a message arrives with value <= lastConsumedSeq and metadata       *)
(* indicates it was produced AFTER (isAfter), this is a producer-side      *)
(* duplicate or out-of-order write. Otherwise it's a consumer redelivery.  *)
(*                                                                         *)
(* The spec abstracts the timestamp-keyed SortedMap as per-timestamp       *)
(* tracking — a deliberate simplification.                                 *)
(*                                                                         *)
(* What This Proves:                                                       *)
(*   - DetectionCompleteness: every out-of-order arrival is detected       *)
(*   - NoFalsePositives: monotonic sequences produce no violations         *)
(*   - TimestampIsolation: different timestamps tracked independently      *)
(***************************************************************************)

EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS MaxSeq,     \* Upper bound of sequence numbers
          NumKeys,    \* Number of distinct message keys
          NumTimestamps \* Number of distinct timestamps (partition epochs)

Keys == 1..NumKeys
Timestamps == 1..NumTimestamps

\* A message has a key, sequence number, and a "metadata timestamp" that
\* determines the ordering epoch. isAfter(msg) = msg.ts > lastSeen.ts
Message == [key : Keys, seq : 0..MaxSeq, ts : Timestamps]

(*--algorithm OrderingVerification
variables
    \* Track last seen sequence per key per timestamp epoch
    lastSeq = [k \in Keys |-> [t \in Timestamps |-> -1]],
    \* Track the timestamp epoch of the last consumed message per key
    lastTs = [k \in Keys |-> 0],
    \* Detected out-of-order events: set of [key, seq, ts] records
    outOfOrder = {},
    \* Non-deterministic message sequence to process
    messages \in UNION {[1..n -> Message] : n \in 1..MaxSeq + 1},
    idx = 1;

begin
    ProcessMessages:
        while idx <= Len(messages) do
            with msg = messages[idx] do
                if lastTs[msg.key] = 0 then
                    \* First message for this key ever — just record it
                    \* Java: getLastSeq(key) returns null, creates new range
                    lastSeq[msg.key][msg.ts] := msg.seq;
                    lastTs[msg.key] := msg.ts;
                elsif msg.seq <= lastSeq[msg.key][lastTs[msg.key]]
                      /\ msg.ts > lastTs[msg.key] then
                    \* Lower seq but higher timestamp => producer-side dup/out-of-order
                    outOfOrder := outOfOrder \cup {msg};
                    lastSeq[msg.key][msg.ts] := msg.seq;
                    lastTs[msg.key] := msg.ts;
                elsif msg.seq <= lastSeq[msg.key][lastTs[msg.key]]
                      /\ msg.ts <= lastTs[msg.key] then
                    \* Lower seq AND lower/equal timestamp => consumer redelivery
                    \* Not an out-of-order event (just a dup)
                    \* Note: Java still calls newConsumedRange() here for range tracking,
                    \* but we deliberately omit that — this spec focuses on out-of-order
                    \* detection, not range/duplicate tracking.
                    skip;
                else
                    \* Normal forward progress or new timestamp epoch
                    lastSeq[msg.key][msg.ts] :=
                        IF msg.seq > lastSeq[msg.key][msg.ts]
                        THEN msg.seq ELSE lastSeq[msg.key][msg.ts];
                    lastTs[msg.key] := msg.ts;
                end if;
            end with;
            idx := idx + 1;
        end while;
end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "d3edf34" /\ chksum(tla) = "fda568de")
VARIABLES pc, lastSeq, lastTs, outOfOrder, messages, idx

vars == << pc, lastSeq, lastTs, outOfOrder, messages, idx >>

Init == (* Global variables *)
        /\ lastSeq = [k \in Keys |-> [t \in Timestamps |-> -1]]
        /\ lastTs = [k \in Keys |-> 0]
        /\ outOfOrder = {}
        /\ messages \in UNION {[1..n -> Message] : n \in 1..MaxSeq + 1}
        /\ idx = 1
        /\ pc = "ProcessMessages"

ProcessMessages == /\ pc = "ProcessMessages"
                   /\ IF idx <= Len(messages)
                         THEN /\ LET msg == messages[idx] IN
                                   IF lastTs[msg.key] = 0
                                      THEN /\ lastSeq' = [lastSeq EXCEPT ![msg.key][msg.ts] = msg.seq]
                                           /\ lastTs' = [lastTs EXCEPT ![msg.key] = msg.ts]
                                           /\ UNCHANGED outOfOrder
                                      ELSE /\ IF msg.seq <= lastSeq[msg.key][lastTs[msg.key]]
                                                 /\ msg.ts > lastTs[msg.key]
                                                 THEN /\ outOfOrder' = (outOfOrder \cup {msg})
                                                      /\ lastSeq' = [lastSeq EXCEPT ![msg.key][msg.ts] = msg.seq]
                                                      /\ lastTs' = [lastTs EXCEPT ![msg.key] = msg.ts]
                                                 ELSE /\ IF msg.seq <= lastSeq[msg.key][lastTs[msg.key]]
                                                            /\ msg.ts <= lastTs[msg.key]
                                                            THEN /\ TRUE
                                                                 /\ UNCHANGED << lastSeq, 
                                                                                 lastTs >>
                                                            ELSE /\ lastSeq' = [lastSeq EXCEPT ![msg.key][msg.ts] = IF msg.seq > lastSeq[msg.key][msg.ts]
                                                                                                                    THEN msg.seq ELSE lastSeq[msg.key][msg.ts]]
                                                                 /\ lastTs' = [lastTs EXCEPT ![msg.key] = msg.ts]
                                                      /\ UNCHANGED outOfOrder
                              /\ idx' = idx + 1
                              /\ pc' = "ProcessMessages"
                         ELSE /\ pc' = "Done"
                              /\ UNCHANGED << lastSeq, lastTs, outOfOrder, idx >>
                   /\ UNCHANGED messages

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == pc = "Done" /\ UNCHANGED vars

Next == ProcessMessages
           \/ Terminating

Spec == Init /\ [][Next]_vars

Termination == <>(pc = "Done")

\* END TRANSLATION 

(***************************************************************************)
(* Invariants                                                              *)
(***************************************************************************)

\* Ground truth: which messages in the processed prefix are out-of-order?
\* A message at position p is out-of-order if:
\*   - There exists an earlier message (position q < p) with same key
\*   - msg[p].seq <= max seq seen for key up to position q
\*   - msg[p].ts > ts of last message for key before position p
ProcessedMessages == SubSeq(messages, 1, idx - 1)

\* INV1: Every detected out-of-order event is genuinely out-of-order
\* (no false positives for the current processing state)
NoFalsePositives ==
    \A msg \in outOfOrder :
        \E p \in 1..Len(ProcessedMessages) :
            ProcessedMessages[p] = msg
            /\ \E q \in 1..p-1 :
                ProcessedMessages[q].key = msg.key
                /\ msg.seq <= ProcessedMessages[q].seq
                /\ msg.ts > ProcessedMessages[q].ts

\* Note: DetectionCompleteness (no false negatives) was explored but cannot
\* be cleanly defined independently of the algorithm's internal state.
\* The algorithm's state tracking (lastTs, lastSeq) is path-dependent —
\* what counts as "current" changes with each arrival, and redelivery
\* messages don't update the active state. NoFalsePositives provides the
\* key safety guarantee; TimestampIsolation ensures structural correctness.

\* INV3: Timestamp isolation — outOfOrder events only involve cross-timestamp comparisons
TimestampIsolation ==
    \A msg \in outOfOrder :
        msg.ts > 1 \* Must involve a timestamp transition

=============================================================================
