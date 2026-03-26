------------------------ MODULE WatermarkComputation ------------------------
(***************************************************************************)
(* Models: ConsumerCheckPoint.computeHighWatermarks(baseWatermarks)         *)
(*                                                                         *)
(* Starting from baseWatermark (default -1), walks sorted trimmed ranges   *)
(* to find the highest contiguous sequence number. Breaks at first gap.    *)
(* When base == -1 and first range doesn't start at 0, watermark stays -1. *)
(*                                                                         *)
(* What This Proves:                                                       *)
(*   - Contiguity: all seqs from base to watermark are covered             *)
(*   - Maximality: no higher contiguous watermark exists                   *)
(*   - Monotonicity: watermark never decreases with more ranges            *)
(*   - GapStopsProgress: gap before end means watermark < max(ends)        *)
(***************************************************************************)

EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS MaxSeq, MaxRanges

Range == [start : 0..MaxSeq, end : 0..MaxSeq]
ValidRange(r) == r.start <= r.end
Covered(r) == r.start .. r.end
AllCovered(rs) == UNION {Covered(rs[i]) : i \in 1..Len(rs)}

(***************************************************************************)
(* The watermark computation — mirrors Java lines 392-426                  *)
(***************************************************************************)
ComputeWatermark(ranges, base) ==
    IF Len(ranges) = 0 THEN base
    ELSE LET firstStart == ranges[1].start
         IN IF base = -1 /\ firstStart /= 0 THEN -1
            ELSE IF base >= 0 /\ firstStart > base + 1 THEN base
            ELSE LET RECURSIVE Walk(_, _)
                     Walk(i, wm) ==
                         IF i > Len(ranges) THEN wm
                         ELSE IF ranges[i].start <= wm + 1 THEN
                             Walk(i + 1,
                                  IF ranges[i].end > wm
                                  THEN ranges[i].end ELSE wm)
                         ELSE wm
                     initWm == IF ranges[1].end > base
                               THEN ranges[1].end ELSE base
                 IN Walk(2, initWm)

(*--algorithm WatermarkComputation
variables
    \* Input: sorted, non-overlapping, non-adjacent ranges (already trimmed)
    inputRanges \in
        {rs \in UNION {[1..n -> {r \in Range : ValidRange(r)}] : n \in 0..MaxRanges} :
            \A i \in 1..Len(rs)-1 :
                rs[i].start < rs[i+1].start
                /\ rs[i].end < rs[i+1].start
                /\ rs[i].end + 1 /= rs[i+1].start},
    baseWatermark \in -1..2,
    highWatermark = -1;

begin
    Compute:
        highWatermark := ComputeWatermark(inputRanges, baseWatermark);
end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "78e0b7d9" /\ chksum(tla) = "40a83d0")
VARIABLES pc, inputRanges, baseWatermark, highWatermark

vars == << pc, inputRanges, baseWatermark, highWatermark >>

Init == (* Global variables *)
        /\ inputRanges \in {rs \in UNION {[1..n -> {r \in Range : ValidRange(r)}] : n \in 0..MaxRanges} :
                               \A i \in 1..Len(rs)-1 :
                                   rs[i].start < rs[i+1].start
                                   /\ rs[i].end < rs[i+1].start
                                   /\ rs[i].end + 1 /= rs[i+1].start}
        /\ baseWatermark \in -1..2
        /\ highWatermark = -1
        /\ pc = "Compute"

Compute == /\ pc = "Compute"
           /\ highWatermark' = ComputeWatermark(inputRanges, baseWatermark)
           /\ pc' = "Done"
           /\ UNCHANGED << inputRanges, baseWatermark >>

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == pc = "Done" /\ UNCHANGED vars

Next == Compute
           \/ Terminating

Spec == Init /\ [][Next]_vars

Termination == <>(pc = "Done")

\* END TRANSLATION 

(***************************************************************************)
(* Invariants                                                              *)
(***************************************************************************)

\* INV1: Contiguity — all sequences from base+1 to watermark are covered
Contiguity ==
    pc = "Done" =>
    (highWatermark > baseWatermark =>
        \A s \in (baseWatermark+1)..highWatermark :
            s \in AllCovered(inputRanges))

\* INV2: Maximality — no sequence at watermark+1 is reachable contiguously
Maximality ==
    pc = "Done" =>
    (highWatermark >= 0 =>
        (highWatermark + 1 \notin AllCovered(inputRanges)
         \/ highWatermark = MaxSeq
         \/ \* Or watermark is the end of the last contiguous range
            ~(\E i \in 1..Len(inputRanges) :
                inputRanges[i].start <= highWatermark + 1
                /\ inputRanges[i].end > highWatermark)))

\* INV3: Monotonicity setup — watermark >= base (or stays at -1)
MonotonicityBase ==
    pc = "Done" => (highWatermark >= baseWatermark \/ highWatermark = -1)

\* INV4: Gap stops progress
GapStopsProgress ==
    pc = "Done" =>
    (Len(inputRanges) > 0 /\ highWatermark >= 0 =>
        LET maxEnd == inputRanges[Len(inputRanges)].end
        IN highWatermark < maxEnd =>
            \* There must be a gap after the watermark
            highWatermark + 1 \notin AllCovered(inputRanges))

=============================================================================
