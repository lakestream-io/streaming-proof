--------------------------- MODULE GapDetection ----------------------------
(***************************************************************************)
(* Models: ConsumerCheckPoint.getMissedSeqs()                              *)
(*                                                                         *)
(* Detects gaps between sorted, trimmed ranges. The Java code uses         *)
(* EXCLUSIVE boundaries: gap.start = previous.end, gap.end = current.start *)
(* so actual missed sequences are previous.end+1 through current.start-1.  *)
(* The missed count formula confirms: end.seq - start.seq - 1              *)
(*                                                                         *)
(* What This Proves:                                                       *)
(*   - Completeness: every truly missing sequence is in exactly one gap    *)
(*   - NoFalseGaps: gap boundaries correctly identify missing sequences    *)
(*   - SortedOutput: gaps are sorted by start                              *)
(*   - Disjointness: gaps don't overlap with input ranges                  *)
(***************************************************************************)

EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS MaxSeq, MaxRanges

Range == [start : 0..MaxSeq, end : 0..MaxSeq]
ValidRange(r) == r.start <= r.end

Covered(r) == r.start .. r.end
AllCovered(rs) == UNION {Covered(rs[i]) : i \in 1..Len(rs)}

\* Import SortByStart and Trim from SeqRangeMerge conceptually;
\* for standalone checking, we inline a simplified version.
\* Input to GapDetection is already sorted and trimmed.

(***************************************************************************)
(* The GapDetection algorithm — mirrors Java getMissedSeqs() lines 347-371 *)
(* Input: sorted, trimmed ranges (no overlaps, no adjacency)               *)
(* Output: gap ranges with EXCLUSIVE boundaries matching Java semantics     *)
(***************************************************************************)
RECURSIVE DetectGaps(_)
DetectGaps(ranges) ==
    IF Len(ranges) <= 1 THEN << >>
    ELSE LET prev == ranges[1]
             curr == ranges[2]
             rest == SubSeq(ranges, 2, Len(ranges))
         IN IF curr.start > prev.end + 1 THEN
                \* Gap found — Java sets start=prev.end, end=curr.start (exclusive)
                <<[start |-> prev.end, end |-> curr.start]>> \o DetectGaps(rest)
            ELSE
                DetectGaps(rest)

\* The actual missed sequences for a gap with exclusive boundaries
MissedInGap(gap) == (gap.start + 1) .. (gap.end - 1)

(*--algorithm GapDetection
variables
    \* Input: sorted, non-overlapping, non-adjacent ranges
    inputRanges \in
        {rs \in UNION {[1..n -> {r \in Range : ValidRange(r)}] : n \in 1..MaxRanges} :
            \* Sorted, non-overlapping, non-adjacent (all in one quantifier)
            \A i \in 1..Len(rs)-1 :
                rs[i].start < rs[i+1].start
                /\ rs[i].end < rs[i+1].start
                /\ rs[i].end + 1 /= rs[i+1].start},
    gaps = << >>;

begin
    Detect:
        gaps := DetectGaps(inputRanges);
end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "e2eebff2" /\ chksum(tla) = "e8f2e6c0")
VARIABLES pc, inputRanges, gaps

vars == << pc, inputRanges, gaps >>

Init == (* Global variables *)
        /\ inputRanges \in {rs \in UNION {[1..n -> {r \in Range : ValidRange(r)}] : n \in 1..MaxRanges} :
                           
                               \A i \in 1..Len(rs)-1 :
                                   rs[i].start < rs[i+1].start
                                   /\ rs[i].end < rs[i+1].start
                                   /\ rs[i].end + 1 /= rs[i+1].start}
        /\ gaps = << >>
        /\ pc = "Detect"

Detect == /\ pc = "Detect"
          /\ gaps' = DetectGaps(inputRanges)
          /\ pc' = "Done"
          /\ UNCHANGED inputRanges

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == pc = "Done" /\ UNCHANGED vars

Next == Detect
           \/ Terminating

Spec == Init /\ [][Next]_vars

Termination == <>(pc = "Done")

\* END TRANSLATION 

(***************************************************************************)
(* Invariants                                                              *)
(***************************************************************************)

\* The set of all sequences between the first range start and last range end
\* that are NOT in any input range
TrulyMissed ==
    IF Len(inputRanges) <= 1 THEN {}
    ELSE LET minS == inputRanges[1].start
             maxE == inputRanges[Len(inputRanges)].end
         IN (minS..maxE) \ AllCovered(inputRanges)

\* All sequences identified as missed by the gap ranges
IdentifiedMissed ==
    UNION {MissedInGap(gaps[i]) : i \in 1..Len(gaps)}

\* INV1: Every truly missing sequence is identified
Completeness == pc = "Done" => TrulyMissed = IdentifiedMissed

\* INV2: No false gaps — every identified sequence is truly missing
NoFalseGaps == pc = "Done" => IdentifiedMissed \cap AllCovered(inputRanges) = {}

\* INV3: Gaps are sorted
SortedOutput ==
    pc = "Done" => \A i \in 1..Len(gaps)-1 : gaps[i].start < gaps[i+1].start

\* INV4: Gap boundaries don't overlap with input range coverage
Disjointness ==
    pc = "Done" =>
    \A i \in 1..Len(gaps) :
        MissedInGap(gaps[i]) \cap AllCovered(inputRanges) = {}

=============================================================================
