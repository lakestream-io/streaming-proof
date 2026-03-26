--------------------------- MODULE SeqRangeMerge ---------------------------
(***************************************************************************)
(* Models: ConsumerCheckPoint.trim(List<SeqRange>)                         *)
(*                                                                         *)
(* The trim() method merges overlapping and adjacent sequence ranges into   *)
(* a minimal set of non-overlapping, non-adjacent ranges. It is purely     *)
(* structural — it does NOT track duplicate counts.                        *)
(*                                                                         *)
(* What This Proves:                                                       *)
(*   - NoOverlaps: output ranges never overlap                             *)
(*   - NoAdjacent: no two output ranges are adjacent (gap >= 2)            *)
(*   - CoveragePreservation: every input sequence appears in output        *)
(*   - SortedOutput: output ranges are sorted by start                     *)
(*   - Idempotence: trimming an already-trimmed set is a no-op             *)
(***************************************************************************)

EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS MaxSeq,   \* Upper bound of sequence space (e.g., 5)
          MaxRanges \* Maximum number of input ranges (e.g., 4)

(***************************************************************************)
(* A Range is a record [start |-> Int, end |-> Int] where start <= end.    *)
(* We omit 'duplicated' because trim() does not use it.                    *)
(***************************************************************************)
Range == [start : 0..MaxSeq, end : 0..MaxSeq]
ValidRange(r) == r.start <= r.end

(***************************************************************************)
(* Helper: set of all integers covered by a range                          *)
(***************************************************************************)
Covered(r) == r.start .. r.end

(***************************************************************************)
(* Helper: set of all integers covered by a sequence of ranges             *)
(***************************************************************************)
AllCovered(rs) == UNION {Covered(rs[i]) : i \in 1..Len(rs)}

(***************************************************************************)
(* Sort a sequence of ranges by start field (selection sort for TLA+)      *)
(***************************************************************************)
RECURSIVE SortByStart(_)
SortByStart(rs) ==
    IF rs = << >> THEN << >>
    ELSE LET minIdx == CHOOSE i \in 1..Len(rs) :
                \A j \in 1..Len(rs) : rs[i].start <= rs[j].start
             minElem == rs[minIdx]
             rest == SubSeq(rs, 1, minIdx-1) \o SubSeq(rs, minIdx+1, Len(rs))
         IN <<minElem>> \o SortByStart(rest)

(***************************************************************************)
(* The Trim algorithm — mirrors Java trim() lines 448-495                  *)
(***************************************************************************)
RECURSIVE TrimSorted(_)
TrimSorted(sorted) ==
    IF Len(sorted) <= 1 THEN sorted
    ELSE LET current == sorted[1]
             next    == sorted[2]
             rest    == SubSeq(sorted, 2, Len(sorted))
         IN IF next.start <= current.end THEN
                \* Case 1: Overlap — extend current if next ends further
                LET merged == [start |-> current.start,
                               end   |-> IF next.end > current.end
                                         THEN next.end ELSE current.end]
                IN TrimSorted(<<merged>> \o SubSeq(sorted, 3, Len(sorted)))
            ELSE IF next.start = current.end + 1 THEN
                \* Case 2: Adjacent — extend current
                LET merged == [start |-> current.start, end |-> next.end]
                IN TrimSorted(<<merged>> \o SubSeq(sorted, 3, Len(sorted)))
            ELSE
                \* Case 3: Gap — keep current, continue with rest
                <<current>> \o TrimSorted(rest)

Trim(ranges) == TrimSorted(SortByStart(ranges))

(*--algorithm SeqRangeMerge
variables
    \* Non-deterministically choose input ranges to explore all possibilities
    inputRanges \in
        UNION {[1..n -> {r \in Range : ValidRange(r)}] : n \in 1..MaxRanges},
    output = << >>;

begin
    Merge:
        output := Trim(inputRanges);
end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "de28413c" /\ chksum(tla) = "76a71904")
VARIABLES pc, inputRanges, output

vars == << pc, inputRanges, output >>

Init == (* Global variables *)
        /\ inputRanges \in UNION {[1..n -> {r \in Range : ValidRange(r)}] : n \in 1..MaxRanges}
        /\ output = << >>
        /\ pc = "Merge"

Merge == /\ pc = "Merge"
         /\ output' = Trim(inputRanges)
         /\ pc' = "Done"
         /\ UNCHANGED inputRanges

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == pc = "Done" /\ UNCHANGED vars

Next == Merge
           \/ Terminating

Spec == Init /\ [][Next]_vars

Termination == <>(pc = "Done")

\* END TRANSLATION 

(***************************************************************************)
(* Invariants                                                              *)
(***************************************************************************)

\* INV1: No two output ranges overlap
NoOverlaps ==
    pc = "Done" =>
        \A i, j \in 1..Len(output) :
            i /= j => Covered(output[i]) \cap Covered(output[j]) = {}

\* INV2: No two output ranges are adjacent (would have been merged)
NoAdjacent ==
    pc = "Done" =>
        \A i \in 1..Len(output)-1 :
            output[i].end + 1 /= output[i+1].start

\* INV3: Every sequence in the input is in the output, and vice versa
CoveragePreservation ==
    pc = "Done" => AllCovered(inputRanges) = AllCovered(output)

\* INV4: Output ranges are sorted by start
SortedOutput ==
    pc = "Done" =>
        \A i \in 1..Len(output)-1 :
            output[i].start < output[i+1].start

\* INV5: Trimming the output again produces the same result (idempotence)
Idempotence ==
    pc = "Done" => Trim(output) = output

=============================================================================
