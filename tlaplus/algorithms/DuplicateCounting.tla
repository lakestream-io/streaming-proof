-------------------------- MODULE DuplicateCounting -------------------------
(***************************************************************************)
(* Models: ConsumerCheckPoint.getDuplicatedCount()                         *)
(*                                                                         *)
(* For each range[i], trims ranges[0..i-1], then counts overlap between    *)
(* range[i] and the trimmed prior ranges. Also accumulates each range's    *)
(* intrinsic 'duplicated' count (set during message receipt).              *)
(*                                                                         *)
(* What This Proves:                                                       *)
(*   - Accuracy: dupCount = overlap sequences + intrinsic duplicates       *)
(*   - NonNegative: count is never negative                                *)
(*   - ZeroWhenNoOverlaps: no overlaps and no intrinsics => count = 0      *)
(*   - Commutative: input order doesn't affect final count (non-obvious)   *)
(***************************************************************************)

EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS MaxSeq, MaxRanges, MaxIntrinsicDup

\* Ranges now include an intrinsic duplicate count
DupRange == [start : 0..MaxSeq, end : 0..MaxSeq, duplicated : 0..MaxIntrinsicDup]
ValidRange(r) == r.start <= r.end

Covered(r) == r.start .. r.end
AllCovered(rs) == UNION {Covered(rs[i]) : i \in 1..Len(rs)}

\* Trim (from SeqRangeMerge) — simplified inline version
RECURSIVE SortByStart(_)
SortByStart(rs) ==
    IF rs = << >> THEN << >>
    ELSE LET minIdx == CHOOSE i \in 1..Len(rs) :
                \A j \in 1..Len(rs) : rs[i].start <= rs[j].start
             minElem == rs[minIdx]
             rest == SubSeq(rs, 1, minIdx-1) \o SubSeq(rs, minIdx+1, Len(rs))
         IN <<minElem>> \o SortByStart(rest)

RECURSIVE TrimSorted(_)
TrimSorted(sorted) ==
    IF Len(sorted) <= 1 THEN sorted
    ELSE LET cur == sorted[1]
             nxt == sorted[2]
         IN IF nxt.start <= cur.end THEN
                LET m == [start |-> cur.start,
                          end |-> IF nxt.end > cur.end THEN nxt.end ELSE cur.end,
                          duplicated |-> 0]
                IN TrimSorted(<<m>> \o SubSeq(sorted, 3, Len(sorted)))
            ELSE IF nxt.start = cur.end + 1 THEN
                LET m == [start |-> cur.start, end |-> nxt.end, duplicated |-> 0]
                IN TrimSorted(<<m>> \o SubSeq(sorted, 3, Len(sorted)))
            ELSE <<cur>> \o TrimSorted(SubSeq(sorted, 2, Len(sorted)))

TrimRanges(ranges) == TrimSorted(SortByStart(ranges))

\* Compute overlap between a range and a sequence of trimmed ranges
\* Uses index-based recursion to avoid set deduplication of equal overlap counts
RECURSIVE OverlapCount(_, _, _)
OverlapCount(current, trimmed, j) ==
    IF j > Len(trimmed) THEN 0
    ELSE LET oStart == IF current.start > trimmed[j].start THEN current.start ELSE trimmed[j].start
             oEnd   == IF current.end < trimmed[j].end THEN current.end ELSE trimmed[j].end
             cnt    == IF oEnd >= oStart THEN oEnd - oStart + 1 ELSE 0
         IN cnt + OverlapCount(current, trimmed, j + 1)

\* The Java algorithm: iterate ranges, trim prior, count overlaps + intrinsics
RECURSIVE CountDups(_, _)
CountDups(ranges, idx) ==
    IF idx > Len(ranges) THEN 0
    ELSE IF idx = 1 THEN
        ranges[1].duplicated + CountDups(ranges, 2)
    ELSE
        LET prior   == SubSeq(ranges, 1, idx - 1)
            trimmed == TrimRanges(prior)
            current == ranges[idx]
            overlap == OverlapCount(current, trimmed, 1)
        IN overlap + current.duplicated + CountDups(ranges, idx + 1)

\* Ground truth: count how many sequences appear in more than one range
MultiCoveredCount(ranges) ==
    LET seqs == AllCovered(ranges)
        count(s) == Cardinality({i \in 1..Len(ranges) : s \in Covered(ranges[i])})
        multiCovered == {s \in seqs : count(s) > 1}
        \* For each multi-covered seq, it's duplicated (count-1) times
        dupContribution == LET RECURSIVE SumOver(_, _)
                               SumOver(S, acc) == IF S = {} THEN acc
                                                  ELSE LET x == CHOOSE x \in S : TRUE
                                                       IN SumOver(S \ {x}, acc + count(x) - 1)
                           IN SumOver(multiCovered, 0)
    IN dupContribution

IntrinsicTotal(ranges) ==
    LET RECURSIVE SumIntr(_, _)
        SumIntr(idx, acc) == IF idx > Len(ranges) THEN acc
                             ELSE SumIntr(idx + 1, acc + ranges[idx].duplicated)
    IN SumIntr(1, 0)

(*--algorithm DuplicateCounting
variables
    inputRanges \in
        UNION {[1..n -> {r \in DupRange : ValidRange(r)}] : n \in 1..MaxRanges},
    dupCount = 0;

begin
    Count:
        dupCount := CountDups(inputRanges, 1);
end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "da61758c" /\ chksum(tla) = "80a91476")
VARIABLES pc, inputRanges, dupCount

vars == << pc, inputRanges, dupCount >>

Init == (* Global variables *)
        /\ inputRanges \in UNION {[1..n -> {r \in DupRange : ValidRange(r)}] : n \in 1..MaxRanges}
        /\ dupCount = 0
        /\ pc = "Count"

Count == /\ pc = "Count"
         /\ dupCount' = CountDups(inputRanges, 1)
         /\ pc' = "Done"
         /\ UNCHANGED inputRanges

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == pc = "Done" /\ UNCHANGED vars

Next == Count
           \/ Terminating

Spec == Init /\ [][Next]_vars

Termination == <>(pc = "Done")

\* END TRANSLATION 

(***************************************************************************)
(* Invariants                                                              *)
(***************************************************************************)

\* INV1: Accuracy — dupCount = overlap duplicates + intrinsic duplicates
Accuracy ==
    pc = "Done" => dupCount = MultiCoveredCount(inputRanges) + IntrinsicTotal(inputRanges)

\* INV2: Non-negative
NonNegative == pc = "Done" => dupCount >= 0

\* INV3: Zero when no overlaps and no intrinsic duplicates
ZeroWhenClean ==
    pc = "Done" =>
    LET NoOverlaps == \A i, j \in 1..Len(inputRanges) :
                          i /= j => Covered(inputRanges[i]) \cap Covered(inputRanges[j]) = {}
        NoIntrinsics == IntrinsicTotal(inputRanges) = 0
    IN (NoOverlaps /\ NoIntrinsics) => dupCount = 0

\* INV4: Commutative — processing order doesn't affect result (non-obvious!)
\* TLC will verify or refute this with a counterexample.
RECURSIVE Perms(_)
Perms(S) ==
    IF S = << >> THEN {<< >>}
    ELSE UNION {
        {<<S[i]>> \o p : p \in Perms(
            SubSeq(S, 1, i-1) \o SubSeq(S, i+1, Len(S)))}
        : i \in 1..Len(S)}

Commutative ==
    pc = "Done" =>
    \A perm \in Perms(inputRanges) :
        CountDups(perm, 1) = dupCount

=============================================================================
