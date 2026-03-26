# TLA+ Verification for Streaming Proof

Formal verification of the checkpoint verification algorithms and distributed
coordinator-worker protocol using TLA+/PlusCal and the TLC model checker.

## Quick Start

Download tla2tools.jar (one-time setup):

```bash
curl -sL -o tlaplus/ci/tla2tools.jar \
    "https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar"
```

Run all specs:

```bash
bash tlaplus/ci/run-tlc.sh
```

Run a single spec:

```bash
bash tlaplus/ci/run-tlc.sh --spec SeqRangeMerge
```

## What is TLA+?

TLA+ is a formal specification language for describing and verifying
concurrent and distributed systems. PlusCal is an algorithm language that
compiles to TLA+, reading more like pseudocode.

The TLC model checker exhaustively explores all possible states of a
PlusCal spec, verifying that invariants hold in every reachable state.
If a violation is found, TLC produces a counterexample trace showing
exactly how to reach the violating state.

## Spec → Java Mapping

| Spec | Java Source | What It Verifies |
|------|------------|-----------------|
| SeqRangeMerge | `ConsumerCheckPoint.trim()` | Range merging produces no overlaps, no adjacent ranges, preserves coverage |
| GapDetection | `ConsumerCheckPoint.getMissedSeqs()` | Gap detection is complete and produces no false gaps |
| DuplicateCounting | `ConsumerCheckPoint.getDuplicatedCount()` | Duplicate count matches ground truth (overlaps + intrinsics) |
| OrderingVerification | `ProofConsumerTask.onMessage()` | Out-of-order detection has no false positives, respects timestamp isolation |
| WatermarkComputation | `ConsumerCheckPoint.computeHighWatermarks()` | Watermarks are contiguous, maximal, and monotonic |
| CheckpointVerification | `ProofTask.scheduleCheckpoint()` | Verification state machine: no false positives, no regression, progress |
| CoordinatorWorker | `ProofTask.aggregateCheckpoints()` + `WorkerHandler` | No data loss from trimming, no phantom sequences, watermark safety |

## Learning Path

Read specs in this order (each introduces new concepts):

1. **SeqRangeMerge** — PlusCal basics: variables, recursion, records, invariants
2. **GapDetection** — Set operations, completeness reasoning
3. **DuplicateCounting** — Nested computation, commutativity verification
4. **OrderingVerification** — Multi-variable state, message processing loops
5. **WatermarkComputation** — Base state, monotonicity properties
6. **CheckpointVerification** — Multi-process PlusCal, liveness properties
7. **CoordinatorWorker** — Distributed protocol, non-atomicity, failure modeling

## IDE Setup

For interactive exploration, install the [TLA+ Toolbox](https://lamport.azurewebsites.net/tla/toolbox.html)
or the [VSCode TLA+ extension](https://marketplace.visualstudio.com/items?itemName=alygin.vscode-tlaplus).
These let you step through counterexample traces visually.

## CI

TLA+ specs are checked automatically on PRs that modify:
- `common/src/main/java/**`
- `coordinator/src/main/java/**`
- `worker/src/main/java/**`
- `tlaplus/**`
