#!/usr/bin/env bash
set -euo pipefail

# TLC runner for TLA+ specs
# Usage: ./run-tlc.sh [--spec NAME]
#
# Runs all specs by default, or a single spec if --spec is provided.
# Exit code 0 = all specs pass, non-zero = at least one failure.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TLAPLUS_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
MODELS_DIR="$TLAPLUS_DIR/models"

SINGLE_SPEC=""
if [[ "${1:-}" == "--spec" && -n "${2:-}" ]]; then
    SINGLE_SPEC="$2"
fi

FAILED=0
PASSED=0
TOTAL=0

for cfg in "$MODELS_DIR"/*.cfg; do
    spec_name="$(basename "$cfg" .cfg)"

    if [[ -n "$SINGLE_SPEC" && "$spec_name" != "$SINGLE_SPEC" ]]; then
        continue
    fi

    # Find the .tla file in algorithms/ or protocol/
    # Try exact match first, then progressively shorter prefixes
    # (handles multi-config specs like CheckpointVerificationShared.cfg
    #  mapping to CheckpointVerification.tla)
    tla_file=""
    candidate="$spec_name"
    while [[ -n "$candidate" ]]; do
        for dir in algorithms protocol; do
            if [[ -f "$TLAPLUS_DIR/$dir/$candidate.tla" ]]; then
                tla_file="$TLAPLUS_DIR/$dir/$candidate.tla"
                break 2
            fi
        done
        # Remove trailing uppercase-started word (e.g., "Shared" from "CheckpointVerificationShared")
        shorter="${candidate%[A-Z]*}"
        if [[ "$shorter" == "$candidate" ]]; then
            break
        fi
        candidate="$shorter"
    done

    if [[ -z "$tla_file" ]]; then
        echo "ERROR: No .tla file found for $spec_name"
        FAILED=$((FAILED + 1))
        TOTAL=$((TOTAL + 1))
        continue
    fi

    TOTAL=$((TOTAL + 1))
    echo "========================================"
    echo "Running: $spec_name"
    echo "  TLA: $tla_file"
    echo "  CFG: $cfg"
    echo "========================================"

    # Use tla2tools.jar if available (CI/local), fall back to tlc command (Docker)
    if [[ -f "$SCRIPT_DIR/tla2tools.jar" ]]; then
        TLC_CMD="java -XX:+UseParallelGC -cp $SCRIPT_DIR/tla2tools.jar tlc2.TLC"
    else
        TLC_CMD="tlc"
    fi

    if $TLC_CMD \
        -config "$cfg" \
        -workers auto \
        "$tla_file"; then
        echo "PASS: $spec_name"
        PASSED=$((PASSED + 1))
    else
        echo "FAIL: $spec_name"
        FAILED=$((FAILED + 1))
    fi
    echo ""
done

echo "========================================"
echo "Results: $PASSED/$TOTAL passed, $FAILED failed"
echo "========================================"

exit $(( FAILED > 0 ? 1 : 0 ))
