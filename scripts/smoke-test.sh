#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Smoke test: build, deploy via Docker Compose, run a proof, verify results.
#
# Usage:
#   ./scripts/smoke-test.sh              # full run (build + test)
#   ./scripts/smoke-test.sh --skip-build # skip Maven build & Docker image build
#   ./scripts/smoke-test.sh --cleanup    # only tear down the Docker Compose stack

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_DIR="$PROJECT_ROOT/deploy/docker-compose/pulsar"
COMPOSE_FILE="$COMPOSE_DIR/docker-compose-develop.yml"
COORDINATOR_URL="http://localhost:8080"

# Proof parameters
PROOF_DRIVER="serverless"
PROOF_TOPIC="persistent://public/default/smoke-test"
PROOF_DURATION=30
PROOF_MSG_RATE=100
PROOF_CHECKPOINT_INTERVAL=3
PROOF_TIMEOUT=60

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
NC='\033[0m'

info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
fail()  { echo -e "${RED}[FAIL]${NC}  $*"; exit 1; }

cleanup() {
  info "Tearing down Docker Compose stack..."
  docker compose -f "$COMPOSE_FILE" down --remove-orphans 2>/dev/null || true
  ok "Stack removed."
}

# ── Argument parsing ──────────────────────────────────────────────
SKIP_BUILD=false
CLEANUP_ONLY=false
for arg in "$@"; do
  case "$arg" in
    --skip-build) SKIP_BUILD=true ;;
    --cleanup)    CLEANUP_ONLY=true ;;
    -h|--help)
      echo "Usage: $0 [--skip-build] [--cleanup]"
      exit 0
      ;;
  esac
done

if $CLEANUP_ONLY; then
  cleanup
  exit 0
fi

# ── Step 1: Build ─────────────────────────────────────────────────
if $SKIP_BUILD; then
  warn "Skipping build (--skip-build)"
else
  info "Building project with Maven..."
  cd "$PROJECT_ROOT"
  mvn package -DskipTests -q
  ok "Maven build complete."

  info "Building Docker image..."
  docker build -t streamnative/streaming-proof:latest \
    --build-arg PROOF_TARBALL=distribution/target/streaming-proof-1.0-SNAPSHOT-bin.tar.gz \
    -f docker/Dockerfile . -q
  ok "Docker image built."
fi

# ── Step 2: Start environment ─────────────────────────────────────
info "Starting Docker Compose stack..."
docker compose -f "$COMPOSE_FILE" down --remove-orphans 2>/dev/null || true
docker compose -f "$COMPOSE_FILE" up -d 2>/dev/null
ok "Stack started. Waiting for coordinator to be ready..."

# Wait for coordinator
for i in $(seq 1 30); do
  if curl -sf "$COORDINATOR_URL/proofs" >/dev/null 2>&1; then
    break
  fi
  if [ "$i" -eq 30 ]; then
    fail "Coordinator did not become ready within 30s"
  fi
  sleep 1
done
ok "Coordinator is ready."

# Verify configs are loaded
CONFIGS=$(curl -sf "$COORDINATOR_URL/configs" 2>&1)
if echo "$CONFIGS" | grep -q '"workers"'; then
  ok "Configs loaded: $CONFIGS"
else
  fail "Configs not loaded. Got: $CONFIGS"
fi

# ── Step 3: Submit proof ──────────────────────────────────────────
info "Submitting proof (duration=${PROOF_DURATION}s, rate=${PROOF_MSG_RATE} msg/s)..."
PROOF_RESPONSE=$(curl -sf -X POST "$COORDINATOR_URL/proofs" \
  -H 'Content-Type: application/json' \
  -d "{
    \"name\": \"Smoke Test\",
    \"driver\": \"$PROOF_DRIVER\",
    \"topic\": \"$PROOF_TOPIC\",
    \"partitions\": 1,
    \"producers\": 1,
    \"consumers\": 1,
    \"msgRate\": $PROOF_MSG_RATE,
    \"duration\": $PROOF_DURATION,
    \"checkPointInterval\": $PROOF_CHECKPOINT_INTERVAL,
    \"timeout\": $PROOF_TIMEOUT,
    \"features\": [\"exactly_once\", \"ordering\"]
  }")

PROOF_ID=$(echo "$PROOF_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
ok "Proof submitted: id=$PROOF_ID"
info "UI available at: $COORDINATOR_URL/ui/"

# ── Step 4: Wait for completion ───────────────────────────────────
MAX_WAIT=$((PROOF_DURATION + PROOF_TIMEOUT + 30))
info "Waiting up to ${MAX_WAIT}s for proof to complete..."

for i in $(seq 1 "$MAX_WAIT"); do
  STATUS=$(curl -sf "$COORDINATOR_URL/proofs/$PROOF_ID/report" | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])" 2>/dev/null || echo "unknown")
  if [ "$STATUS" = "passed" ] || [ "$STATUS" = "failed" ] || [ "$STATUS" = "stopped" ]; then
    break
  fi
  if [ "$i" -eq "$MAX_WAIT" ]; then
    fail "Proof did not complete within ${MAX_WAIT}s (last status: $STATUS)"
  fi
  # Print progress every 10s
  if [ $((i % 10)) -eq 0 ]; then
    info "  ...still running (${i}s elapsed, status=$STATUS)"
  fi
  sleep 1
done

ok "Proof completed."

# ── Step 5: Verify results ────────────────────────────────────────
info "Verifying results..."
REPORT=$(curl -sf "$COORDINATOR_URL/proofs/$PROOF_ID/report")

RESULT_STATUS=$(echo "$REPORT" | python3 -c "import sys,json; data=json.load(sys.stdin); print(data.get('resultStatus', data.get('status', 'unknown')))")
RESULT_REASON=$(echo "$REPORT" | python3 -c "import sys,json; print(json.load(sys.stdin)['resultReason'])")
VERIFIED=$(echo "$REPORT"      | python3 -c "import sys,json; print(json.load(sys.stdin)['summary']['verified'])")
MISSED=$(echo "$REPORT"        | python3 -c "import sys,json; print(json.load(sys.stdin)['summary']['missed'])")
DUPLICATES=$(echo "$REPORT"    | python3 -c "import sys,json; print(json.load(sys.stdin)['summary']['duplicates'])")
OUT_OF_ORDER=$(echo "$REPORT"  | python3 -c "import sys,json; print(json.load(sys.stdin)['summary']['outOfOrders'])")
ERRORS=$(echo "$REPORT"        | python3 -c "import sys,json; print(json.load(sys.stdin)['summary']['errors'])")
PUBLISHED=$(echo "$REPORT"     | python3 -c "import sys,json; print(json.load(sys.stdin)['performanceSummary']['publishedMessages'])")
CONSUMED=$(echo "$REPORT"      | python3 -c "import sys,json; print(json.load(sys.stdin)['performanceSummary']['consumedMessages'])")
VERIFIED_PERF=$(echo "$REPORT" | python3 -c "import sys,json; print(json.load(sys.stdin)['performanceSummary']['verifiedMessages'])")

echo ""
echo "  ┌─────────────────────────────────────────┐"
echo "  │           Smoke Test Results             │"
echo "  ├──────────────────┬──────────────────────┤"
printf "  │ %-16s │ %-20s │\n" "Result" "$RESULT_STATUS"
printf "  │ %-16s │ %-20s │\n" "Published" "$PUBLISHED"
printf "  │ %-16s │ %-20s │\n" "Consumed" "$CONSUMED"
printf "  │ %-16s │ %-20s │\n" "Verified" "$VERIFIED"
printf "  │ %-16s │ %-20s │\n" "Missed" "$MISSED"
printf "  │ %-16s │ %-20s │\n" "Duplicates" "$DUPLICATES"
printf "  │ %-16s │ %-20s │\n" "Out-of-Order" "$OUT_OF_ORDER"
printf "  │ %-16s │ %-20s │\n" "Errors" "$ERRORS"
echo "  └──────────────────┴──────────────────────┘"
echo "  Reason: $RESULT_REASON"
echo ""

# Assertions
PASS=true
if [ "$RESULT_STATUS" != "passed" ]; then
  fail "Result status is '$RESULT_STATUS', expected 'passed'"
  PASS=false
fi
if [ "$MISSED" != "0" ]; then
  fail "Missed messages: $MISSED"
  PASS=false
fi
if [ "$OUT_OF_ORDER" != "0" ]; then
  fail "Out-of-order messages: $OUT_OF_ORDER"
  PASS=false
fi
if [ "$PUBLISHED" != "$CONSUMED" ] || [ "$PUBLISHED" != "$VERIFIED_PERF" ]; then
  fail "Message count mismatch: published=$PUBLISHED consumed=$CONSUMED verified=$VERIFIED_PERF"
  PASS=false
fi

if $PASS; then
  ok "All assertions passed!"
else
  fail "Some assertions failed."
fi

# ── Step 6: Cleanup ───────────────────────────────────────────────
info "Cleaning up..."
cleanup
ok "Smoke test complete."
