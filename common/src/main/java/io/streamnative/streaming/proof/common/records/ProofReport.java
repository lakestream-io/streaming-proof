/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.streamnative.streaming.proof.common.records;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * A page-oriented report view of a proof run.
 *
 * <p>This record is intentionally lighter than {@link ProofDetails}. It keeps
 * the proof configuration and correctness summary while exposing a compact
 * checkpoint digest that is easier for browser-based UIs to render directly.
 *
 * @param proof The original proof configuration
 * @param status Current execution status, such as {@code running} or {@code stopped}
 * @param resultStatus Current result state, such as {@code running}, {@code passed}, or {@code failed}
 * @param resultReason Human-readable explanation for the current result state
 * @param clusterTargets Resolved driver targets and sanitized endpoints for the current run
 * @param summary Correctness summary for the proof
 * @param checkpointSummary Compact checkpoint counts for the UI
 * @param performanceSummary Compact performance summary derived from live checkpoints
 * @param timeSeries Sampled history captured during checkpointing
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ProofReport(
        Proof proof,
        String status,
        String resultStatus,
        String resultReason,
        java.util.List<ProofClusterTarget> clusterTargets,
        ProofSummary summary,
        ProofCheckpointSummary checkpointSummary,
        ProofPerformanceSummary performanceSummary,
        java.util.List<ProofTimeSeriesPoint> timeSeries) {
}
