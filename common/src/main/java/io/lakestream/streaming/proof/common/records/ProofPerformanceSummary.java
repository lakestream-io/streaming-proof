/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package io.lakestream.streaming.proof.common.records;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * A compact performance summary derived from the current proof state.
 *
 * <p>This initial version is intentionally based on existing checkpoints so it
 * can power the report UI without requiring a separate metrics pipeline.
 *
 * @param elapsedSeconds Seconds since the proof started
 * @param plannedDurationSeconds Configured proof duration in seconds
 * @param remainingSeconds Remaining time before the planned duration is reached
 * @param progressPercent Progress through the configured duration, from 0 to 100
 * @param targetMsgRate Configured target publish rate
 * @param publishedMessages Number of successfully published messages seen in checkpoints
 * @param publishErrors Number of producer-side errors
 * @param publishAttempts Total publish attempts, including errors
 * @param consumedMessages Number of consumed deliveries derived from consumer ranges
 * @param backlogMessages Estimated message backlog derived from published and consumed totals
 * @param verifiedMessages Number of verified messages from correctness summary
 * @param publishRate Published messages per second
 * @param consumeRate Consumed messages per second
 * @param publishErrorRate Publish errors per second
 * @param verifyRate Verified messages per second
 * @param publishBytesRate Estimated published bytes per second
 * @param consumeBytesRate Estimated consumed bytes per second
 * @param publishLatency Publish latency summary
 * @param endToEndLatency End-to-end latency summary
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ProofPerformanceSummary(
        long elapsedSeconds,
        long plannedDurationSeconds,
        long remainingSeconds,
        double progressPercent,
        int targetMsgRate,
        long publishedMessages,
        long publishErrors,
        long publishAttempts,
        long consumedMessages,
        long backlogMessages,
        long verifiedMessages,
        double publishRate,
        double consumeRate,
        double publishErrorRate,
        double verifyRate,
        double publishBytesRate,
        double consumeBytesRate,
        LatencySummary publishLatency,
        LatencySummary endToEndLatency) {
}
