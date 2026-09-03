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
 * A single report time-series point captured during checkpointing.
 *
 * @param elapsedSeconds Seconds since the proof started
 * @param publishRate Current publish rate in messages per second
 * @param consumeRate Current consume rate in messages per second
 * @param backlogMessages Current backlog size
 * @param publishErrorRate Current publish error rate in errors per second
 * @param publishLatencyP95 Current publish latency p95 in milliseconds
 * @param publishLatencyP99 Current publish latency p99 in milliseconds
 * @param endToEndLatencyP95 Current end-to-end latency p95 in milliseconds
 * @param endToEndLatencyP99 Current end-to-end latency p99 in milliseconds
 * @param publishBytesRate Current published bytes per second
 * @param consumeBytesRate Current consumed bytes per second
 * @param verifiedMessages Current verified message count
 * @param publishedMessages Current published message count
 * @param consumedMessages Current consumed message count
 * @param errors Current error count
 * @param timeouts Current timeout count
 * @param missed Current missed message count
 * @param duplicates Current duplicate message count
 * @param outOfOrders Current out-of-order message count
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ProofTimeSeriesPoint(
        long elapsedSeconds,
        double publishRate,
        double consumeRate,
        long backlogMessages,
        double publishErrorRate,
        double publishLatencyP95,
        double publishLatencyP99,
        double endToEndLatencyP95,
        double endToEndLatencyP99,
        double publishBytesRate,
        double consumeBytesRate,
        long verifiedMessages,
        long publishedMessages,
        long consumedMessages,
        long errors,
        long timeouts,
        long missed,
        long duplicates,
        long outOfOrders) {
}
