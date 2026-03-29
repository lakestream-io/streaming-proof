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
 * Worker-side client metrics snapshot for a single proof run.
 *
 * @param sendAttempts Total producer send attempts
 * @param acknowledgedMessages Total acknowledged producer sends
 * @param acknowledgedBytes Estimated total bytes acknowledged by producers
 * @param publishErrors Total producer errors
 * @param receivedMessages Total consumed messages
 * @param receivedBytes Estimated total bytes observed by consumers
 * @param publishLatency Snapshot for producer publish latency
 * @param endToEndLatency Snapshot for end-to-end latency
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ProofWorkerMetricsSnapshot(
        long sendAttempts,
        long acknowledgedMessages,
        long acknowledgedBytes,
        long publishErrors,
        long receivedMessages,
        long receivedBytes,
        LatencyMetricSnapshot publishLatency,
        LatencyMetricSnapshot endToEndLatency) {
}
