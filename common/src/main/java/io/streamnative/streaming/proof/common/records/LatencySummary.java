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
 * A latency summary for report rendering.
 *
 * @param count Total number of latency samples
 * @param avg Average latency in milliseconds
 * @param p50 50th percentile latency in milliseconds
 * @param p95 95th percentile latency in milliseconds
 * @param p99 99th percentile latency in milliseconds
 * @param max Maximum latency in milliseconds
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record LatencySummary(
        long count,
        double avg,
        double p50,
        double p95,
        double p99,
        double max) {
}
