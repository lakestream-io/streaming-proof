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
import java.util.List;

/**
 * A mergeable latency snapshot with bounded samples.
 *
 * <p>The sample list is intentionally bounded so workers can expose latency
 * distributions without returning every single measurement.
 *
 * @param count Total number of recorded samples
 * @param sumMillis Sum of all recorded latencies in milliseconds
 * @param maxMillis Maximum observed latency in milliseconds
 * @param samplesMillis A bounded sample set used for percentile estimation
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record LatencyMetricSnapshot(
        long count,
        double sumMillis,
        long maxMillis,
        List<Long> samplesMillis) {
}
