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
import java.util.Map;

/**
 * A record representing a streaming proof driver configuration.
 * This immutable record encapsulates the driver type and its associated configurations.
 * Used in conjunction with {@link ProofDriver} implementations to create producers and consumers.
 *
 * @param driverType The type of the driver (e.g., "kafka")
 * @param driverConfigs A map of configuration parameters specific to the driver implementation.
 *                     The keys and values vary depending on the driver type.
 *                     For example, Kafka driver configs might include "bootstrap.servers",
 *                     "security.protocol", etc.
 * @param metadata Optional descriptive metadata for reporting. This can include
 *                 cluster display names, sizing summaries, worker notes, or any
 *                 other non-sensitive environment context that should be shown in reports.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Driver(String driverType, Map<String, Object> driverConfigs, Map<String, Object> metadata) {

    public Driver(String driverType, Map<String, Object> driverConfigs) {
        this(driverType, driverConfigs, null);
    }
}
