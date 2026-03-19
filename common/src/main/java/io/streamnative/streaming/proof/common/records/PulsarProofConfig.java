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

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Per-proof Pulsar-specific configuration.
 *
 * <p>Holds optional consumer configuration that is passed through to
 * Pulsar's {@code ConsumerBuilder.loadConf()} at consumer creation time.
 * Any key accepted by Pulsar's {@code ConsumerConfigurationData} can be used.
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class PulsarProofConfig {

    /**
     * Consumer configuration map passed to {@code ConsumerBuilder.loadConf()}.
     * Keys must match Pulsar's ConsumerConfigurationData field names (case-sensitive).
     * Example keys: "subscriptionType" ("Exclusive", "Failover", "Key_Shared"),
     * "receiverQueueSize", "ackTimeoutMillis".
     */
    private Map<String, Object> consumerConfig;
}
