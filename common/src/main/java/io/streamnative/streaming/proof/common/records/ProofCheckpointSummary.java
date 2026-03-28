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

/**
 * A lightweight checkpoint summary intended for UI consumption.
 *
 * <p>Unlike {@link Checkpoints}, this record only exposes key counts for the
 * most important checkpoint snapshots, which keeps report payloads compact
 * while still showing test progress at a glance.
 *
 * @param inCheckKeys Number of keys in the current in-check producer snapshot
 * @param latestProducerKeys Number of keys in the latest producer snapshot
 * @param latestConsumerKeys Number of keys in the latest consumer snapshot
 * @param verifiedProducerKeys Number of keys in the last verified producer snapshot
 * @param verifiedConsumerKeys Number of keys in the last verified consumer snapshot
 * @param failedProducerKeys Number of keys in the last failed producer snapshot
 * @param failedConsumerKeys Number of keys in the last failed consumer snapshot
 */
public record ProofCheckpointSummary(
        int inCheckKeys,
        int latestProducerKeys,
        int latestConsumerKeys,
        int verifiedProducerKeys,
        int verifiedConsumerKeys,
        int failedProducerKeys,
        int failedConsumerKeys) {
}
