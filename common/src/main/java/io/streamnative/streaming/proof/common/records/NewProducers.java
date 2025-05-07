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

import io.streamnative.streaming.proof.common.ProofProducer;

/**
 * A record representing a request to create new producers in the streaming proof system.
 * This immutable record encapsulates all necessary information to initialize and start
 * a group of producers for performance and reliability testing.
 *
 * <p>The record is used by the {@link io.streamnative.streaming.proof.Worker} to create
 * and manage producer instances that will generate test messages with specific patterns
 * and rates.
 *
 * <p>Example usage:
 * <pre>{@code
 * Driver kafkaDriver = new Driver("kafka", kafkaConfigs);
 * NewProducers producers = new NewProducers(
 *     "proof-123",      // unique identifier
 *     "test-topic",     // topic to produce to
 *     4,                // number of producer instances
 *     100,              // number of unique message keys
 *     1000,             // message rate (msgs/sec)
 *     kafkaDriver       // messaging system driver
 * );
 * }</pre>
 *
 * @param id A unique identifier for this group of producers, typically matching
 *          the proof test ID they belong to
 * @param topic The name of the topic to which these producers will send messages
 * @param producers The number of producer instances to create, enabling parallel
 *                 message production for higher throughput
 * @param keys The number of unique message keys to use across all producers,
 *            affecting the distribution and partitioning of messages
 * @param msgRate The target message production rate in messages per second,
 *               distributed across all producer instances
 * @param driver The messaging system driver configuration that specifies how to
 *              create and configure the producer instances
 *
 * @see Driver
 * @see ProofProducer
 * @see io.streamnative.streaming.proof.ProofProducers
 */
public record NewProducers(String id,
                           String topic,
                           int producers,
                           int keys,
                           int msgRate,
                           String driverName,
                           Driver driver) {
}
