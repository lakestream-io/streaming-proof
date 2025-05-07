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

import io.streamnative.streaming.proof.common.ProofConsumer;

/**
 * A record representing a request to create new consumers in the streaming proof system.
 * This immutable record encapsulates all necessary information to initialize and start
 * a group of consumers for a specific streaming proof test.
 *
 * <p>The record is used by the {@link io.streamnative.streaming.proof.Worker} to create
 * and manage consumer instances that will verify message delivery guarantees.
 *
 * <p>Example usage:
 * <pre>{@code
 * Driver kafkaDriver = new Driver("kafka", kafkaConfigs);
 * NewConsumers consumers = new NewConsumers(
 *     "proof-123",      // unique identifier
 *     "test-topic",     // topic to consume from
 *     4,                // number of consumer instances
 *     kafkaDriver       // messaging system driver
 * );
 * }</pre>
 *
 * @param id A unique identifier for this group of consumers, typically matching
 *          the proof test ID they belong to
 * @param topic The name of the topic from which these consumers will read messages
 * @param consumers The number of consumer instances to create, allowing for parallel
 *                 message consumption across multiple partitions
 * @param consumeDelayMs The delay in milliseconds between message published and consumed
 * @param driver The messaging system driver configuration that specifies how to
 *              create and configure the consumer instances
 *
 * @see Driver
 * @see ProofConsumer
 */
public record NewConsumers(String id,
                           String topic,
                           int partitions,
                           int consumers,
                           long consumeDelayMs,
                           String driverName,
                           Driver driver) {
}
