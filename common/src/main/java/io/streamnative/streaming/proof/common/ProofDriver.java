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
package io.streamnative.streaming.proof.common;

import java.io.Closeable;
import java.util.Map;

/**
 * A driver interface for messaging systems in the streaming proof verification framework.
 * 
 * <p>ProofDriver provides a unified abstraction layer over different messaging systems
 * (like Kafka, Pulsar) to enable standardized verification of messaging guarantees.
 * It handles:
 * <ul>
 *   <li>System initialization with messaging-specific configurations</li>
 *   <li>Topic lifecycle management with appropriate partitioning</li>
 *   <li>Producer creation with at-least-once delivery semantics</li>
 *   <li>Consumer creation with configurable consumption behavior</li>
 *   <li>Message metadata extraction for verification tracing</li>
 * </ul>
 *
 * <p>The driver architecture enables:
 * <ul>
 *   <li>System-agnostic verification of messaging guarantees</li>
 *   <li>Consistent testing methodology across different messaging platforms</li>
 *   <li>Isolation of system-specific implementation details</li>
 *   <li>Extensibility to support additional messaging systems</li>
 * </ul>
 *
 *
 * @see ProofProducer
 * @see ProofConsumer
 * @see MessageListener
 */
public interface ProofDriver extends Closeable {

    /**
     * Initializes the driver with system-specific configurations.
     *
     * @param configs A map of configuration parameters specific to the messaging system.
     *               For example, Kafka configurations like bootstrap servers, security settings, etc.
     */
    void init(Map<String, Object> configs);

    /**
     * Creates a new topic in the messaging system.
     *
     * @param topicName The name of the topic to create
     * @param partitions The number of partitions for the topic, affecting parallelism
     *                  and message distribution
     * @param configs Topic-specific configuration parameters
     */
    void createTopic(String topicName, int partitions, Map<String, String> configs);

    /**
     * Deletes a topic from the messaging system.
     *
     * @param topicName The name of the topic to delete
     */
    void deleteTopic(String topicName);

    /**
     * Creates a new producer instance for sending messages to a specific topic.
     *
     * @param topicName The name of the topic to produce messages to
     * @param configs Producer-specific configuration parameters
     * @param messageSize The total size in bytes of each message value, including the
     *                    8-byte sequence number. Sizes above the minimum pad the message
     *                    so that storage-path behaviour can be exercised at a realistic
     *                    scale. Must be at least {@link ProofValue#MIN_SIZE}.
     * @return A new {@link ProofProducer} instance configured for the specified topic
     */
    ProofProducer createProducer(String topicName, Map<String, Object> configs, int messageSize);

    /**
     * Creates a new consumer instance for receiving messages from a specific topic.
     *
     * @param topicName The name of the topic to consume messages from
     * @param partitions The number of partitions of the topic to consume from
     * @param configs Consumer-specific configuration parameters
     * @param listener A {@link MessageListener} that will receive the consumed messages
     * @return A new {@link ProofConsumer} instance configured for the specified topic
     */
    ProofConsumer createConsumer(String topicName, int partitions, long consumeDelayMs, Map<String, Object> configs,
                 MessageListener listener);
}
