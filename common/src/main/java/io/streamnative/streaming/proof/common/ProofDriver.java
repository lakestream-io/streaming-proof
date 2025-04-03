/**
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

import java.util.Map;

/**
 * A driver interface for messaging systems in the streaming proof framework.
 * This interface provides the necessary operations to manage topics and create
 * producers and consumers for testing messaging system guarantees.
 *
 * <p>The driver acts as an abstraction layer between the streaming proof system
 * and the underlying messaging system (e.g., Kafka, Pulsar). It handles:
 * <ul>
 *   <li>System initialization and configuration</li>
 *   <li>Topic lifecycle management</li>
 *   <li>Producer and consumer creation</li>
 *   <li>System-specific configuration management</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * ProofDriver driver = new KafkaProofDriver();
 * Map<String, Object> configs = Map.of(
 *     "bootstrap.servers", "localhost:9092",
 *     "client.id", "streaming-proof"
 * );
 * 
 * // Initialize the driver
 * driver.init(configs);
 * 
 * // Create a test topic
 * driver.createTopic("test-topic", 8);
 * 
 * // Create producer and consumer
 * ProofProducer producer = driver.createProducer("test-topic", producerConfigs);
 * ProofConsumer consumer = driver.createConsumer("test-topic", consumerConfigs, listener);
 * }</pre>
 *
 * @see ProofProducer
 * @see ProofConsumer
 * @see MessageListener
 * @see KafkaProofDriver
 */
public interface ProofDriver {

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
     */
    void createTopic(String topicName, int partitions);

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
     * @return A new {@link ProofProducer} instance configured for the specified topic
     */
    ProofProducer createProducer(String topicName, Map<String, Object> configs);

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
