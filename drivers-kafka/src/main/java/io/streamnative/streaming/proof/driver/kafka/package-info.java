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
/**
 * Kafka implementation of the streaming proof driver that provides at-least-once message
 * delivery guarantees for distributed messaging system testing.
 *
 * <p>This package provides Kafka-specific implementations of:
 * <ul>
 *   <li>Producer - Asynchronous message production with delivery guarantees</li>
 *   <li>Consumer - Single-threaded message consumption with offset management</li>
 *   <li>Driver - Topic and client lifecycle management</li>
 * </ul>
 *
 * <p>Features:
 * <ul>
 *   <li>At-least-once message delivery semantics</li>
 *   <li>Asynchronous message production and consumption</li>
 *   <li>Configurable offset commit strategies</li>
 *   <li>Topic partitioning and replication management</li>
 *   <li>Zone-aware client ID configuration</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * // Initialize the Kafka driver
 * Map<String, Object> configs = new HashMap<>();
 * configs.put("bootstrap.servers", "localhost:9092");
 * 
 * KafkaProofDriver driver = new KafkaProofDriver();
 * driver.init(configs);
 * 
 * // Create a topic and clients
 * driver.createTopic("test-topic", 8);
 * ProofProducer producer = driver.createProducer("test-topic", producerConfigs);
 * ProofConsumer consumer = driver.createConsumer("test-topic", consumerConfigs, listener);
 * }</pre>
 *
 * @see io.streamnative.streaming.proof.driver.kafka.KafkaAtLeastOnceProofProducer
 * @see io.streamnative.streaming.proof.driver.kafka.KafkaAtLeastOnceProofConsumer
 * @see io.streamnative.streaming.proof.driver.kafka.KafkaProofDriver
 */
package io.streamnative.streaming.proof.driver.kafka;