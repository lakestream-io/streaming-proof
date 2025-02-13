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

/**
 * A consumer interface for the streaming proof system that verifies message delivery
 * guarantees and ordering properties. This interface extends {@link AutoCloseable}
 * to ensure proper resource cleanup.
 *
 * <p>Implementations of this interface are responsible for:
 * <ul>
 *   <li>Consuming messages from a messaging system (e.g., Kafka)</li>
 *   <li>Delivering messages to a {@link MessageListener} for verification</li>
 *   <li>Managing consumer lifecycle and resources</li>
 *   <li>Handling consumer configuration and setup</li>
 * </ul>
 *
 * <p>Example implementation:
 * <pre>{@code
 * public class KafkaProofConsumer implements ProofConsumer {
 *     private final KafkaConsumer<String, Long> consumer;
 *     private final MessageListener listener;
 *
 *     public KafkaProofConsumer(KafkaConsumer<String, Long> consumer,
 *                              MessageListener listener) {
 *         this.consumer = consumer;
 *         this.listener = listener;
 *     }
 *
 *     @Override
 *     public void close() throws Exception {
 *         consumer.close();
 *     }
 * }
 * }</pre>
 *
 * @see MessageListener
 * @see ProofDriver
 * @see KafkaProofConsumer
 */
public interface ProofConsumer extends AutoCloseable {

    String name();
}
