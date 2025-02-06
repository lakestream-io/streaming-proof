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

import java.util.concurrent.CompletableFuture;

/**
 * A producer interface for the streaming proof system that generates and sends
 * messages to verify messaging system guarantees. This interface extends
 * {@link AutoCloseable} to ensure proper resource cleanup.
 *
 * <p>The producer generates sequential values for each key to enable verification
 * of message ordering and delivery guarantees. Each message consists of:
 * <ul>
 *   <li>A string key that identifies the message sequence</li>
 *   <li>A sequential value that represents the message's position in its key's sequence</li>
 * </ul>
 *
 * <p>The asynchronous nature of the interface allows for high-throughput testing
 * and better resource utilization. Failed sends are reported through the
 * CompletableFuture's exceptional completion.
 *
 * <p>Example usage:
 * <pre>{@code
 * ProofProducer producer = driver.createProducer("test-topic", configs);
 * 
 * // Send messages with sequential values for a key
 * String key = "test-key";
 * for (long i = 0; i < 1000; i++) {
 *     producer.sendAsync(key, i)
 *             .whenComplete((v, e) -> {
 *                 if (e != null) {
 *                     // Handle send failure
 *                     handleError(e);
 *                 }
 *             });
 * }
 * }</pre>
 *
 * @see ProofDriver
 * @see KafkaProofProducer
 * @see ProofProducerTask
 */
public interface ProofProducer extends AutoCloseable {

    /**
     * Asynchronously sends a message with the specified key and sequential value.
     *
     * @param key The message key used for partitioning and sequence identification.
     *            Keys are used to group related messages and verify ordering within
     *            each sequence.
     * @param value A sequential value representing the message's position in its
     *              key's sequence. Used to verify message ordering and detect
     *              duplicates or missing messages.
     * @return A CompletableFuture that completes when the send operation is
     *         acknowledged by the messaging system. The future completes
     *         exceptionally if the send operation fails.
     */
    CompletableFuture<Void> sendAsync(String key, long value);

}
