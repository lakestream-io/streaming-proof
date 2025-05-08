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

import java.util.concurrent.CompletableFuture;

/**
 * A producer interface for the streaming proof verification framework that generates
 * and sends sequentially numbered messages to verify messaging system guarantees.
 *
 * <p>The ProofProducer is responsible for:
 * <ul>
 *   <li>Generating sequential values for each message key</li>
 *   <li>Sending messages with at-least-once delivery semantics</li>
 *   <li>Tracking message metadata for verification purposes</li>
 *   <li>Supporting high-throughput asynchronous operations</li>
 *   <li>Providing proper resource management through AutoCloseable</li>
 * </ul>
 *
 * <p>Each message consists of:
 * <ul>
 *   <li>A string key that identifies the message sequence and determines routing</li>
 *   <li>A sequential long value that represents the message's position in its key's sequence</li>
 *   <li>System-specific metadata captured on successful delivery</li>
 * </ul>
 *
 *
 * @see ProofDriver
 * @see MessageMetadata
 * @see io.streamnative.streaming.proof.common.records.ProducerCheckpoint
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
     * @return A CompletableFuture that completes with the message metadata when the
     *         send operation is acknowledged by the messaging system. The metadata
     *         includes information such as the message offset. The future completes
     *         exceptionally if the send operation fails.
     * @see MessageMetadata
     */
    CompletableFuture<MessageMetadata> sendAsync(String key, long value);

}
