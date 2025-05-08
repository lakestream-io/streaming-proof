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

/**
 * Core verification interface that processes received messages and tracks sequence ranges.
 * 
 * <p>MessageListener is the central component for verifying messaging guarantees by:
 * <ul>
 *   <li>Receiving messages with their key, sequence value, and metadata</li>
 *   <li>Maintaining sequence ranges for each key to detect gaps or duplicates</li>
 *   <li>Tracking message ordering to verify sequence integrity</li>
 *   <li>Building checkpoints that capture the current verification state</li>
 *   <li>Detecting violations of messaging guarantees in real-time</li>
 * </ul>
 *
 * <p>The sequence-based verification methodology enables validation of:
 * <ul>
 *   <li><strong>At-least-once delivery:</strong> Verified by ensuring no gaps exist in sequence ranges</li>
 *   <li><strong>Exactly-once processing:</strong> Verified by detecting any overlapping sequence ranges</li>
 *   <li><strong>Ordering guarantees:</strong> Verified by ensuring sequences are received in ascending order</li>
 *   <li><strong>Partition isolation:</strong> Verified by tracking sequence ranges per partition</li>
 * </ul>
 *
 * <p>Implementations typically use efficient data structures like range sets to track
 * received sequences with minimal memory overhead, even for high-volume message streams.
 *
 * @see ProofConsumer
 * @see MessageMetadata
 * @see io.streamnative.streaming.proof.common.records.ConsumerCheckPoint
 * @see io.streamnative.streaming.proof.worker.DefaultMessageListener
 */
public interface MessageListener {

    /**
     * Processes a received message for verification tracking.
     *
     * <p>This method is called by the {@link ProofConsumer} implementation whenever a message
     * is received from the underlying messaging system. The implementation should:
     * <ul>
     *   <li>Track the sequence value for the given key</li>
     *   <li>Update internal range sets to reflect the received sequence</li>
     *   <li>Detect and log any sequence gaps, duplicates, or ordering violations</li>
     *   <li>Store metadata for checkpoint creation and verification</li>
     * </ul>
     *
     * <p>Thread-safety considerations:
     * <ul>
     *   <li>This method may be called concurrently from multiple consumer threads</li>
     *   <li>Implementations must use thread-safe data structures for sequence tracking</li>
     *   <li>Checkpoint creation should be synchronized to ensure consistency</li>
     * </ul>
     *
     * @param key The message key that identifies the sequence stream
     * @param value The sequential value within the key's stream (should be monotonically increasing)
     * @param metadata System-specific positioning information for verification and debugging
     * 
     * @see MessageMetadata#isAfter(MessageMetadata)
     * @see io.streamnative.streaming.proof.common.records.ConsumerCheckPoint
     * @see io.streamnative.streaming.proof.worker.DefaultMessageListener
     */
    void onMessage(String key, long value, MessageMetadata metadata);
}
