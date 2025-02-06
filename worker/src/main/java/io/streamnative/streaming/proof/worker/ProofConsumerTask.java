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
package io.streamnative.streaming.proof.worker;

import io.streamnative.streaming.proof.common.MessageListener;
import io.streamnative.streaming.proof.common.ProofConsumer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.Setter;

/**
 * A task that processes and validates messages consumed from a messaging system.
 * This class implements both MessageListener for receiving messages and AutoCloseable
 * for resource cleanup. It tracks message sequences, duplicates, and ordering violations
 * to verify messaging system guarantees.
 *
 * <p>The task maintains the following metrics:
 * <ul>
 *   <li>Sequence numbers per message key</li>
 *   <li>Duplicate message counts</li>
 *   <li>Out-of-order message counts</li>
 *   <li>Missing sequence numbers</li>
 *   <li>Out-of-order sequence pairs</li>
 * </ul>
 *
 * <p>Message sequence validation rules:
 * <ul>
 *   <li>Sequential: value = previous + 1</li>
 *   <li>Duplicate: value ≤ previous</li>
 *   <li>Out-of-order: value > previous + 1</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * ProofConsumerTask task = new ProofConsumerTask();
 * ProofConsumer consumer = driver.createConsumer(topic, config, task);
 * task.setConsumer(consumer);
 *
 * // Message processing happens automatically through onMessage callback
 * // Later, check the metrics:
 * System.out.println("Duplicates: " + task.getDups().get());
 * System.out.println("Out of order: " + task.getOutOfOrders().get());
 * System.out.println("Missing sequences: " + task.getMissedSeqs());
 * }</pre>
 *
 * @see MessageListener
 * @see ProofConsumer
 */
@Getter
public class ProofConsumerTask implements MessageListener, AutoCloseable {

    /** The consumer instance that this task is associated with */
    @Setter
    private ProofConsumer consumer;

    /** Maps message keys to their latest sequence numbers */
    private final Map<String, Long> keySeq;

    /** Counter for duplicate messages detected */
    private final AtomicInteger dups = new AtomicInteger(0);

    /** Counter for out-of-order messages detected */
    private final AtomicInteger outOfOrders = new AtomicInteger(0);

    /** Set of sequence numbers that were skipped (indicating missing messages) */
    private final Set<Long> missedSeqs = new HashSet<>();

    /** List of sequence number pairs that were received out of order */
    private final List<Long> outOfOrderSeqs = new ArrayList<>();

    /**
     * Creates a new ProofConsumerTask with an empty sequence tracking map.
     */
    public ProofConsumerTask() {
        this.keySeq = new HashMap<>();
    }

    /**
     * Processes a received message and validates its sequence number.
     * This method is synchronized to ensure thread-safe updates to the sequence tracking maps.
     *
     * <p>The method performs the following validations:
     * <ul>
     *   <li>Checks if the message is in sequence (value = previous + 1)</li>
     *   <li>Detects duplicate messages (value ≤ previous)</li>
     *   <li>Identifies missing sequences (gaps between previous and value)</li>
     *   <li>Tracks out-of-order message pairs</li>
     * </ul>
     *
     * @param key The message key used for sequence tracking
     * @param value The sequence number of the message
     */
    @Override
    public synchronized void onMessage(String key, long value) {
        long seq = keySeq.getOrDefault(key, -1L);
        missedSeqs.remove(value);
        if (value - seq == 1) {
            keySeq.put(key, value);
        } else if (value <= seq) {
            dups.incrementAndGet();
        } else {
            for (long i = seq + 1; i < value; i++) {
                missedSeqs.add(i);
            }
            outOfOrders.incrementAndGet();
            outOfOrderSeqs.add(seq);
            outOfOrderSeqs.add(value);
            keySeq.put(key, value);
        }
    }

    /**
     * Closes the associated consumer and releases resources.
     *
     * @throws Exception if an error occurs while closing the consumer
     */
    @Override
    public void close() throws Exception {
        consumer.close();
    }
}
