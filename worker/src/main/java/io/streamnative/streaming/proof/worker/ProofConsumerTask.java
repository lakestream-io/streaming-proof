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

import io.streamnative.streaming.proof.common.LongSeq;
import io.streamnative.streaming.proof.common.MessageListener;
import io.streamnative.streaming.proof.common.MessageMetadata;
import io.streamnative.streaming.proof.common.ProofConsumer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    /** 
     * Maps message keys to their latest received message information.
     * Each entry contains both the sequence number and metadata for the most recently
     * processed message for a given key.
     *
     * <p>Key: The message key used for grouping related messages
     * <p>Value: A {@link LongSeq} containing:
     * <ul>
     *   <li>The sequence number of the latest message</li>
     *   <li>Associated metadata (e.g., message offset)</li>
     * </ul>
     *
     * <p>This map is used to:
     * <ul>
     *   <li>Track message ordering within each key's sequence</li>
     *   <li>Detect duplicate messages (when new sequence ≤ stored sequence)</li>
     *   <li>Identify gaps in message sequences (missing messages)</li>
     *   <li>Maintain message metadata for checkpointing</li>
     * </ul>
     *
     * @see LongSeq
     * @see MessageMetadata
     */
    private final Map<String, LongSeq> keySeq;

    /** Counter for duplicate messages detected */
    private final AtomicInteger dups = new AtomicInteger(0);

    /** Counter for out-of-order messages detected */
    private final AtomicInteger outOfOrders = new AtomicInteger(0);

    /** 
     * Tracks missing sequence numbers per message key.
     * Key: Message key
     * Value: List of sequence numbers that were skipped in the sequence.
     * For example, if messages arrive with sequence numbers [1,2,5], then [3,4] will be
     * recorded as missed sequences for that key.
     */
    private final Map<String, List<Long>> missedSeqs = new HashMap<>();

    /** 
     * Records out-of-order message sequences per message key.
     * Each entry tracks the message pairs that violate sequential ordering.
     *
     * <p>Key: The message key used for grouping related messages
     * <p>Value: List of message pairs, where each pair contains:
     * <ul>
     *   <li>First element: The last correctly sequenced message ({@link LongSeq})</li>
     *   <li>Second element: The out-of-order message that followed it ({@link LongSeq})</li>
     * </ul>
     *
     * <p>For example, if messages should arrive in sequence [1,2,3,4,5] but we receive
     * [1,2,5], then a pair containing messages [2,5] will be added to the list,
     * indicating that message 5 arrived immediately after 2, skipping 3 and 4.
     *
     * <p>Each {@link LongSeq} in the pair contains:
     * <ul>
     *   <li>The sequence number of the message</li>
     *   <li>Associated metadata (e.g., message offset)</li>
     * </ul>
     *
     * @see LongSeq
     * @see MessageMetadata
     */
    private final Map<String, List<List<LongSeq>>> outOfOrderSeqs = new HashMap<>();

    /**
     * Creates a new ProofConsumerTask with an empty sequence tracking map.
     */
    public ProofConsumerTask() {
        this.keySeq = new HashMap<>();
    }

    /**
     * Processes a received message and validates its sequence number against the expected order.
     * This method is synchronized to ensure thread-safe updates to the sequence tracking maps.
     *
     * <p>The method handles three scenarios:
     * <ul>
     *   <li><b>In-sequence message:</b> When value = previous + 1
     *       <br>Updates the latest sequence number and metadata for the key</li>
     *   <li><b>Duplicate message:</b> When value ≤ previous
     *       <br>Increments the duplicate counter without updating the sequence</li>
     *   <li><b>Out-of-order message:</b> When value > previous + 1
     *       <br>Records missing sequences in the gap
     *       <br>Tracks the out-of-order pair [previous message, current message]
     *       <br>Updates the latest sequence number and metadata
     *       <br>Increments the out-of-order counter</li>
     * </ul>
     *
     * <p>For each message, the method also checks if its sequence number matches any
     * previously recorded missing sequences and removes it from the missing sequences
     * list if found.
     *
     * @param key The message key used for sequence tracking and message grouping
     * @param value The sequence number of the message, used to verify ordering
     * @param metadata Additional message information such as offset or timestamp
     * @see LongSeq
     * @see MessageMetadata
     */
    @Override
    public synchronized void onMessage(String key, long value, MessageMetadata metadata) {
        LongSeq newMsg = new LongSeq(value, metadata);
        LongSeq lastMsg = keySeq.getOrDefault(key, LongSeq.empty());
        long seq = lastMsg.seq();
        missedSeqs.computeIfPresent(key, (k, list) -> {
            list.remove(value);
            return list.isEmpty() ? null : list;
        });
        if (value - seq == 1) {
            keySeq.put(key, newMsg);
        } else if (value <= seq) {
            dups.incrementAndGet();
        } else {
            for (long i = seq + 1; i < value; i++) {
                missedSeqs.computeIfAbsent(key, k -> new ArrayList<>()).add(i);
            }
            outOfOrders.incrementAndGet();
            List<List<LongSeq>> outOfOrder = outOfOrderSeqs.computeIfAbsent(key, k -> new ArrayList<>());
            List<LongSeq> pair = List.of(lastMsg, newMsg);
            outOfOrder.add(pair);
            keySeq.put(key, newMsg);
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
