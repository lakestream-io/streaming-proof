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
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
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
    private final Map<String, LongSeq> keySeq = new HashMap<>();

    /** Counter for duplicate messages detected */
    @Getter
    private final Map<String, Integer> dups = new HashMap<>();

    /** 
     * Tracks ranges of missing sequence numbers per message key.
     * 
     * <p>Key: Message key used for grouping related messages
     * <p>Value: List of sequence number ranges, where each range is represented as a list
     * containing [start, end] inclusive. Multiple non-contiguous ranges are stored as
     * separate entries in the list.
     *
     * <p>Examples:
     * <ul>
     *   <li>If messages arrive as [1,2,5]:
     *       <br>missedSeqs["key"] = [[3,4]]</li>
     *   <li>If messages arrive as [1,5,10]:
     *       <br>missedSeqs["key"] = [[2,4], [6,9]]</li>
     *   <li>When message 3 arrives for previous gap [3,4]:
     *       <br>missedSeqs["key"] = [[4,4]]</li>
     *   <li>When message 4 arrives:
     *       <br>missedSeqs["key"] is removed (range becomes empty)</li>
     * </ul>
     *
     * <p>The ranges are maintained in sorted order and automatically merged or split
     * when new messages arrive that fill parts of existing gaps.
     *
     * @see #updateMissedSequences(String, long)
     * @see #handleMissedSeqs(String, LongSeq, LongSeq)
     */
    private final Map<String, List<List<Long>>> missedSeqs = new HashMap<>();

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
    
        // Update missed sequences if this message fills a gap
        updateMissedSequences(key, value);
    
       if (value <= seq) {
            handleDuplicateMessage(key, lastMsg, newMsg);
        } else if (value - seq > 1) {
            handleMissedSeqs(key, newMsg, lastMsg);
        }
        // Update latest sequence
        keySeq.put(key, newMsg);
    }

    public Map<String, LongSeq> getKeySeq() {
        return new HashMap<>(keySeq);
    }

    public Map<String, List<List<Long>>> getMissedSeqs() {
        return new HashMap<>(missedSeqs);
    }

    public Map<String, List<List<LongSeq>>> getOutOfOrderSeqs() {
        return new HashMap<>(outOfOrderSeqs);
    }

    private void updateMissedSequences(String key, long value) {
        missedSeqs.computeIfPresent(key, (k, ranges) -> {
            List<List<Long>> newRanges = new ArrayList<>();

            for (List<Long> range : ranges) {
                if (!isValueInRange(value, range)) {
                    newRanges.add(range);
                    continue;
                }
                range = new ArrayList<>(range);
                if (value == range.get(0)) {
                    range.set(0, value + 1);
                    if (range.get(0) <= range.get(1)) {
                        newRanges.add(range);
                    }
                } else if (value == range.get(1)) {
                    range.set(1, value - 1);
                    if (range.get(0) <= range.get(1)) {
                        newRanges.add(range);
                    }
                } else {
                    splitRange(range, value, newRanges);
                }
            }
            newRanges.sort(this::compareRanges);
            return newRanges.isEmpty() ? null : newRanges;
        });
    }

    /**
     * Splits a range of missing sequence numbers when a message arrives that falls within the range.
     * The original range is split into two new ranges: one before and one after the received value.
     * Only valid ranges (where start ≤ end) are added to the result list.
     *
     * <p>For example:
     * <pre>
     * Original range: [2,6]
     * Received value: 4
     * Result ranges: [2,3], [5,6]
     * </pre>
     *
     * <p>Edge cases:
     * <ul>
     *   <li>If value - 1 < range start: only right range is added</li>
     *   <li>If value + 1 > range end: only left range is added</li>
     *   <li>If resulting range has start > end: range is discarded</li>
     * </ul>
     *
     * @param range The original range of missing sequence numbers [start, end]
     * @param value The sequence number that splits the range
     * @param newRanges List to store the resulting valid ranges
     * @see #updateMissedSequences(String, long)
     */
    private void splitRange(List<Long> range, long value, List<List<Long>> newRanges) {
        List<Long> left = new ArrayList<>(2);
        left.add(range.get(0));
        left.add(value - 1);
        
        List<Long> right = new ArrayList<>(2);
        right.add(value + 1);
        right.add(range.get(1));
        
        if (left.get(0) <= left.get(1)) {
            newRanges.add(left);
        }
        if (right.get(0) <= right.get(1)) {
            newRanges.add(right);
        }
    }

    private boolean isValueInRange(long value, List<Long> range) {
        return value >= range.get(0) && value <= range.get(1);
    }

    private int compareRanges(List<Long> a, List<Long> b) {
        long diff = a.get(0) - b.get(0);
        return diff == 0 ? (int) (a.get(1) - b.get(1)) : (int) diff;
    }

    private void handleDuplicateMessage(String key, LongSeq lastMsg, LongSeq newMsg) {
        int dupCount = (int) (lastMsg.seq() - newMsg.seq() + 1);
        dups.compute(key, (k, v) -> v == null ? dupCount : v + dupCount);
        List<List<Long>> missedRanges = missedSeqs.get(key);
        if (missedRanges != null) {
            boolean removed = missedRanges.removeIf(range -> range.getFirst() >= newMsg.seq());
            if (removed) {
                outOfOrderSeqs.computeIfAbsent(key, k -> new ArrayList<>()).add(List.of(lastMsg, newMsg));
            } else {
                for (List<Long> missedRange : missedRanges) {
                    if (isValueInRange(newMsg.seq(), missedRange)) {
                        outOfOrderSeqs.computeIfAbsent(key, k -> new ArrayList<>()).add(List.of(lastMsg, newMsg));
                        break;
                    }
                }
            }
        }
    }

    public String getConsumerName() {
        return consumer.name();
    }

    private void handleMissedSeqs(String key, LongSeq newMsg, LongSeq lastMsg) {
        List<Long> range = List.of(lastMsg.seq() + 1, newMsg.seq() - 1);
        missedSeqs.computeIfAbsent(key, k -> new ArrayList<>()).add(range);
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
