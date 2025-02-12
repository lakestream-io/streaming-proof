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
package io.streamnative.streaming.proof.common.records;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.streamnative.streaming.proof.common.LongSeq;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * CheckPoint class represents a checkpoint for tracking message processing statistics and sequence information
 * in a streaming system. This class is essential for monitoring message delivery guarantees and debugging
 * message processing issues.
 *
 * <p>The checkpoint tracks various aspects of message processing:
 * <ul>
 *   <li>Message sequence numbers for each key</li>
 *   <li>Duplicate message detection</li>
 *   <li>Processing errors</li>
 *   <li>Out-of-order message delivery</li>
 *   <li>Missing messages in sequences</li>
 * </ul>
 *
 * <p>This class supports merging of checkpoints through its {@link #merge(Checkpoint)} method,
 * making it suitable for aggregating statistics from multiple sources.
 *
 * <p>The class implements {@link Cloneable} to support creating independent copies of checkpoints,
 * and uses {@link JsonInclude} to optimize JSON serialization by excluding null values.
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Checkpoint implements Cloneable {
    /** 
     * Map storing the sequence number for each key.
     * The key represents a message key, and the value represents the latest sequence number processed for that key.
     */
    private Map<String, LongSeq> keys;

    /**
     * Map tracking the number of duplicate messages for each key.
     * The key represents a message key, and the value represents the count of duplicate messages.
     */
    private Map<String, Integer> duplicates;

    /** 
     * Number of errors encountered during message processing.
     * This includes any failures during message processing, such as serialization errors
     * or application-specific processing failures. Error message is the key and the value is the count.
     */
    private Map<String, Integer> errors;

    /** 
     * List of sequence numbers for missed messages.
     * Contains the specific sequence numbers of messages that were expected but not received.
     */
    /** 
     * Tracks missed messages per key in the message sequence.
     * Key: Message key
     * Value: List of sequence numbers that were expected but not received in the sequence.
     * For example, if we expect messages 1,2,3,4,5 but receive 1,2,4,5, then 3 will be in this list.
     */
    private Map<String, List<List<Long>>> missedSeqs;

    /** 
     * Stores out-of-order message sequences per key.
     * Key: Message key
     * Value: List of sequence number pairs where each consecutive pair represents
     * [expected_seq, actual_received_seq]. For example, if we expect sequence 5
     * but receive 7, the pair will be [5,7].
     */
    private Map<String, List<List<LongSeq>>> outOfOrderSeqs;

    /**
     * Merges another checkpoint's data into this checkpoint, combining sequence information,
     * errors, and statistics to present the latest status of the messag keys.
     *
     * The method is used to aggregate the statistics of multiple message keys and merge
     * the statistics of the same key from different checkpoints due to the active consumer
     * of partitions might be changed.
     *
     * @param checkPoint The checkpoint to merge into this one. If null, no changes are made.
     *                   Fields within the checkpoint may be null, in which case those aspects
     *                   are skipped during merging.
     */
    public void merge(Checkpoint checkPoint) {
        if (checkPoint.getMissedSeqs() != null && !checkPoint.getMissedSeqs().isEmpty()) {
            checkPoint.getMissedSeqs().forEach((k, v) -> {
                LongSeq existSeq = keys.get(k);
                LongSeq newSeq = checkPoint.getKeys().get(k);
                if (existSeq != null) {
                    missedSeqs.compute(k, (key, value) -> {
                        List<List<Long>> missed = mergeMissedSeqs(existSeq.seq(), value, newSeq.seq(), v);
                        return missed.isEmpty() ? null : missed;
                    });
                } else {
                    missedSeqs.put(k, new ArrayList<>(v));
                }
            });
        }
        if (missedSeqs != null) {
            new HashMap<>(missedSeqs).forEach((k, v) -> {
                LongSeq existSeq = keys.get(k);
                LongSeq newSeq = checkPoint.getKeys().get(k);
                if (existSeq != null && newSeq != null) {
                    missedSeqs.compute(k, (key, value) -> {
                        List<List<Long>> missed = mergeMissedSeqs(existSeq.seq(), value, newSeq.seq(),
                                checkPoint.getMissedSeqs().get(k));
                        return missed.isEmpty() ? null : missed;
                    });
                } else {
                    missedSeqs.put(k, new ArrayList<>(v));
                }
            });
        }

        if (checkPoint.getOutOfOrderSeqs() != null) {
            checkPoint.getOutOfOrderSeqs().forEach((k, v) ->
                    outOfOrderSeqs.computeIfAbsent(k, key -> new ArrayList<>()).addAll(v));
        }

        if (checkPoint.getKeys() != null) {
            keys.entrySet().removeIf(entry ->
                checkPoint.keys.containsKey(entry.getKey())
                    && entry.getValue().compareTo(checkPoint.keys.get(entry.getKey())) < 0);

            if (checkPoint.duplicates != null) {
                duplicates.keySet().removeIf(k -> !keys.containsKey(k));
                checkPoint.duplicates.forEach((k, v) -> duplicates.putIfAbsent(k, v));
            }
            checkPoint.keys.forEach((k, v) -> keys.putIfAbsent(k, v));
        }

        if (checkPoint.errors != null) {
            checkPoint.errors.forEach((k, v) -> errors.merge(k, v, Integer::sum));
        }
    }

    public static Checkpoint empty() {
        return new Checkpoint(new HashMap<>(),
        new HashMap<>(),
        new HashMap<>(),
        new HashMap<>(),
        new HashMap<>());
    }

    @Override
    public Checkpoint clone() {
        return new Checkpoint(new HashMap<>(keys),
            new HashMap<>(duplicates),
            new HashMap<>(errors),
            new HashMap<>(missedSeqs),
            new HashMap<>(outOfOrderSeqs));
    }

    public Checkpoint trim() {
        if (this.duplicates != null && this.duplicates.isEmpty()) {
            this.duplicates = null;
        }
        if (this.errors != null && this.errors.isEmpty()) {
            this.errors = null;
        }
        if (this.outOfOrderSeqs != null && this.outOfOrderSeqs.isEmpty()) {
            this.outOfOrderSeqs = null;
        }
        if (this.missedSeqs != null && this.missedSeqs.isEmpty()) {
            this.missedSeqs = null;
        }
        if (this.keys != null && this.keys.isEmpty()) {
            this.keys = null;
        }
        return this;
    }

    /**
     * Merges two sets of missed sequence ranges while maintaining the latest missed sequence.
     * This method handles the combination of missed sequence ranges from different checkpoints,
     * ensuring that only valid missing sequences are preserved.
     *
     * <p>For example:
     * <pre>
     * Example 1:
     * Left checkpoint: seq=5, ranges=[[1,3]]
     *      means the latest received seq is 5 and missed seqs are 1 to 3
     * Right checkpoint: seq=7, ranges=[[0,6]]
     *      means the latest received seq is 7 and missed seqs are 0 to 6
     * Result: seq=7, ranges=[[1,3],[6,6]]
     *      means the latest received seq is 7 and the missed seqs are 1 to 3 and 6
     * </pre>
     *
     * <pre>
     * Example 2:
     * Left checkpoint: seq=7, ranges=[[1,3],[6,6]]
     *      means the latest received seq is 7 and missed seqs are 1 to 3 and 6
     * Right checkpoint: seq=8, ranges=[[0,7]]
     *      means the latest received seq is 8 and missed seqs are 0 to 7
     * Result: seq=8, ranges=[[1,3],[6,6]]
     * </pre>
     *
     * <pre>
     * Example 3:
     * Left checkpoint: seq=7, ranges=[[0,6]]
     *      means the latest received seq is 7 and missed seqs are 0 to 6
     * Right checkpoint: seq=5, ranges=[[1,3]]
     *      means the latest received seq is 5 and missed seqs are 1 to 3
     * Result: seq=7, ranges=[[1,3],[6,6]]
     * </pre>
     *
     * @param leftSeq the latest sequence number from the left checkpoint
     * @param left the list of missed sequence ranges from the left checkpoint
     * @param rightSeq the latest sequence number from the right checkpoint
     * @param right the list of missed sequence ranges from the right checkpoint
     * @return merged list of missed sequence ranges, with invalid ranges filtered out
     */
    private List<List<Long>> mergeMissedSeqs(long leftSeq, List<List<Long>> left,
                                              long rightSeq, List<List<Long>> right) {
        List<List<Long>> noMissed = new ArrayList<>();
        noMissed.addAll(getNoMissedRanges(leftSeq, left));
        noMissed.addAll(getNoMissedRanges(rightSeq, right));
        noMissed.sort(Comparator.comparingLong(List::getFirst));
        List<List<Long>> mergedNoMissed = new ArrayList<>();
        for (int i = 0; i < noMissed.size(); i++) {
            if (i == 0) {
                mergedNoMissed.add(noMissed.get(i));
            } else {
                List<Long> previous = mergedNoMissed.getLast();
                List<Long> now = noMissed.get(i);
                if (now.getFirst() > previous.getLast() + 1) {
                    mergedNoMissed.add(now);
                } else {
                    mergedNoMissed.set(mergedNoMissed.size() - 1,
                            List.of(previous.getFirst(), Math.max(previous.getLast(), now.getLast())));
                }
            }
        }
        List<List<Long>> result = new ArrayList<>();
        for (int i = 0; i < mergedNoMissed.size(); i++) {
            if (i == 0) {
                if (mergedNoMissed.get(i).getFirst() > 0) {
                    result.add(List.of(0L, mergedNoMissed.get(i).getFirst() - 1));
                }
            } else  {
                List<Long> previous = mergedNoMissed.get(i - 1);
                List<Long> now = mergedNoMissed.get(i);
                if (now.getFirst() > previous.getLast() + 1) {
                    result.add(List.of(previous.getLast() + 1, now.getFirst() - 1));
                }
            }
            if (i == mergedNoMissed.size() - 1 && mergedNoMissed.get(i).getLast() < rightSeq) {
                result.add(List.of(mergedNoMissed.get(i).getLast() + 1, rightSeq));
            }
        }
        return result;
    }

    private List<List<Long>> getNoMissedRanges(long latestSeq, List<List<Long>> missedSeqs) {
        if (missedSeqs == null || missedSeqs.isEmpty()) {
            return List.of(List.of(0L, latestSeq));
        }
        missedSeqs.sort(Comparator.comparingLong(List::getFirst));
        List<List<Long>> noMissed = new ArrayList<>();
        for (int i = 0; i < missedSeqs.size(); i++) {
            if (i == 0) {
                if (missedSeqs.get(i).getFirst() > 0) {
                    noMissed.add(List.of(0L, missedSeqs.get(i).getFirst() - 1));
                }
            } else  {
                List<Long> previous = missedSeqs.get(i - 1);
                List<Long> now = missedSeqs.get(i);
                if (now.getFirst() > previous.getLast() + 1) {
                    noMissed.add(List.of(previous.getLast() + 1, now.getFirst() - 1));
                }
            }
            if (i == missedSeqs.size() - 1 && missedSeqs.get(i).getLast() < latestSeq) {
                noMissed.add(List.of(missedSeqs.get(i).getLast() + 1, latestSeq));
            }
        }
        return noMissed;
    }
}
