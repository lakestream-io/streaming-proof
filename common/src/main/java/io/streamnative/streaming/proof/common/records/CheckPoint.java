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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
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
 * <p>This class supports merging of checkpoints through its {@link #add(CheckPoint)} method,
 * making it suitable for aggregating statistics from multiple sources.
 *
 * <p>The class implements {@link Cloneable} to support creating independent copies of checkpoints,
 * and uses {@link JsonInclude} to optimize JSON serialization by excluding null values.
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CheckPoint implements Cloneable {
    /** 
     * Map storing the sequence number for each key.
     * The key represents a message key, and the value represents the latest sequence number processed for that key.
     */
    private Map<String, Long> keySeq;

    /** 
     * Number of duplicate messages detected.
     * A message is considered duplicate if its sequence number is less than or equal to the last processed
     * sequence number for the same key.
     */
    private Integer duplicates;

    /** 
     * Number of errors encountered during message processing.
     * This includes any failures during message processing, such as serialization errors
     * or application-specific processing failures.
     */
    private Integer errors;

    /** 
     * Number of messages received out of order.
     * A message is considered out of order if its sequence number is greater than the expected
     * next sequence number for its key.
     */
    private Integer outOfOrders;

    /** 
     * Number of messages that were missed in the sequence.
     * This represents gaps in the sequence numbers for any key.
     */
    private Integer missed;

    /** 
     * List of sequence numbers for missed messages.
     * Contains the specific sequence numbers of messages that were expected but not received.
     */
    private List<Long> missedSeqs;

    /** 
     * List of sequence numbers for messages that failed processing.
     * Tracks the sequence numbers of messages that encountered errors during processing.
     */
    private List<Long> failedSeqs;

    /** 
     * List of sequence numbers for messages received out of order.
     * Contains pairs of sequence numbers where each pair represents [expected_seq, received_seq].
     */
    private List<Long> outOfOrderSeqs;

    public void add(CheckPoint checkPoint) {
        if (checkPoint.keySeq != null) {
            keySeq.putAll(checkPoint.keySeq);
        }
        if (checkPoint.duplicates != null) {
            duplicates += checkPoint.duplicates;
        }
        if (checkPoint.errors != null) {
            errors += checkPoint.errors;
        }
        if (checkPoint.outOfOrders != null) {
            outOfOrders += checkPoint.outOfOrders;
        }
        if (checkPoint.missed != null) {
            missed += checkPoint.missed;
        }
        if (checkPoint.failedSeqs != null) {
            failedSeqs.addAll(checkPoint.failedSeqs);
        }
        if (checkPoint.outOfOrderSeqs != null) {
            outOfOrderSeqs.addAll(checkPoint.outOfOrderSeqs);
        }
        if (checkPoint.missedSeqs != null) {
            missedSeqs.addAll(checkPoint.missedSeqs);
        }
    }

    public static CheckPoint empty() {
        return new CheckPoint(new HashMap<>(),
        0,
        0,
        0,
        0,
        new ArrayList<>(),
        new ArrayList<>(),
        new ArrayList<>());
    }

    @Override
    public CheckPoint clone() {
        return new CheckPoint(new HashMap<>(keySeq),
        duplicates,
        errors,
        outOfOrders,
        missed,
        new ArrayList<>(missedSeqs),
        new ArrayList<>(failedSeqs),
        new ArrayList<>(outOfOrderSeqs));
    }

    public CheckPoint trim() {
        if (this.duplicates == 0) {
            this.duplicates = null;
        }
        if (this.errors == 0) {
            this.errors = null;
        }
        if (this.outOfOrders == 0) {
            this.outOfOrders = null;
        }
        if (this.missed == 0) {
            this.missed = null;
        }
        if (this.failedSeqs.isEmpty()) {
            this.failedSeqs = null;
        }
        if (this.outOfOrderSeqs.isEmpty()) {
            this.outOfOrderSeqs = null;
        }
        if (this.missedSeqs.isEmpty()) {
            this.missedSeqs = null;
        }
        if (this.keySeq.isEmpty()) {
            this.keySeq = null;
        }
        return this;
    }
}
