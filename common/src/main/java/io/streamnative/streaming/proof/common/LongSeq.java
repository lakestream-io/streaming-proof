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

import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * A record class representing a message with a long value and associated metadata in the streaming proof system.
 * This class is used to track message sequences and their processing state throughout the system.
 *
 * <p>The record contains:
 * <ul>
 *   <li>A long value representing the message sequence number</li>
 *   <li>Metadata containing additional message information like offset</li>
 * </ul>
 *
 * <p>The class implements {@link Comparable} to enable ordering and sorting of messages
 * based on their sequence numbers.
 *
 * @param seq The sequence number or value of the message
 * @param metadata Additional message information including offset
 *
 * @see MessageMetadata
 */
public record LongSeq(long seq, MessageMetadata metadata) implements Comparable<LongSeq> {

    /** An empty message instance with sequence number -1 */
    private static final LongSeq EMPTY = new LongSeq(-1, MessageMetadata.empty());

    /**
     * Returns an empty message instance.
     * This method provides a singleton instance representing an uninitialized or
     * invalid message state.
     *
     * @return A LongMessage instance with sequence number -1 and offset -1
     */
    public static LongSeq empty() {
        return EMPTY;
    }

    /**
     * Compares this message with another message based on their sequence numbers.
     *
     * @param other The message to compare with
     * @return A negative integer, zero, or a positive integer as this message's
     *         sequence number is less than, equal to, or greater than the other's
     */
    @Override
    public int compareTo(LongSeq other) {
        return Long.compare(this.seq, other.seq);
    }

    @Override
    public String toString() {
        try {
            return Util.JSON_WRITER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
