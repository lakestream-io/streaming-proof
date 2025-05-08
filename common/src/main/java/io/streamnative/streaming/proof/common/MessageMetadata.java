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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * An immutable record that encapsulates message metadata for verification tracking.
 * 
 * <p>MessageMetadata captures system-specific positioning information that enables:
 * <ul>
 *   <li>Tracking message delivery order across distributed systems</li>
 *   <li>Comparing message positions to detect reordering</li>
 *   <li>Identifying message sources through partition information</li>
 *   <li>Supporting both Kafka-style offsets and Pulsar-style ledger/entry IDs</li>
 * </ul>
 *
 * <p>The metadata structure is designed to be flexible across different messaging systems:
 * <ul>
 *   <li>For Kafka: primarily uses offset and partition fields</li>
 *   <li>For Pulsar: primarily uses ledgerId and entryId fields</li>
 *   <li>For other systems: can be extended with additional fields as needed</li>
 * </ul>
 *
 * <p>The {@link #isAfter(MessageMetadata)} method provides a system-agnostic way to
 * compare message positions, which is essential for verifying ordering guarantees.
 *
 * @param offset The position of the message in a linear log (Kafka-style).
 *               A value of -1 indicates an invalid or uninitialized offset.
 * @param ledgerId The ID of the ledger containing the message (Pulsar-style).
 *                 A value of -1 indicates this field is not applicable.
 * @param entryId The position of the message within its ledger (Pulsar-style).
 *                A value of -1 indicates this field is not applicable.
 * @param partition The partition ID where the message was stored.
 *                  May be null if not applicable to the messaging system.
 *
 * @see ProofProducer#sendAsync(String, long)
 * @see MessageListener#onMessage(String, long, MessageMetadata)
 * @see io.streamnative.streaming.proof.common.records.ConsumerCheckPoint
 */
public record MessageMetadata(
        @JsonInclude(Include.NON_NULL)
        Long offset,
        
        @JsonInclude(Include.NON_NULL)
        Long ledgerId,
        
        @JsonInclude(Include.NON_NULL)
        Long entryId,

        @JsonInclude(Include.NON_NULL)
        Integer partition
) {

    public static MessageMetadata empty() {
        return new MessageMetadata(-1L, -1L, -1L, null);
    }
    public MessageMetadata(long offset) {
        this(offset, null, null, null);
    }

    public MessageMetadata(long offset, int partition) {
        this(offset, null, null, partition);
    }

    public MessageMetadata(long ledgerId, long entryId) {
        this(null, ledgerId, entryId, null);
    }

    public boolean isAfter(MessageMetadata other) {
        if (other == null) {
            return true;
        }
        
        // If both have ledger and entry IDs, compare them
        if (this.ledgerId != null && this.entryId != null
                && other.ledgerId != null && other.entryId != null) {
            return this.ledgerId > other.ledgerId
                    || (this.ledgerId.equals(other.ledgerId) && this.entryId > other.entryId);
        }
        
        // Otherwise compare by offset
        return this.offset > other.offset;
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
