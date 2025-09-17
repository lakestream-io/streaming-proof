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
package io.streamnative.streaming.proof.worker;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

import io.streamnative.streaming.proof.common.MessageMetadata;
import io.streamnative.streaming.proof.common.ProofConsumer;
import io.streamnative.streaming.proof.common.records.ConsumerCheckPoint;
import java.util.Map;
import java.util.SortedMap;
import org.testng.annotations.Test;

public class ProofConsumerTaskTest {

    private static final String KEY1 = "key1";
    private static final String KEY2 = "key2";
    private static final String KEY3 = "key3";

    private MessageMetadata createMetadata(long offset) {
        return new MessageMetadata(offset);
    }

    private ProofConsumer mockConsumer = mock(ProofConsumer.class);

    @Test
    public void testSequentialMessages() throws Exception {
        try (ProofConsumerTask consumerTask = new ProofConsumerTask()) {
            consumerTask.setConsumer(mockConsumer);
            // Send sequential messages to all three keys
            generateMessage(consumerTask, KEY1, 0L, createMetadata(1L));
            generateMessage(consumerTask, KEY2, 0L, createMetadata(2L));
            generateMessage(consumerTask, KEY3, 0L, createMetadata(3L));
            generateMessage(consumerTask, KEY1, 1L, createMetadata(4L));
            generateMessage(consumerTask, KEY2, 1L, createMetadata(5L));
            generateMessage(consumerTask, KEY3, 1L, createMetadata(6L));
            assertEquals(consumerTask.getConsumed().size(), 3);
            assertEquals(consumerTask.getConsumed().get(KEY1).size(), 1);
            assertEquals(consumerTask.getConsumed().get(KEY2).size(), 1);
            assertEquals(consumerTask.getConsumed().get(KEY3).size(), 1);
            assertEquals(consumerTask.getConsumed().get(KEY1).firstEntry().getValue().getStart().seq(), 0L);
            assertEquals(consumerTask.getConsumed().get(KEY1).firstEntry().getValue().getEnd().seq(), 1L);
            assertEquals(consumerTask.getConsumed().get(KEY2).firstEntry().getValue().getStart().seq(), 0L);
            assertEquals(consumerTask.getConsumed().get(KEY2).firstEntry().getValue().getEnd().seq(), 1L);
            assertEquals(consumerTask.getConsumed().get(KEY3).firstEntry().getValue().getStart().seq(), 0L);
            assertEquals(consumerTask.getConsumed().get(KEY3).firstEntry().getValue().getEnd().seq(), 1L);
        }
    }

    @Test
    public void testDuplicateMessages() throws Exception {
        try (ProofConsumerTask consumerTask = new ProofConsumerTask()) {
            consumerTask.setConsumer(mockConsumer);
            // Send duplicates to different keys
            generateMessage(consumerTask, KEY1, 0L, createMetadata(1L));
            generateMessage(consumerTask, KEY2, 0L, createMetadata(2L));
            generateMessage(consumerTask, KEY3, 0L, createMetadata(3L));
            generateMessage(consumerTask, KEY1, 0L, createMetadata(1L));
            generateMessage(consumerTask, KEY2, 0L, createMetadata(2L));
            generateMessage(consumerTask, KEY3, 0L, createMetadata(3L));

            assertEquals(consumerTask.getConsumed().size(), 3);
            assertEquals(consumerTask.getConsumed().get(KEY1).size(), 2);
            assertEquals(consumerTask.getConsumed().get(KEY2).size(), 2);
            assertEquals(consumerTask.getConsumed().get(KEY3).size(), 2);
            assertEquals(consumerTask.getConsumed().get(KEY1).firstEntry().getValue().getStart().seq(), 0L);
            assertEquals(consumerTask.getConsumed().get(KEY1).firstEntry().getValue().getEnd().seq(), 0L);
            assertEquals(consumerTask.getConsumed().get(KEY2).firstEntry().getValue().getStart().seq(), 0L);
            assertEquals(consumerTask.getConsumed().get(KEY2).firstEntry().getValue().getEnd().seq(), 0L);
            assertEquals(consumerTask.getConsumed().get(KEY3).firstEntry().getValue().getStart().seq(), 0L);
            assertEquals(consumerTask.getConsumed().get(KEY3).firstEntry().getValue().getEnd().seq(), 0L);
            assertEquals(consumerTask.getConsumed().get(KEY1).lastEntry().getValue().getStart().seq(), 0L);
            assertEquals(consumerTask.getConsumed().get(KEY1).lastEntry().getValue().getEnd().seq(), 0L);
            assertEquals(consumerTask.getConsumed().get(KEY2).lastEntry().getValue().getStart().seq(), 0L);
            assertEquals(consumerTask.getConsumed().get(KEY2).lastEntry().getValue().getEnd().seq(), 0L);
            assertEquals(consumerTask.getConsumed().get(KEY3).lastEntry().getValue().getStart().seq(), 0L);
            assertEquals(consumerTask.getConsumed().get(KEY3).lastEntry().getValue().getEnd().seq(), 0L);
        }
    }

    @Test
    public void testMissedSequences() throws Exception {
        try (ProofConsumerTask consumerTask = new ProofConsumerTask()) {
            consumerTask.setConsumer(mockConsumer);
            // Create gaps in all three sequences
            generateMessage(consumerTask, KEY1, 0L, createMetadata(1L));
            generateMessage(consumerTask, KEY2, 0L, createMetadata(2L));
            generateMessage(consumerTask, KEY3, 0L, createMetadata(3L));
            generateMessage(consumerTask, KEY1, 5L, createMetadata(10L));
            generateMessage(consumerTask, KEY2, 4L, createMetadata(11L));
            generateMessage(consumerTask, KEY3, 6L, createMetadata(12L));

            assertEquals(consumerTask.getConsumed().size(), 3);
            assertEquals(consumerTask.getConsumed().get(KEY1).size(), 2);
            assertEquals(consumerTask.getConsumed().get(KEY2).size(), 2);
            assertEquals(consumerTask.getConsumed().get(KEY3).size(), 2);
            assertEquals(consumerTask.getConsumed().get(KEY1).firstEntry().getValue().getStart().seq(), 0L);
            assertEquals(consumerTask.getConsumed().get(KEY1).firstEntry().getValue().getEnd().seq(), 0L);
            assertEquals(consumerTask.getConsumed().get(KEY2).firstEntry().getValue().getStart().seq(), 0L);
            assertEquals(consumerTask.getConsumed().get(KEY2).firstEntry().getValue().getEnd().seq(), 0L);
            assertEquals(consumerTask.getConsumed().get(KEY3).firstEntry().getValue().getStart().seq(), 0L);
            assertEquals(consumerTask.getConsumed().get(KEY3).firstEntry().getValue().getEnd().seq(), 0L);
            assertEquals(consumerTask.getConsumed().get(KEY1).lastEntry().getValue().getStart().seq(), 5L);
            assertEquals(consumerTask.getConsumed().get(KEY1).lastEntry().getValue().getEnd().seq(), 5L);
            assertEquals(consumerTask.getConsumed().get(KEY2).lastEntry().getValue().getStart().seq(), 4L);
            assertEquals(consumerTask.getConsumed().get(KEY2).lastEntry().getValue().getEnd().seq(), 4L);
            assertEquals(consumerTask.getConsumed().get(KEY3).lastEntry().getValue().getStart().seq(), 6L);
            assertEquals(consumerTask.getConsumed().get(KEY3).lastEntry().getValue().getEnd().seq(), 6L);
        }
    }

    @Test
    public void testGetTrimmedConsumed() throws Exception {
        try (ProofConsumerTask consumerTask = new ProofConsumerTask()) {
            consumerTask.setConsumer(mockConsumer);
            
            // Generate messages with overlapping and non-overlapping ranges
            generateMessage(consumerTask, KEY1, 0L, createMetadata(0L));
            generateMessage(consumerTask, KEY1, 1L, createMetadata(1L));
            generateMessage(consumerTask, KEY1, 5L, createMetadata(5L));
            generateMessage(consumerTask, KEY1, 6L, createMetadata(6L));
            
            generateMessage(consumerTask, KEY2, 0L, createMetadata(0L));
            generateMessage(consumerTask, KEY2, 1L, createMetadata(1L));
            generateMessage(consumerTask, KEY2, 2L, createMetadata(2L));
            
            generateMessage(consumerTask, KEY2, 1L, createMetadata(1L));
            
            // Get merged consumed ranges
            Map<String, SortedMap<String, ConsumerCheckPoint.SeqRange>> merged = consumerTask.getTrimmedConsumed();
            
            // Verify results
            assertEquals(merged.size(), 2);
            
            // For KEY1, we should have two ranges: [0-1] and [5-6]
            assertEquals(merged.get(KEY1).size(), 2);
            
            // For KEY2, we should have 1 ranges: [0-2]
            assertEquals(merged.get(KEY2).size(), 1);

            // Verify the first range for KEY1
            ConsumerCheckPoint.SeqRange firstRangeKey1 = merged.get(KEY1).firstEntry().getValue();
            assertEquals(firstRangeKey1.getStart().seq(), 0L);
            assertEquals(firstRangeKey1.getEnd().seq(), 1L);
            
            // Verify the second range for KEY1
            ConsumerCheckPoint.SeqRange secondRangeKey1 = merged.get(KEY1).lastEntry().getValue();
            assertEquals(secondRangeKey1.getStart().seq(), 5L);
            assertEquals(secondRangeKey1.getEnd().seq(), 6L);
            
            // Verify the first range for KEY2
            ConsumerCheckPoint.SeqRange firstRangeKey2 = merged.get(KEY2).firstEntry().getValue();
            assertEquals(firstRangeKey2.getStart().seq(), 0L);
            assertEquals(firstRangeKey2.getEnd().seq(), 2L);
            assertEquals(firstRangeKey2.getDuplicated(), 1);

            generateMessage(consumerTask, KEY2, 1L, createMetadata(1L));
            generateMessage(consumerTask, KEY2, 2L, createMetadata(2L));
            generateMessage(consumerTask, KEY2, 3L, createMetadata(3L));
            generateMessage(consumerTask, KEY2, 4L, createMetadata(4L));
            merged = consumerTask.getTrimmedConsumed();
            assertEquals(merged.get(KEY2).size(), 1);
            firstRangeKey2 = merged.get(KEY2).firstEntry().getValue();
            assertEquals(firstRangeKey2.getStart().seq(), 0L);
            assertEquals(firstRangeKey2.getEnd().seq(), 4L);
            assertEquals(firstRangeKey2.getDuplicated(), 3);
        }
    }

    private void generateMessage(ProofConsumerTask task, String key, long seq,
                                 MessageMetadata metadata) throws InterruptedException {
        Thread.sleep(10);
        task.onMessage(key, seq, metadata);
    }

    @Test
    public void testClose() throws Exception {
        ProofConsumer mockConsumer = mock(ProofConsumer.class);
        try (ProofConsumerTask consumerTask = new ProofConsumerTask()) {
            consumerTask.setConsumer(mockConsumer);
            consumerTask.close();
            verify(mockConsumer).close();
        }
    }

    @Test
    public void testMultipleMessagesPerKey() throws Exception {
        try (ProofConsumerTask consumerTask = new ProofConsumerTask()) {
            consumerTask.setConsumer(mockConsumer);
            
            // Send multiple sequential messages for each key
            generateMessage(consumerTask, KEY1, 0L, createMetadata(1L));
            generateMessage(consumerTask, KEY1, 1L, createMetadata(2L));
            generateMessage(consumerTask, KEY1, 2L, createMetadata(3L));
            
            generateMessage(consumerTask, KEY2, 0L, createMetadata(4L));
            generateMessage(consumerTask, KEY2, 1L, createMetadata(5L));
            generateMessage(consumerTask, KEY2, 2L, createMetadata(6L));
            
            generateMessage(consumerTask, KEY3, 0L, createMetadata(7L));
            generateMessage(consumerTask, KEY3, 1L, createMetadata(8L));
            generateMessage(consumerTask, KEY3, 2L, createMetadata(9L));
            
            // Verify basic sequential processing
            assertEquals(consumerTask.getConsumed().size(), 3);
            assertEquals(consumerTask.getConsumed().get(KEY1).size(), 1);
            assertEquals(consumerTask.getConsumed().get(KEY2).size(), 1);
            assertEquals(consumerTask.getConsumed().get(KEY3).size(), 1);
            
            // Verify ranges for each key
            assertEquals(consumerTask.getConsumed().get(KEY1).firstEntry().getValue().getStart().seq(), 0L);
            assertEquals(consumerTask.getConsumed().get(KEY1).firstEntry().getValue().getEnd().seq(), 2L);
            assertEquals(consumerTask.getConsumed().get(KEY2).firstEntry().getValue().getStart().seq(), 0L);
            assertEquals(consumerTask.getConsumed().get(KEY2).firstEntry().getValue().getEnd().seq(), 2L);
            assertEquals(consumerTask.getConsumed().get(KEY3).firstEntry().getValue().getStart().seq(), 0L);
            assertEquals(consumerTask.getConsumed().get(KEY3).firstEntry().getValue().getEnd().seq(), 2L);
            
            // Now add some producer duplicates (same sequence but higher metadata)
            MessageMetadata higherMetadata1 = createMetadata(10L);
            MessageMetadata higherMetadata2 = createMetadata(11L);
            
            generateMessage(consumerTask, KEY1, 1L, higherMetadata1); // Producer duplicate
            generateMessage(consumerTask, KEY2, 1L, higherMetadata2); // Producer duplicate
            
            // Verify producer duplicates are tracked
            assertEquals(consumerTask.getWriteDupsOrOutOrder().size(), 2);
            assertEquals(consumerTask.getWriteDupsOrOutOrder().get(KEY1).size(), 1);
            assertEquals(consumerTask.getWriteDupsOrOutOrder().get(KEY2).size(), 1);
            
            // Add gaps to create multiple ranges
            generateMessage(consumerTask, KEY1, 5L, createMetadata(12L)); // Gap after 2
            generateMessage(consumerTask, KEY2, 5L, createMetadata(13L)); // Gap after 2
            generateMessage(consumerTask, KEY3, 5L, createMetadata(14L)); // Gap after 2
            
            // Verify multiple ranges
            assertEquals(consumerTask.getConsumed().get(KEY1).size(), 3);
            assertEquals(consumerTask.getConsumed().get(KEY2).size(), 3);
            assertEquals(consumerTask.getConsumed().get(KEY3).size(), 2);
            
            // Verify the second range for each key
            assertEquals(consumerTask.getConsumed().get(KEY1).lastEntry().getValue().getStart().seq(), 5L);
            assertEquals(consumerTask.getConsumed().get(KEY1).lastEntry().getValue().getEnd().seq(), 5L);
            assertEquals(consumerTask.getConsumed().get(KEY2).lastEntry().getValue().getStart().seq(), 5L);
            assertEquals(consumerTask.getConsumed().get(KEY2).lastEntry().getValue().getEnd().seq(), 5L);
            assertEquals(consumerTask.getConsumed().get(KEY3).lastEntry().getValue().getStart().seq(), 5L);
            assertEquals(consumerTask.getConsumed().get(KEY3).lastEntry().getValue().getEnd().seq(), 5L);
            
            // Test trimmed consumed with multiple ranges
            Map<String, SortedMap<String, ConsumerCheckPoint.SeqRange>> merged = consumerTask.getTrimmedConsumed();
            
            // Verify trimmed results
            assertEquals(merged.size(), 3);
            assertEquals(merged.get(KEY1).size(), 2); // Two separate ranges
            assertEquals(merged.get(KEY2).size(), 2); // Two separate ranges
            assertEquals(merged.get(KEY3).size(), 2); // Two separate ranges
            
            // Fill the gaps to create continuous ranges
            generateMessage(consumerTask, KEY1, 3L, createMetadata(15L));
            generateMessage(consumerTask, KEY1, 4L, createMetadata(16L));
            
            // Verify ranges are merged in trimmed result
            merged = consumerTask.getTrimmedConsumed();

            assertEquals(merged.get(KEY1).size(), 3); // Now one continuous range
            assertEquals(merged.get(KEY1).firstEntry().getValue().getStart().seq(), 0L);
            assertEquals(merged.get(KEY1).firstEntry().getValue().getEnd().seq(), 2L);
            assertEquals(merged.get(KEY1).lastEntry().getValue().getStart().seq(), 3L);
            assertEquals(merged.get(KEY1).lastEntry().getValue().getEnd().seq(), 4L);
            
            // Verify the producer duplicates or out-of-order seqs are still tracked
            Map<String, SortedMap<String, ConsumerCheckPoint.SeqRange>> writeDupsOrOutOrder = 
                consumerTask.getWriteDupsOrOutOrder();
            assertEquals(writeDupsOrOutOrder.get(KEY1).size(), 2);
            assertEquals(writeDupsOrOutOrder.get(KEY1).firstEntry().getValue().getStart().seq(), 1L);
            assertEquals(writeDupsOrOutOrder.get(KEY1).firstEntry().getValue().getEnd().seq(), 1L);
            assertEquals(writeDupsOrOutOrder.get(KEY1).lastEntry().getValue().getStart().seq(), 3L);
            assertEquals(writeDupsOrOutOrder.get(KEY1).lastEntry().getValue().getEnd().seq(), 3L);

            // Verify the message redeliveries to the consumer
            generateMessage(consumerTask, KEY1, 5L, createMetadata(12L));
            generateMessage(consumerTask, KEY1, 6L, createMetadata(13L));
            merged = consumerTask.getTrimmedConsumed();
            assertEquals(merged.get(KEY1).size(), 2);
            assertEquals(merged.get(KEY1).lastEntry().getValue().getStart().seq(), 3L);
            assertEquals(merged.get(KEY1).lastEntry().getValue().getEnd().seq(), 6L);
            assertEquals(merged.get(KEY1).lastEntry().getValue().getDuplicated(), 1);
        }
    }
}