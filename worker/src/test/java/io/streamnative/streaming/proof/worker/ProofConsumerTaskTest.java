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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import io.streamnative.streaming.proof.common.MessageMetadata;
import io.streamnative.streaming.proof.common.ProofConsumer;
import java.util.List;
import java.util.Map;
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
            consumerTask.onMessage(KEY1, 0L, createMetadata(1L));
            consumerTask.onMessage(KEY2, 0L, createMetadata(2L));
            consumerTask.onMessage(KEY3, 0L, createMetadata(3L));
            consumerTask.onMessage(KEY1, 1L, createMetadata(4L));
            consumerTask.onMessage(KEY2, 1L, createMetadata(5L));
            consumerTask.onMessage(KEY3, 1L, createMetadata(6L));

            assertEquals(consumerTask.getDups().size(), 0);
            assertEquals(consumerTask.getOutOfOrderSeqs().size(), 0);
            assertTrue(consumerTask.getMissedSeqs().isEmpty());
            assertTrue(consumerTask.getOutOfOrderSeqs().isEmpty());
        }
    }

    @Test
    public void testDuplicateMessages() throws Exception {
        try (ProofConsumerTask consumerTask = new ProofConsumerTask()) {
            consumerTask.setConsumer(mockConsumer);
            // Send duplicates to different keys
            consumerTask.onMessage(KEY1, 0L, createMetadata(1L));
            consumerTask.onMessage(KEY2, 0L, createMetadata(2L));
            consumerTask.onMessage(KEY3, 0L, createMetadata(3L));
            consumerTask.onMessage(KEY1, 0L, createMetadata(4L));
            consumerTask.onMessage(KEY2, 0L, createMetadata(5L));
            consumerTask.onMessage(KEY3, 0L, createMetadata(6L));


            assertEquals(consumerTask.getDups().values().stream().reduce(0, Integer::sum), 3);
            assertTrue(consumerTask.getMissedSeqs().isEmpty());
            assertTrue(consumerTask.getOutOfOrderSeqs().isEmpty());
        }
    }

    @Test
    public void testOutOfOrderMessages() throws Exception {
        try (ProofConsumerTask consumerTask = new ProofConsumerTask()) {
            consumerTask.setConsumer(mockConsumer);
            // Send out-of-order messages to different keys
            consumerTask.onMessage(KEY1, 0L, createMetadata(1L));
            consumerTask.onMessage(KEY2, 0L, createMetadata(2L));
            consumerTask.onMessage(KEY3, 0L, createMetadata(3L));
            consumerTask.onMessage(KEY1, 4L, createMetadata(10L));
            consumerTask.onMessage(KEY2, 3L, createMetadata(11L));
            consumerTask.onMessage(KEY3, 5L, createMetadata(12L));

            assertEquals(consumerTask.getDups().size(), 0);
            assertEquals(consumerTask.getMissedSeqs().size(), 3);
            assertEquals(consumerTask.getOutOfOrderSeqs().size(), 0);
            consumerTask.onMessage(KEY1, 0L, createMetadata(13L));
            consumerTask.onMessage(KEY2, 1L, createMetadata(14L));
            consumerTask.onMessage(KEY3, 4L, createMetadata(15L));
            assertEquals(consumerTask.getMissedSeqs().get(KEY1).size(), 0);
            assertEquals(consumerTask.getMissedSeqs().get(KEY2).size(), 0);
            assertEquals(consumerTask.getMissedSeqs().get(KEY3).size(), 1);
            assertEquals(consumerTask.getMissedSeqs().get(KEY3).getFirst().getFirst(), 1L);
            assertEquals(consumerTask.getMissedSeqs().get(KEY3).getFirst().getLast(), 3L);
            assertEquals(consumerTask.getOutOfOrderSeqs().size(), 2);
            assertEquals(consumerTask.getOutOfOrderSeqs().get(KEY1).size(), 1);
            assertEquals(consumerTask.getOutOfOrderSeqs().get(KEY1).getFirst().getFirst().seq(), 4L);
            assertEquals(consumerTask.getOutOfOrderSeqs().get(KEY1).getFirst().getLast().seq(), 0L);
            assertEquals(consumerTask.getOutOfOrderSeqs().get(KEY2).size(), 1);
            assertEquals(consumerTask.getOutOfOrderSeqs().get(KEY2).getFirst().getFirst().seq(), 3L);
            assertEquals(consumerTask.getOutOfOrderSeqs().get(KEY2).getFirst().getLast().seq(), 1L);
            assertNull(consumerTask.getOutOfOrderSeqs().get(KEY3));
            consumerTask.onMessage(KEY3, 1L, createMetadata(15L));
            consumerTask.onMessage(KEY3, 2L, createMetadata(16L));
            consumerTask.onMessage(KEY3, 3L, createMetadata(15L));
            assertEquals(consumerTask.getKeySeq().get(KEY3).seq(), 3L);
            assertNull(consumerTask.getMissedSeqs().get(KEY3));
            assertEquals(consumerTask.getOutOfOrderSeqs().get(KEY3).size(), 1);
            assertEquals(consumerTask.getOutOfOrderSeqs().get(KEY3).getFirst().getFirst().seq(), 4L);
            assertEquals(consumerTask.getOutOfOrderSeqs().get(KEY3).getFirst().getLast().seq(), 1L);
        }
    }

    @Test
    public void testMissedSequences() throws Exception {
        try (ProofConsumerTask consumerTask = new ProofConsumerTask()) {
            consumerTask.setConsumer(mockConsumer);
            // Create gaps in all three sequences
            consumerTask.onMessage(KEY1, 0L, createMetadata(1L));
            consumerTask.onMessage(KEY2, 0L, createMetadata(2L));
            consumerTask.onMessage(KEY3, 0L, createMetadata(3L));
            consumerTask.onMessage(KEY1, 5L, createMetadata(10L));
            consumerTask.onMessage(KEY2, 4L, createMetadata(11L));
            consumerTask.onMessage(KEY3, 6L, createMetadata(12L));

            Map<String, List<List<Long>>> missedSeqs = consumerTask.getMissedSeqs();
            assertEquals(missedSeqs.get(KEY1).size(), 1); // Missing 1,2,3,4
            assertEquals(missedSeqs.get(KEY1), List.of(List.of(1L, 4L)));
            assertEquals(missedSeqs.get(KEY2).size(), 1); // Missing 1,2,3
            assertEquals(missedSeqs.get(KEY2), List.of(List.of(1L, 3L)));
            assertEquals(missedSeqs.get(KEY3).size(), 1); // Missing 1,2,3,4,5
            assertEquals(missedSeqs.get(KEY3), List.of(List.of(1L, 5L)));

            consumerTask.onMessage(KEY1, 8L, createMetadata(15L));
            assertEquals(missedSeqs.get(KEY1).size(), 2); // Missing 1,2,3,4,6,7
            assertEquals(missedSeqs.get(KEY1), List.of(List.of(1L, 4L), List.of(6L, 7L)));
        }
    }

    @Test
    public void testMissedSequenceRecovery() throws Exception {
        try (ProofConsumerTask consumerTask = new ProofConsumerTask()) {
            consumerTask.setConsumer(mockConsumer);
            // Create gaps with increasing offsets
            consumerTask.onMessage(KEY1, 0L, createMetadata(1L));
            consumerTask.onMessage(KEY2, 0L, createMetadata(2L));
            consumerTask.onMessage(KEY3, 0L, createMetadata(3L));
            consumerTask.onMessage(KEY1, 4L, createMetadata(10L));
            consumerTask.onMessage(KEY2, 4L, createMetadata(11L));
            consumerTask.onMessage(KEY3, 4L, createMetadata(12L));
            
            // Recover missing sequences with later offsets
            consumerTask.onMessage(KEY1, 1L, createMetadata(12L));
            consumerTask.onMessage(KEY1, 2L, createMetadata(13L));
            consumerTask.onMessage(KEY2, 2L, createMetadata(16L));
            assertEquals(consumerTask.getMissedSeqs().size(), 2);
            assertEquals(consumerTask.getKeySeq().get(KEY1).seq(), 2L);
            assertEquals(consumerTask.getMissedSeqs().get(KEY2), List.of(List.of(1L, 1L)));
            assertEquals(consumerTask.getKeySeq().get(KEY2).seq(), 2L);
            assertEquals(consumerTask.getMissedSeqs().get(KEY3), List.of(List.of(1L, 3L)));
            assertEquals(consumerTask.getKeySeq().get(KEY3).seq(), 4L);
            consumerTask.onMessage(KEY1, 3L, createMetadata(14L));
            consumerTask.onMessage(KEY2, 1L, createMetadata(15L));
            consumerTask.onMessage(KEY2, 2L, createMetadata(19L));
            consumerTask.onMessage(KEY2, 3L, createMetadata(17L));
            consumerTask.onMessage(KEY3, 1L, createMetadata(18L));
            consumerTask.onMessage(KEY3, 2L, createMetadata(19L));
            consumerTask.onMessage(KEY3, 3L, createMetadata(20L));

            assertTrue(consumerTask.getMissedSeqs().isEmpty());
        }
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
}