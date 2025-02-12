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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import io.streamnative.streaming.proof.common.LongSeq;
import io.streamnative.streaming.proof.common.MessageMetadata;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CheckpointTest {
    
    private static final String TEST_KEY = "testKey";
    private Checkpoint checkpoint;

    @BeforeMethod
    public void setup() {
        checkpoint = Checkpoint.empty();
    }

    @Test
    public void testMergeWithNullFields() {
        Checkpoint other = new Checkpoint();
        checkpoint.merge(other);
        assertTrue(checkpoint.getKeys().isEmpty());
        assertTrue(checkpoint.getDuplicates().isEmpty());
        assertTrue(checkpoint.getErrors().isEmpty());
        assertTrue(checkpoint.getMissedSeqs().isEmpty());
        assertTrue(checkpoint.getOutOfOrderSeqs().isEmpty());
    }

    @Test
    public void testMergeWithKeys() {
        // Setup initial state
        LongSeq seq1 = new LongSeq(1L, new MessageMetadata(1L));
        LongSeq seq2 = new LongSeq(2L, new MessageMetadata(2L));
        checkpoint.getKeys().put(TEST_KEY, seq1);

        // Create checkpoint to add
        Checkpoint other = Checkpoint.empty();
        other.getKeys().put(TEST_KEY, seq2);

        // Test adding checkpoint with higher sequence
        checkpoint.merge(other);
        assertEquals(checkpoint.getKeys().get(TEST_KEY), seq2);
    }

    @Test
    public void testMergeDuplicates() {
        // Setup initial duplicates
        checkpoint.getDuplicates().put(TEST_KEY, 2);
        checkpoint.getKeys().put(TEST_KEY, new LongSeq(2, new MessageMetadata(2L)));

        // Create checkpoint with duplicates to merge
        Checkpoint other = Checkpoint.empty();
        other.getDuplicates().put(TEST_KEY, 3);
        other.getKeys().put(TEST_KEY, new LongSeq(3, new MessageMetadata(3L)));

        // Test merging duplicates
        checkpoint.merge(other);
        assertEquals(checkpoint.getDuplicates().get(TEST_KEY), Integer.valueOf(3));

        other = Checkpoint.empty();
        other.getDuplicates().put(TEST_KEY, 6);
        other.getKeys().put(TEST_KEY, new LongSeq(2, new MessageMetadata(2L)));

        checkpoint.merge(other);
        assertEquals(checkpoint.getDuplicates().get(TEST_KEY), Integer.valueOf(3));
    }

    @Test
    public void testMergeMissedSequences() {
        checkpoint.getKeys().put(TEST_KEY, new LongSeq(9, new MessageMetadata(9L)));

        // 9->[] merge 10->[[0, 9]] = 10->[]
        Checkpoint other = Checkpoint.empty();
        List<List<Long>> missedSeqs = new ArrayList<>();
        missedSeqs.add(List.of(0L, 9L));
        other.getMissedSeqs().put(TEST_KEY, missedSeqs);
        other.getKeys().put(TEST_KEY, new LongSeq(10, new MessageMetadata(10L)));
        other.merge(checkpoint.clone());
        checkpoint.merge(other.clone());
        assertNull(checkpoint.getMissedSeqs().get(TEST_KEY));
        assertNull(other.getMissedSeqs().get(TEST_KEY));

        // 10->[] merge 12->[[0, 11]] = 12->[[11, 11]]
        other = Checkpoint.empty();
        missedSeqs = new ArrayList<>();
        missedSeqs.add(List.of(0L, 11L));
        other.getMissedSeqs().put(TEST_KEY, missedSeqs);
        other.getKeys().put(TEST_KEY, new LongSeq(12, new MessageMetadata(12L)));
        checkpoint.merge(other);
        assertEquals(checkpoint.getMissedSeqs().get(TEST_KEY).size(), 1);
        assertEquals(checkpoint.getMissedSeqs().get(TEST_KEY).getFirst(), List.of(11L, 11L));

        // 12->[[11, 11]] merge 14->[[0, 13]] = 14->[[11, 11], [13, 13]]
        other = Checkpoint.empty();
        missedSeqs = new ArrayList<>();
        missedSeqs.add(List.of(0L, 13L));
        other.getMissedSeqs().put(TEST_KEY, missedSeqs);
        other.getKeys().put(TEST_KEY, new LongSeq(14, new MessageMetadata(14L)));
        checkpoint.merge(other);
        assertEquals(checkpoint.getMissedSeqs().get(TEST_KEY).size(), 2);
        assertEquals(checkpoint.getMissedSeqs().get(TEST_KEY).getFirst(), List.of(11L, 11L));
        assertEquals(checkpoint.getMissedSeqs().get(TEST_KEY).getLast(), List.of(13L, 13L));

        // 14->[[11, 11], [13, 13]] merge 15->[[0, 14]] = 15->[[11, 11], [13, 13]]
        other = Checkpoint.empty();
        missedSeqs = new ArrayList<>();
        missedSeqs.add(List.of(0L, 14L));
        other.getMissedSeqs().put(TEST_KEY, missedSeqs);
        other.getKeys().put(TEST_KEY, new LongSeq(16, new MessageMetadata(16L)));
        checkpoint.merge(other);
        assertEquals(checkpoint.getMissedSeqs().get(TEST_KEY).size(), 2);
        assertEquals(checkpoint.getMissedSeqs().get(TEST_KEY).getFirst(), List.of(11L, 11L));
        assertEquals(checkpoint.getMissedSeqs().get(TEST_KEY).getLast(), List.of(13L, 13L));

        // 16->[[11, 11], [13, 13]] merge 8->[[2, 3][5, 7]] = 16->[[11, 11], [13, 13]]
        other = Checkpoint.empty();
        missedSeqs = new ArrayList<>();
        missedSeqs.add(List.of(2L, 3L));
        missedSeqs.add(List.of(5L, 7L));
        other.getMissedSeqs().put(TEST_KEY, missedSeqs);
        other.getKeys().put(TEST_KEY, new LongSeq(8, new MessageMetadata(8L)));
        checkpoint.merge(other);
        assertEquals(checkpoint.getMissedSeqs().get(TEST_KEY).size(), 2);
        assertEquals(checkpoint.getMissedSeqs().get(TEST_KEY).getFirst(), List.of(11L, 11L));
        assertEquals(checkpoint.getMissedSeqs().get(TEST_KEY).getLast(), List.of(13L, 13L));

        //16->[[2, 3],[5, 7][11, 11], [13, 13]] merge 30->[[21, 23], [26, 28]]
        // = 30->[[21, 23], [26, 28]]
        other = Checkpoint.empty();
        missedSeqs = new ArrayList<>();
        missedSeqs.add(List.of(21L, 23L));
        missedSeqs.add(List.of(26L, 28L));
        other.getMissedSeqs().put(TEST_KEY, missedSeqs);
        other.getKeys().put(TEST_KEY, new LongSeq(30, new MessageMetadata(30L)));
        checkpoint.merge(other);
        assertEquals(checkpoint.getMissedSeqs().get(TEST_KEY).size(), 2);
        assertEquals(checkpoint.getMissedSeqs().get(TEST_KEY).getFirst(), List.of(21L, 23L));
        assertEquals(checkpoint.getMissedSeqs().get(TEST_KEY).getLast(), List.of(26L, 28L));

        checkpoint = Checkpoint.empty();
        checkpoint.getKeys().put(TEST_KEY, new LongSeq(100, new MessageMetadata(100L)));
        missedSeqs = new ArrayList<>();
        missedSeqs.add(List.of(0L, 49L));
        checkpoint.getMissedSeqs().put(TEST_KEY, missedSeqs);
        other = Checkpoint.empty();
        missedSeqs = new ArrayList<>();
        missedSeqs.add(List.of(50L, 99L));
        other.getMissedSeqs().put(TEST_KEY, missedSeqs);
        other.getKeys().put(TEST_KEY, new LongSeq(150, new MessageMetadata(150L)));
        Checkpoint mergedClone = checkpoint.clone();
        Checkpoint otherClone = other.clone();
        checkpoint.merge(otherClone);
        other.merge(mergedClone);
        assertNull(checkpoint.getMissedSeqs().get(TEST_KEY));
        assertNull(other.getMissedSeqs().get(TEST_KEY));

        checkpoint = Checkpoint.empty();
        checkpoint.getKeys().put(TEST_KEY, new LongSeq(30347L, new MessageMetadata(30347L)));
        missedSeqs = new ArrayList<>();
        missedSeqs.add(List.of(0L, 30347L));
        other = Checkpoint.empty();
        other.getMissedSeqs().put(TEST_KEY, missedSeqs);
        other.getKeys().put(TEST_KEY, new LongSeq(54476L, new MessageMetadata(54476L)));
        mergedClone = checkpoint.clone();
        otherClone = other.clone();
        checkpoint.merge(otherClone);
        mergedClone.getMissedSeqs().put("other_key", List.of(List.of(0L, 10L)));
        other.merge(mergedClone);
        assertNull(checkpoint.getMissedSeqs().get(TEST_KEY));
        assertNull(other.getMissedSeqs().get(TEST_KEY));
    }

    @Test
    public void testMergeOutOfOrderSequences() {
        List<List<LongSeq>> outOfOrderSeqs = new ArrayList<>();
        outOfOrderSeqs.add(new ArrayList<>());
        outOfOrderSeqs.get(0).add(new LongSeq(1L, new MessageMetadata(1L)));
        outOfOrderSeqs.get(0).add(new LongSeq(3L, new MessageMetadata(2L)));
        checkpoint.getOutOfOrderSeqs().put(TEST_KEY, outOfOrderSeqs);
        checkpoint.getKeys().put(TEST_KEY, new LongSeq(3, new MessageMetadata(3L)));

        Checkpoint other = Checkpoint.empty();
        List<List<LongSeq>> otherOutOfOrderSeqs = new ArrayList<>();
        otherOutOfOrderSeqs.add(new ArrayList<>());
        otherOutOfOrderSeqs.get(0).add(new LongSeq(4L, new MessageMetadata(3L)));
        otherOutOfOrderSeqs.get(0).add(new LongSeq(5L, new MessageMetadata(4L)));
        other.getOutOfOrderSeqs().put(TEST_KEY, otherOutOfOrderSeqs);
        other.getKeys().put(TEST_KEY, new LongSeq(5, new MessageMetadata(5L)));

        checkpoint.merge(other);
        assertEquals(checkpoint.getOutOfOrderSeqs().get(TEST_KEY).size(), 2);

        other = Checkpoint.empty();
        otherOutOfOrderSeqs = new ArrayList<>();
        otherOutOfOrderSeqs.add(new ArrayList<>());
        otherOutOfOrderSeqs.get(0).add(new LongSeq(4L, new MessageMetadata(3L)));
        otherOutOfOrderSeqs.get(0).add(new LongSeq(5L, new MessageMetadata(4L)));
        other.getOutOfOrderSeqs().put(TEST_KEY, otherOutOfOrderSeqs);
        other.getKeys().put(TEST_KEY, new LongSeq(7, new MessageMetadata(7L)));

        checkpoint.merge(other);
        assertEquals(checkpoint.getOutOfOrderSeqs().get(TEST_KEY).size(), 3);

        other = Checkpoint.empty();
        otherOutOfOrderSeqs = new ArrayList<>();
        otherOutOfOrderSeqs.add(new ArrayList<>());
        otherOutOfOrderSeqs.get(0).add(new LongSeq(7L, new MessageMetadata(7L)));
        otherOutOfOrderSeqs.get(0).add(new LongSeq(8L, new MessageMetadata(8L)));
        other.getOutOfOrderSeqs().put(TEST_KEY, otherOutOfOrderSeqs);
        other.getKeys().put(TEST_KEY, new LongSeq(8, new MessageMetadata(8L)));

        checkpoint.merge(other);
        assertEquals(checkpoint.getOutOfOrderSeqs().get(TEST_KEY).size(), 4);

        other = Checkpoint.empty();
        otherOutOfOrderSeqs = new ArrayList<>();
        otherOutOfOrderSeqs.add(new ArrayList<>());
        otherOutOfOrderSeqs.get(0).add(new LongSeq(0L, new MessageMetadata(7L)));
        otherOutOfOrderSeqs.get(0).add(new LongSeq(9L, new MessageMetadata(8L)));
        other.getOutOfOrderSeqs().put(TEST_KEY, otherOutOfOrderSeqs);
        other.getKeys().put(TEST_KEY, new LongSeq(9, new MessageMetadata(8L)));

        checkpoint.merge(other);
        assertEquals(checkpoint.getOutOfOrderSeqs().get(TEST_KEY).size(), 5);

        other = Checkpoint.empty();
        otherOutOfOrderSeqs = new ArrayList<>();
        otherOutOfOrderSeqs.add(new ArrayList<>());
        otherOutOfOrderSeqs.get(0).add(new LongSeq(0L, new MessageMetadata(7L)));
        otherOutOfOrderSeqs.get(0).add(new LongSeq(11L, new MessageMetadata(8L)));
        other.getOutOfOrderSeqs().put(TEST_KEY, otherOutOfOrderSeqs);
        other.getKeys().put(TEST_KEY, new LongSeq(11L, new MessageMetadata(8L)));

        checkpoint.merge(other);
        assertEquals(checkpoint.getOutOfOrderSeqs().get(TEST_KEY).size(), 6);
    }

    @Test
    public void testMergeErrors() {
        // Setup initial errors
        checkpoint.getErrors().put(TEST_KEY, 2);

        // Create checkpoint with errors to merge
        Checkpoint other = Checkpoint.empty();
        other.getErrors().put(TEST_KEY, 3);

        // Test merging errors
        checkpoint.merge(other);
        assertEquals(checkpoint.getErrors().get(TEST_KEY), Integer.valueOf(5), 
            "Errors should be summed");
    }
}