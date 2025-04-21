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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import io.streamnative.streaming.proof.common.LongSeq;
import io.streamnative.streaming.proof.common.MessageMetadata;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ConsumerCheckPointTest {

    private static final String TEST_KEY_1 = "testKey1";
    private static final String TEST_KEY_2 = "testKey2";
    private ConsumerCheckPoint checkPoint;

    @BeforeMethod
    public void setup() {
        checkPoint = new ConsumerCheckPoint();
    }

    @Test
    public void testAddKey() {
        // Add the key to the checkpoint
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 5L));

        // Verify the key was added
        assertNotNull(checkPoint.getConsumed().get(TEST_KEY_1));
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).size(), 1);
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).get("1").getStart().seq(), 1L);
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).get("1").getEnd().seq(), 5L);
    }

    @Test
    public void testMerge() {
        // Create first checkpoint with a range
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 5L));

        // Create second checkpoint with a different range
        ConsumerCheckPoint otherCheckPoint = new ConsumerCheckPoint();
        otherCheckPoint.addKey(TEST_KEY_1, createRangeMap("10", 10L, 15L));

        // Merge the checkpoints
        checkPoint.merge(otherCheckPoint);

        // Verify the merge
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).size(), 2);
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).get("1").getStart().seq(), 1L);
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).get("1").getEnd().seq(), 5L);
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).get("10").getStart().seq(), 10L);
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).get("10").getEnd().seq(), 15L);
    }

    @Test
    public void testGetLastSeq() {
        // Add ranges to the checkpoint
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 5L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("10", 10L, 15L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("15", 10L, 12L));

        // Verify getLastSeq returns the highest sequence
        LongSeq lastSeq = checkPoint.getLastSeq(TEST_KEY_1);
        assertNotNull(lastSeq);
        assertEquals(lastSeq.seq(), 15L);

        // Test with non-existent key
        assertNull(checkPoint.getLastSeq("nonExistentKey"));
    }

    @Test
    public void testCalculateMissedSequences() {
        // Create ranges with a gap between them
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 5L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("10", 10L, 15L));

        // Calculate missed sequences
        checkPoint.calculate();

        // Verify missed sequences
        assertNotNull(checkPoint.getMissedSeqs().get(TEST_KEY_1));
        assertEquals(checkPoint.getMissedSeqs().get(TEST_KEY_1).size(), 1);
        Pair<Long, Long> missedRange = checkPoint.getMissedSeqs().get(TEST_KEY_1).getFirst();
        assertEquals(missedRange.getLeft().longValue(), 6L);
        assertEquals(missedRange.getRight().longValue(), 9L);
    }

    @Test
    public void testCalculateDuplicates() {
        // Create overlapping ranges
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 10L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("5", 5L, 15L));

        // Calculate duplicates
        checkPoint.calculate();

        // Verify duplicates
        assertNotNull(checkPoint.getDuplicatedCount().get(TEST_KEY_1));
        // Overlap from 5 to 10 = 6 messages
        assertEquals(checkPoint.getDuplicatedCount().get(TEST_KEY_1).longValue(), 6L);
    }

    @Test
    public void testMultipleKeys() {
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 5L));
        checkPoint.addKey(TEST_KEY_2, createRangeMap("10", 10L, 15L));

        // Verify both keys were added
        assertEquals(checkPoint.getConsumed().size(), 2);
        assertNotNull(checkPoint.getConsumed().get(TEST_KEY_1));
        assertNotNull(checkPoint.getConsumed().get(TEST_KEY_2));
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).get("1").getEnd().seq(), 5L);
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_2).get("10").getEnd().seq(), 15L);
    }

    @Test
    public void testNoMissedSequencesOrDuplicates() {
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 5L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("6", 6L, 10L));

        // Calculate
        checkPoint.calculate();

        // Verify no missed sequences or duplicates
        assertFalse(checkPoint.getMissedSeqs().containsKey(TEST_KEY_1));
        assertFalse(checkPoint.getDuplicatedCount().containsKey(TEST_KEY_1));
    }
    
    @Test
    public void testDuplicateCountWithFullyOverlap() {
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 10L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("15", 5L, 8L));
        
        checkPoint.calculate();
        
        assertNotNull(checkPoint.getDuplicatedCount().get(TEST_KEY_1));
        assertEquals(checkPoint.getDuplicatedCount().get(TEST_KEY_1).longValue(), 4L);
    }
    
    @Test
    public void testDuplicateCountWithMultipleOverlaps() {
        // Add multiple overlapping ranges to test more complex scenarios
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 10L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("5", 5L, 15L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("12", 12L, 20L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("25", 18L, 22L));
        
        checkPoint.calculate();
        
        // Verify duplicates are correctly counted
        // Overlaps: 5-10 (6 messages) + 12-15 (4 messages) + 18-20 (3 messages) = 13 messages
        assertNotNull(checkPoint.getDuplicatedCount().get(TEST_KEY_1));
        assertEquals(checkPoint.getDuplicatedCount().get(TEST_KEY_1).longValue(), 13L);
    }
    
    @Test
    public void testDuplicateCountWithCompletelyContainedRanges() {
        // Test when one range is completely contained within another
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 20L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("5", 5L, 10L));
        
        checkPoint.calculate();
        
        // The range 5-10 (6 messages) is completely contained within 1-20
        assertNotNull(checkPoint.getDuplicatedCount().get(TEST_KEY_1));
        assertEquals(checkPoint.getDuplicatedCount().get(TEST_KEY_1).longValue(), 6L);
    }
    
    @Test
    public void testDuplicateCountWithMultipleContainedRanges() {
        // Test with multiple contained ranges
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 30L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("5", 5L, 10L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("15", 15L, 20L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("25", 25L, 28L));
        
        checkPoint.calculate();
        
        // Contained ranges: 5-10 (6 messages) + 15-20 (6 messages) + 25-28 (4 messages) = 16 messages
        assertNotNull(checkPoint.getDuplicatedCount().get(TEST_KEY_1));
        assertEquals(checkPoint.getDuplicatedCount().get(TEST_KEY_1).longValue(), 16L);
    }
    
    @Test
    public void testCalculateMultipleMissedSequences() {
        // Create ranges with multiple gaps between them
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 5L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("10", 10L, 15L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("20", 20L, 25L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("30", 30L, 35L));

        // Calculate missed sequences
        checkPoint.calculate();

        // Verify missed sequences
        assertNotNull(checkPoint.getMissedSeqs().get(TEST_KEY_1));
        assertEquals(checkPoint.getMissedSeqs().get(TEST_KEY_1).size(), 3);
        
        // First gap: 6-9
        Pair<Long, Long> firstGap = checkPoint.getMissedSeqs().get(TEST_KEY_1).getFirst();
        assertEquals(firstGap.getLeft().longValue(), 6L);
        assertEquals(firstGap.getRight().longValue(), 9L);
        
        // Second gap: 16-19
        Pair<Long, Long> secondGap = checkPoint.getMissedSeqs().get(TEST_KEY_1).get(1);
        assertEquals(secondGap.getLeft().longValue(), 16L);
        assertEquals(secondGap.getRight().longValue(), 19L);
        
        // Third gap: 26-29
        Pair<Long, Long> thirdGap = checkPoint.getMissedSeqs().get(TEST_KEY_1).get(2);
        assertEquals(thirdGap.getLeft().longValue(), 26L);
        assertEquals(thirdGap.getRight().longValue(), 29L);

        // Test adding a range that overlaps with an existing range
        checkPoint.addKey(TEST_KEY_1, createRangeMap("35", 10L, 40L));
        checkPoint.calculate();
        assertNotNull(checkPoint.getDuplicatedCount().get(TEST_KEY_1));
        // Overlap between range [10-15] [20-25] [30-35] and [10-40] is 18 messages
        assertEquals(checkPoint.getDuplicatedCount().get(TEST_KEY_1).longValue(), 18L);
    }
    
    @Test
    public void testDuplicateCountWithAdjacentRanges() {
        // Test with adjacent ranges (no overlap)
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 5L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("6", 6L, 10L));
        
        checkPoint.calculate();
        
        // Adjacent ranges should not count as duplicates
        assertFalse(checkPoint.getDuplicatedCount().containsKey(TEST_KEY_1));
    }
    
    @Test
    public void testTrimRedundantRanges() {
        // Add all ranges to the checkpoint
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 20L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("2", 21L, 30L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("5", 5L, 10L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("15", 15L, 18L));
        
        // Before trimming, we should have 4 ranges
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).size(), 4);
        
        // Calculate which will trigger the trim method
        checkPoint.calculate();
        
        // After trimming, we should have fewer ranges
        // Note: Since the trim method modifies the list in-place, we need to check the size
        // of the consumed map after calculation
        
        // The trim method should have removed the contained ranges
        // We need to verify that only the main range remains
        assertEquals(checkPoint.getMergedConsumed().get(TEST_KEY_1).size(), 1);
        
        // Verify that the remaining range is the main range
        ConsumerCheckPoint.SeqRange remainingRange = checkPoint.getConsumed().get(TEST_KEY_1).firstEntry().getValue();
        assertEquals(remainingRange.getStart().seq(), 1L);
        assertEquals(remainingRange.getEnd().seq(), 20L);

        ConsumerCheckPoint.SeqRange mergedRange = checkPoint.getMergedConsumed().get(TEST_KEY_1).getFirst();
        assertEquals(mergedRange.getStart().seq(), 1L);
        assertEquals(mergedRange.getEnd().seq(), 30L);
    }

    @Test
    public void testTrimWithNonContainedRanges() {
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 10L));
        checkPoint.addKey(TEST_KEY_1, createRangeMap("20", 20L, 30L));
        
        // Before trimming, we should have 2 ranges
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).size(), 2);
        
        // Calculate which will trigger the trim method
        checkPoint.calculate();
        
        // After trimming, we should still have 2 ranges since neither is contained within the other
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).size(), 2);
    }

    @Test
    public void testEmptyCheckpoint() {
        // Verify behavior with empty checkpoint
        assertNull(checkPoint.getLastSeq(TEST_KEY_1));
        
        // Calculate on empty checkpoint
        checkPoint.calculate();
        
        // Verify no missed sequences or duplicates
        assertFalse(checkPoint.getMissedSeqs().containsKey(TEST_KEY_1));
        assertFalse(checkPoint.getDuplicatedCount().containsKey(TEST_KEY_1));
    }
    
    @Test
    public void testMergeWithEmptyCheckpoint() {
        // Create first checkpoint with a range
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 5L));
        
        // Create empty checkpoint
        ConsumerCheckPoint emptyCheckPoint = new ConsumerCheckPoint();
        
        // Merge the checkpoints
        checkPoint.merge(emptyCheckPoint);
        
        // Verify the merge didn't change anything
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).size(), 1);
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).get("1").getStart().seq(), 1L);
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).get("1").getEnd().seq(), 5L);
        
        // Now merge in the other direction
        emptyCheckPoint.merge(checkPoint);
        
        // Verify the empty checkpoint now has the data
        assertEquals(emptyCheckPoint.getConsumed().get(TEST_KEY_1).size(), 1);
        assertEquals(emptyCheckPoint.getConsumed().get(TEST_KEY_1).get("1").getStart().seq(), 1L);
        assertEquals(emptyCheckPoint.getConsumed().get(TEST_KEY_1).get("1").getEnd().seq(), 5L);
    }
    
    @Test
    public void testMergeOverlappingRanges() {
        // Create first checkpoint with a range
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 10L));
        
        // Create second checkpoint with an overlapping range
        ConsumerCheckPoint otherCheckPoint = new ConsumerCheckPoint();
        otherCheckPoint.addKey(TEST_KEY_1, createRangeMap("5", 5L, 15L));
        
        // Merge the checkpoints
        checkPoint.merge(otherCheckPoint);
        
        // Verify the merge
        assertEquals(checkPoint.getConsumed().get(TEST_KEY_1).size(), 2);
        
        // Calculate which will trigger the trim method
        checkPoint.calculate();
        
        // After merging and trimming, we should have a single continuous range
        assertEquals(checkPoint.getMergedConsumed().get(TEST_KEY_1).size(), 1);
        assertEquals(checkPoint.getMergedConsumed().get(TEST_KEY_1).getFirst().getStart().seq(), 1L);
        assertEquals(checkPoint.getMergedConsumed().get(TEST_KEY_1).getFirst().getEnd().seq(), 15L);
    }
    
    @Test
    public void testTrimEdgeCases() {
        // Test with empty map
        checkPoint.trim();
        assertTrue(checkPoint.getMergedConsumed().isEmpty());
        
        // Test with single range
        checkPoint.addKey(TEST_KEY_1, createRangeMap("1", 1L, 5L));
        checkPoint.trim();
        
        assertEquals(checkPoint.getMergedConsumed().get(TEST_KEY_1).size(), 1);
        assertEquals(checkPoint.getMergedConsumed().get(TEST_KEY_1).getFirst().getStart().seq(), 1L);
        assertEquals(checkPoint.getMergedConsumed().get(TEST_KEY_1).getFirst().getEnd().seq(), 5L);
    }
    
    private Map<String, ConsumerCheckPoint.SeqRange> createRangeMap(String orderKey, Long start, Long end) {
        Map<String, ConsumerCheckPoint.SeqRange> rangeMap = new HashMap<>();
        ConsumerCheckPoint.SeqRange range = new ConsumerCheckPoint.SeqRange();
        range.setStart(new LongSeq(start, new MessageMetadata(start)));
        range.setEnd(new LongSeq(end, new MessageMetadata(end)));
        rangeMap.put(orderKey, range);
        return rangeMap;
    }
}