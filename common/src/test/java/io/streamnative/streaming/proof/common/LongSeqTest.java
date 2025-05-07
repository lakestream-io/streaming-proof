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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.testng.annotations.Test;

public class LongSeqTest {

    @Test
    public void testConstructor() {
        // Test basic constructor
        MessageMetadata metadata = new MessageMetadata(100L);
        LongSeq seq = new LongSeq(42, metadata);
        
        assertEquals(seq.seq(), 42);
        assertEquals(seq.metadata(), metadata);
        assertEquals(seq.metadata().offset(), Long.valueOf(100L));
    }
    
    @Test
    public void testEmpty() {
        // Test the empty() factory method
        LongSeq emptySeq = LongSeq.empty();
        
        assertEquals(emptySeq.seq(), -1);
        assertNotNull(emptySeq.metadata());
        assertEquals(emptySeq.metadata().offset(), Long.valueOf(-1L));
        assertEquals(emptySeq.metadata().ledgerId(), Long.valueOf(-1L));
        assertEquals(emptySeq.metadata().entryId(), Long.valueOf(-1L));
    }
    
    @Test
    public void testCompareTo() {
        // Test comparison with smaller sequence
        LongSeq seq1 = new LongSeq(10, MessageMetadata.empty());
        LongSeq seq2 = new LongSeq(20, MessageMetadata.empty());
        
        assertTrue(seq1.compareTo(seq2) < 0);
        assertTrue(seq2.compareTo(seq1) > 0);
        assertEquals(seq1.compareTo(seq1), 0);
        
        // Test comparison with equal sequence but different metadata
        LongSeq seq3 = new LongSeq(10, new MessageMetadata(100L));
        LongSeq seq4 = new LongSeq(10, new MessageMetadata(200L));
        
        assertEquals(seq3.compareTo(seq4), 0);
        assertEquals(seq4.compareTo(seq3), 0);
        
        // Test comparison with empty sequence
        LongSeq emptySeq = LongSeq.empty();
        assertTrue(emptySeq.compareTo(seq1) < 0);
        assertTrue(seq1.compareTo(emptySeq) > 0);
    }
    
    @Test
    public void testToString() throws JsonProcessingException {
        // Test toString() method
        LongSeq seq = new LongSeq(42, new MessageMetadata(100L));
        String seqString = seq.toString();
        
        // Verify the string contains the sequence number and offset
        assertTrue(seqString.contains("42"));
        assertTrue(seqString.contains("100"));
        
        // Verify the string can be parsed back to a LongSeq
        LongSeq parsedSeq = Util.JSON_MAPPER.readValue(seqString, LongSeq.class);
        assertEquals(parsedSeq.seq(), 42);
        assertEquals(parsedSeq.metadata().offset(), Long.valueOf(100L));
    }
    
    @Test
    public void testEquality() {
        // Test equals() and hashCode()
        LongSeq seq1 = new LongSeq(10, new MessageMetadata(100L));
        LongSeq seq2 = new LongSeq(10, new MessageMetadata(100L));
        LongSeq seq3 = new LongSeq(20, new MessageMetadata(100L));
        LongSeq seq4 = new LongSeq(10, new MessageMetadata(200L));
        
        // Same sequence and metadata should be equal
        assertEquals(seq1, seq2);
        assertEquals(seq1.hashCode(), seq2.hashCode());
        
        // Different sequence should not be equal
        assertFalse(seq1.equals(seq3));
        
        // Different metadata should not be equal
        assertFalse(seq1.equals(seq4));
    }
}