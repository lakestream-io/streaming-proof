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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.testng.annotations.Test;

public class MessageMetadataTest {

    @Test
    public void testConstructors() {
        // Test full constructor
        MessageMetadata metadata1 = new MessageMetadata(100L, 1L, 2L, 3);
        assertEquals(metadata1.offset(), Long.valueOf(100L));
        assertEquals(metadata1.ledgerId(), Long.valueOf(1L));
        assertEquals(metadata1.entryId(), Long.valueOf(2L));
        assertEquals(metadata1.partition(), Integer.valueOf(3));
        
        // Test offset-only constructor
        MessageMetadata metadata2 = new MessageMetadata(200L);
        assertEquals(metadata2.offset(), Long.valueOf(200L));
        assertNull(metadata2.ledgerId());
        assertNull(metadata2.entryId());
        assertNull(metadata2.partition());
        
        // Test offset and partition constructor
        MessageMetadata metadata3 = new MessageMetadata(300L, 5);
        assertEquals(metadata3.offset(), Long.valueOf(300L));
        assertNull(metadata3.ledgerId());
        assertNull(metadata3.entryId());
        assertEquals(metadata3.partition(), Integer.valueOf(5));
        
        // Test ledger and entry ID constructor
        MessageMetadata metadata4 = new MessageMetadata(2L, 3L);
        assertNull(metadata4.offset());
        assertEquals(metadata4.ledgerId(), Long.valueOf(2L));
        assertEquals(metadata4.entryId(), Long.valueOf(3L));
        assertNull(metadata4.partition());
    }
    
    @Test
    public void testEmpty() {
        // Test the empty() factory method
        MessageMetadata emptyMetadata = MessageMetadata.empty();
        
        assertEquals(emptyMetadata.offset(), Long.valueOf(-1L));
        assertEquals(emptyMetadata.ledgerId(), Long.valueOf(-1L));
        assertEquals(emptyMetadata.entryId(), Long.valueOf(-1L));
        assertNull(emptyMetadata.partition());
    }
    
    @Test
    public void testIsAfterWithOffsets() {
        // Test isAfter() with offset-based metadata
        MessageMetadata metadata1 = new MessageMetadata(100L);
        MessageMetadata metadata2 = new MessageMetadata(200L);
        MessageMetadata metadata3 = new MessageMetadata(100L);
        
        // Higher offset should be after
        assertTrue(metadata2.isAfter(metadata1));
        
        // Lower offset should not be after
        assertFalse(metadata1.isAfter(metadata2));
        
        // Same offset should not be after
        assertFalse(metadata1.isAfter(metadata3));
        assertFalse(metadata3.isAfter(metadata1));
        
        // Any metadata should be after null
        assertTrue(metadata1.isAfter(null));
    }
    
    @Test
    public void testIsAfterWithLedgerEntries() {
        // Test isAfter() with ledger and entry ID based metadata
        MessageMetadata metadata1 = new MessageMetadata(1L, 5L);
        MessageMetadata metadata2 = new MessageMetadata(1L, 10L);
        MessageMetadata metadata3 = new MessageMetadata(2L, 1L);
        
        // Same ledger, higher entry should be after
        assertTrue(metadata2.isAfter(metadata1));
        assertFalse(metadata1.isAfter(metadata2));
        
        // Higher ledger should be after regardless of entry
        assertTrue(metadata3.isAfter(metadata1));
        assertTrue(metadata3.isAfter(metadata2));
        assertFalse(metadata1.isAfter(metadata3));
        assertFalse(metadata2.isAfter(metadata3));
    }
    
    @Test
    public void testIsAfterMixedTypes() {
        // Test isAfter() with mixed metadata types
        MessageMetadata offsetMetadata = new MessageMetadata(100L);
        
        // Create a metadata with both offset and ledger/entry values
        MessageMetadata mixedMetadata = new MessageMetadata(50L, 1L, 5L, null);
        
        // When comparing different types with valid offsets, should use offset comparison
        assertTrue(offsetMetadata.isAfter(mixedMetadata));
        assertFalse(mixedMetadata.isAfter(offsetMetadata));
    }
    
    @Test
    public void testToString() throws JsonProcessingException {
        // Test toString() method
        MessageMetadata metadata = new MessageMetadata(100L, 1L, 2L, 3);
        String metadataString = metadata.toString();
        
        // Verify the string contains all the values
        assertTrue(metadataString.contains("100"));
        assertTrue(metadataString.contains("1"));
        assertTrue(metadataString.contains("2"));
        assertTrue(metadataString.contains("3"));
        
        // Verify the string can be parsed back to a MessageMetadata
        MessageMetadata parsedMetadata = Util.JSON_MAPPER.readValue(metadataString, MessageMetadata.class);
        assertEquals(parsedMetadata.offset(), Long.valueOf(100L));
        assertEquals(parsedMetadata.ledgerId(), Long.valueOf(1L));
        assertEquals(parsedMetadata.entryId(), Long.valueOf(2L));
        assertEquals(parsedMetadata.partition(), Integer.valueOf(3));
    }
    
    @Test
    public void testEquality() {
        // Test equals() and hashCode()
        MessageMetadata metadata1 = new MessageMetadata(100L, 1L, 2L, 3);
        MessageMetadata metadata2 = new MessageMetadata(100L, 1L, 2L, 3);
        MessageMetadata metadata3 = new MessageMetadata(200L, 1L, 2L, 3);
        
        // Same values should be equal
        assertEquals(metadata1, metadata2);
        assertEquals(metadata1.hashCode(), metadata2.hashCode());
        
        // Different values should not be equal
        assertFalse(metadata1.equals(metadata3));
    }
}