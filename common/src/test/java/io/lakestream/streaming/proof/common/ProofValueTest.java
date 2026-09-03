/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package io.lakestream.streaming.proof.common;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ProofValueTest {

    @DataProvider(name = "sizes")
    public Object[][] sizes() {
        return new Object[][] {{8}, {9}, {16}, {100}, {1024}, {1024 * 1024}};
    }

    @Test(dataProvider = "sizes")
    public void testRoundTripPreservesSeqAndSize(int size) {
        ProofValue original = new ProofValue(123456789L, size);
        ProofValue decoded = ProofValue.decode(original.encode());

        assertEquals(decoded.seq(), 123456789L);
        assertEquals(decoded.size(), size);
    }

    @Test(dataProvider = "sizes")
    public void testEncodedLengthMatchesRequestedSize(int size) {
        assertEquals(new ProofValue(1L, size).encode().length, size);
    }

    @Test
    public void testMinSizeEncodesToBareSequenceNumber() {
        // At the default size the encoding must be byte-identical to a plain
        // big-endian long, so messages stay readable across the change.
        long seq = 42L;
        byte[] encoded = ProofValue.of(seq).encode();

        assertEquals(encoded.length, Long.BYTES);
        assertEquals(encoded, ByteBuffer.allocate(Long.BYTES).putLong(seq).array());
    }

    @Test
    public void testSizeBelowMinimumIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new ProofValue(1L, 7));
        assertThrows(IllegalArgumentException.class, () -> new ProofValue(1L, 0));
        assertThrows(IllegalArgumentException.class, () -> new ProofValue(1L, -1));
    }

    @Test
    public void testDecodeRejectsShortPayload() {
        assertThrows(IllegalArgumentException.class, () -> ProofValue.decode(new byte[7]));
        assertThrows(IllegalArgumentException.class, () -> ProofValue.decode(new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> ProofValue.decode(null));
    }

    @Test
    public void testPaddingIsReproducibleForSameSeq() {
        assertEquals(new ProofValue(7L, 64).encode(), new ProofValue(7L, 64).encode());
    }

    @Test
    public void testPaddingDiffersAcrossSeq() {
        byte[] first = new ProofValue(1L, 64).encode();
        byte[] second = new ProofValue(2L, 64).encode();

        assertNotEquals(
                Arrays.copyOfRange(first, Long.BYTES, first.length),
                Arrays.copyOfRange(second, Long.BYTES, second.length));
    }

    @Test
    public void testPaddingIsNotConstant() {
        // Constant padding would compress away, defeating the point of inflating
        // the message at all.
        byte[] padding = Arrays.copyOfRange(new ProofValue(1L, 1024).encode(), Long.BYTES, 1024);
        long distinct = Arrays.stream(toBoxed(padding)).distinct().count();

        assertTrue(distinct > 64, "Padding should be high-entropy, distinct bytes: " + distinct);
    }

    @Test
    public void testDecodeIgnoresTrailingBytes() {
        byte[] payload = new byte[32];
        ByteBuffer.wrap(payload).putLong(99L);
        Arrays.fill(payload, Long.BYTES, payload.length, (byte) 0xAB);

        assertEquals(ProofValue.decode(payload).seq(), 99L);
    }

    @Test
    public void testNegativeAndBoundarySeqValues() {
        for (long seq : new long[] {0L, -1L, Long.MIN_VALUE, Long.MAX_VALUE}) {
            assertEquals(ProofValue.decode(new ProofValue(seq, 32).encode()).seq(), seq);
        }
    }

    private static Byte[] toBoxed(byte[] bytes) {
        Byte[] boxed = new Byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            boxed[i] = bytes[i];
        }
        return boxed;
    }
}
