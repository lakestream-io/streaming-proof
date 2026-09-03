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
package io.lakestream.streaming.proof.driver.mqtt;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import io.lakestream.streaming.proof.common.ProofValue;
import java.nio.charset.StandardCharsets;
import org.testng.annotations.Test;

public class MqttProofPayloadTest {

    @Test
    public void testRoundTrip() {
        byte[] payload = MqttProofPayload.encode("key-1", new ProofValue(42L, 128));

        assertEquals(MqttProofPayload.decodeKey(payload), "key-1");
        assertEquals(MqttProofPayload.decodeValue(payload).seq(), 42L);
        assertEquals(MqttProofPayload.decodeValue(payload).size(), 128);
    }

    @Test
    public void testDecodesWhenPaddingContainsSeparator() {
        // Padding is binary and will eventually contain a colon byte. Splitting on
        // every colon rather than the first would corrupt the value, so find a seq
        // whose padding actually contains one and prove it still decodes.
        long seq = seqWithSeparatorInPadding();
        byte[] payload = MqttProofPayload.encode("key-1", new ProofValue(seq, 256));

        assertEquals(MqttProofPayload.decodeKey(payload), "key-1");
        assertEquals(MqttProofPayload.decodeValue(payload).seq(), seq);
    }

    @Test
    public void testDefaultSizeRoundTrips() {
        byte[] payload = MqttProofPayload.encode("k", ProofValue.of(5L));

        assertEquals(MqttProofPayload.decodeKey(payload), "k");
        assertEquals(MqttProofPayload.decodeValue(payload).seq(), 5L);
    }

    @Test
    public void testPayloadWithoutSeparatorIsRejected() {
        byte[] payload = "no-separator".getBytes(StandardCharsets.UTF_8);

        assertThrows(IllegalArgumentException.class, () -> MqttProofPayload.decodeKey(payload));
        assertThrows(IllegalArgumentException.class, () -> MqttProofPayload.decodeValue(payload));
    }

    private static long seqWithSeparatorInPadding() {
        for (long seq = 0; seq < 1000; seq++) {
            byte[] encoded = new ProofValue(seq, 256).encode();
            for (int i = Long.BYTES; i < encoded.length; i++) {
                if (encoded[i] == (byte) ':') {
                    return seq;
                }
            }
        }
        throw new AssertionError("No seq under 1000 produced a colon byte in its padding");
    }

    @Test
    public void testSeparatorInPaddingIsReachable() {
        // Guards the helper above: if this ever fails the colon test has gone vacuous.
        assertTrue(seqWithSeparatorInPadding() >= 0);
    }
}
