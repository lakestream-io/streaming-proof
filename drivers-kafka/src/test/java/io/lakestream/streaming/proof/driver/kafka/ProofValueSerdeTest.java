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
package io.lakestream.streaming.proof.driver.kafka;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import io.lakestream.streaming.proof.common.ProofValue;
import org.apache.kafka.common.serialization.LongSerializer;
import org.testng.annotations.Test;

public class ProofValueSerdeTest {

    private static final String TOPIC = "topic";

    private final ProofValueSerializer serializer = new ProofValueSerializer();
    private final ProofValueDeserializer deserializer = new ProofValueDeserializer();

    @Test
    public void testRoundTrip() {
        ProofValue value = new ProofValue(4242L, 512);
        ProofValue decoded = deserializer.deserialize(TOPIC, serializer.serialize(TOPIC, value));

        assertEquals(decoded.seq(), 4242L);
        assertEquals(decoded.size(), 512);
    }

    @Test
    public void testDefaultSizeMatchesLongSerializer() {
        // Rolling upgrades depend on this: at the default size the bytes must be
        // identical to what LongSerializer used to write, so old and new consumers
        // can read each other's messages.
        try (LongSerializer longSerializer = new LongSerializer()) {
            assertEquals(
                    serializer.serialize(TOPIC, ProofValue.of(7L)),
                    longSerializer.serialize(TOPIC, 7L));
        }
    }

    @Test
    public void testNullPassesThrough() {
        assertNull(serializer.serialize(TOPIC, null));
        assertNull(deserializer.deserialize(TOPIC, null));
    }
}
