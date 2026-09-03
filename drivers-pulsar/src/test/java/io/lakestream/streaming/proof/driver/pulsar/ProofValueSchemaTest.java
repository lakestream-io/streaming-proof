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
package io.lakestream.streaming.proof.driver.pulsar;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import io.lakestream.streaming.proof.common.ProofValue;
import org.apache.pulsar.client.api.Schema;
import org.testng.annotations.Test;

public class ProofValueSchemaTest {

    private final ProofValueSchema schema = new ProofValueSchema();

    @Test
    public void testRoundTrip() {
        ProofValue decoded = schema.decode(schema.encode(new ProofValue(4242L, 512)));

        assertEquals(decoded.seq(), 4242L);
        assertEquals(decoded.size(), 512);
    }

    @Test
    public void testDefaultSizeMatchesInt64Schema() {
        // Rolling upgrades depend on this: at the default size the bytes must be
        // identical to what Schema.INT64 used to write.
        assertEquals(schema.encode(ProofValue.of(7L)), Schema.INT64.encode(7L));
    }

    @Test
    public void testNullPassesThrough() {
        assertNull(schema.encode(null));
        assertNull(schema.decode((byte[]) null));
    }
}
