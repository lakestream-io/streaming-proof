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
package io.streamnative.streaming.proof.common.records;

import static org.testng.Assert.assertEquals;

import io.streamnative.streaming.proof.common.ProofValue;
import io.streamnative.streaming.proof.common.Util;
import org.testng.annotations.Test;

public class ProofMessageSizeTest {

    @Test
    public void testDefaultsWhenAbsentFromJson() throws Exception {
        // Existing proof requests do not carry messageSize. They must still default to
        // the sequence-number size rather than 0, which validation would reject.
        String json = """
                {"name":"p","driver":"kafka","topic":"t"}
                """;

        assertEquals(Util.JSON_MAPPER.readValue(json, Proof.class).getMessageSize(), ProofValue.MIN_SIZE);
    }

    @Test
    public void testReadsExplicitValueFromJson() throws Exception {
        String json = """
                {"name":"p","driver":"kafka","topic":"t","messageSize":1024}
                """;

        assertEquals(Util.JSON_MAPPER.readValue(json, Proof.class).getMessageSize(), 1024);
    }

    @Test
    public void testBuilderDefaults() {
        assertEquals(Proof.builder().build().getMessageSize(), ProofValue.MIN_SIZE);
    }
}
