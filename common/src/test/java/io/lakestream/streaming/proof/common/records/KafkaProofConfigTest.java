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
package io.lakestream.streaming.proof.common.records;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import io.lakestream.streaming.proof.common.Util;
import java.util.Map;
import org.testng.annotations.Test;

public class KafkaProofConfigTest {

    @Test
    public void testSerializationRoundTrip() throws Exception {
        KafkaProofConfig config = KafkaProofConfig.builder()
                .topicConfig(Map.of("unclean.leader.election.enable", "true"))
                .build();

        String json = Util.JSON_MAPPER.writeValueAsString(config);
        KafkaProofConfig deserialized = Util.JSON_MAPPER.readValue(json, KafkaProofConfig.class);

        assertNotNull(deserialized.getTopicConfig());
        assertEquals(deserialized.getTopicConfig().get("unclean.leader.election.enable"), "true");
    }

    @Test
    public void testProofWithKafkaConfig() throws Exception {
        String json = """
                {
                  "name": "kafka-topic",
                  "topic": "latency-topic",
                  "features": ["at_least_once"],
                  "kafka": {
                    "topicConfig": {
                      "unclean.leader.election.enable": "true"
                    }
                  }
                }
                """;

        Proof proof = Util.JSON_MAPPER.readValue(json, Proof.class);

        assertNotNull(proof.getKafka());
        assertEquals(proof.getKafka().getTopicConfig().get("unclean.leader.election.enable"), "true");
    }

    @Test
    public void testProofWithoutKafkaConfig() throws Exception {
        String json = """
                {
                  "name": "test",
                  "topic": "t",
                  "features": ["at_least_once"]
                }
                """;

        Proof proof = Util.JSON_MAPPER.readValue(json, Proof.class);

        assertNull(proof.getKafka());
    }
}
