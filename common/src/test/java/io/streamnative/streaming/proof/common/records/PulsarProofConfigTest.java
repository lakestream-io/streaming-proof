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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import io.streamnative.streaming.proof.common.Util;
import java.util.Map;
import org.testng.annotations.Test;

public class PulsarProofConfigTest {

    @Test
    public void testSerializationRoundTrip() throws Exception {
        PulsarProofConfig config = PulsarProofConfig.builder()
                .consumerConfig(Map.of("subscriptionType", "Key_Shared"))
                .build();

        String json = Util.JSON_MAPPER.writeValueAsString(config);
        PulsarProofConfig deserialized = Util.JSON_MAPPER.readValue(json, PulsarProofConfig.class);

        assertNotNull(deserialized.getConsumerConfig());
        assertEquals(deserialized.getConsumerConfig().get("subscriptionType"), "Key_Shared");
    }

    @Test
    public void testDeserializationWithNullConsumerConfig() throws Exception {
        String json = "{}";
        PulsarProofConfig deserialized = Util.JSON_MAPPER.readValue(json, PulsarProofConfig.class);
        assertNull(deserialized.getConsumerConfig());
    }

    @Test
    public void testProofWithPulsarConfig() throws Exception {
        String json = """
                {
                  "name": "test",
                  "topic": "t",
                  "features": ["at_least_once"],
                  "pulsar": {
                    "consumerConfig": {
                      "subscriptionType": "Key_Shared",
                      "receiverQueueSize": 500
                    }
                  }
                }
                """;
        Proof proof = Util.JSON_MAPPER.readValue(json, Proof.class);
        assertNotNull(proof.getPulsar());
        assertEquals(proof.getPulsar().getConsumerConfig().get("subscriptionType"), "Key_Shared");
        assertEquals(proof.getPulsar().getConsumerConfig().get("receiverQueueSize"), 500);
    }

    @Test
    public void testProofWithoutPulsarConfig() throws Exception {
        String json = """
                {
                  "name": "test",
                  "topic": "t",
                  "features": ["at_least_once"]
                }
                """;
        Proof proof = Util.JSON_MAPPER.readValue(json, Proof.class);
        assertNull(proof.getPulsar());
    }

    @Test
    public void testNewConsumersSerializationWithPulsarConfig() throws Exception {
        Map<String, Object> pulsarConfig = Map.of("subscriptionType", "Key_Shared");
        NewConsumers consumers = new NewConsumers(
                "proof-1", "topic-1", 4, 2, 0L, "pulsar",
                new Driver("pulsar", Map.of("pulsar.service.url", "pulsar://localhost:6650")),
                pulsarConfig);

        String json = Util.JSON_MAPPER.writeValueAsString(consumers);
        NewConsumers deserialized = Util.JSON_MAPPER.readValue(json, NewConsumers.class);

        assertNotNull(deserialized.pulsarConsumerConfig());
        assertEquals(deserialized.pulsarConsumerConfig().get("subscriptionType"), "Key_Shared");
    }

    @Test
    public void testNewConsumersSerializationWithoutPulsarConfig() throws Exception {
        NewConsumers consumers = new NewConsumers(
                "proof-1", "topic-1", 4, 2, 0L, "pulsar",
                new Driver("pulsar", Map.of("pulsar.service.url", "pulsar://localhost:6650")),
                null);

        String json = Util.JSON_MAPPER.writeValueAsString(consumers);
        NewConsumers deserialized = Util.JSON_MAPPER.readValue(json, NewConsumers.class);

        assertNull(deserialized.pulsarConsumerConfig());
    }

    @Test
    public void testDriverMetadataDeserialization() throws Exception {
        String json = """
                {
                  "workers": {
                    "worker.1": "http://worker:8088"
                  },
                  "drivers": {
                    "local_pulsar": {
                      "driverType": "pulsar",
                      "driverConfigs": {
                        "pulsar.service.url": "pulsar://cluster:6650",
                        "pulsar.admin.url": "http://cluster:8080"
                      },
                      "metadata": {
                        "displayName": "Classic Stage",
                        "brokers": "3 x n2-standard-8",
                        "notes": "catchup validation cluster"
                      }
                    }
                  }
                }
                """;

        Configs configs = Util.JSON_MAPPER.readValue(json, Configs.class);
        Driver driver = configs.drivers().get("local_pulsar");

        assertNotNull(driver);
        assertNotNull(driver.metadata());
        assertEquals(driver.metadata().get("displayName"), "Classic Stage");
        assertEquals(driver.metadata().get("brokers"), "3 x n2-standard-8");
    }
}
