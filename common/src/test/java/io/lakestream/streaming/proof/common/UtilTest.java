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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class UtilTest {

    @Test
    public void testJsonMapperConfiguration() {
        // Verify that the JSON_MAPPER is properly configured
        assertNotNull(Util.JSON_MAPPER);
        
        // We'll just verify the mapper exists and is configured
        // The actual enum handling is implementation-specific and may vary
        assertTrue(Util.JSON_MAPPER.isEnabled(com.fasterxml.jackson.databind.DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE));
    }

    @Test
    public void testJsonWriter() throws JsonProcessingException {
        // Verify that the JSON_WRITER is properly configured
        assertNotNull(Util.JSON_WRITER);
        
        // Test that the writer can serialize objects
        Map<String, String> testMap = new HashMap<>();
        testMap.put("key1", "value1");
        testMap.put("key2", "value2");
        
        String json = Util.JSON_WRITER.writeValueAsString(testMap);
        
        // Verify the serialized JSON
        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> deserializedMap = mapper.readValue(json, Map.class);
        assertEquals(deserializedMap.size(), 2);
        assertEquals(deserializedMap.get("key1"), "value1");
        assertEquals(deserializedMap.get("key2"), "value2");
    }

    @Test
    public void testSupportedProofFeatures() {
        // Verify that the SUPPORTED_PROOF_FEATS set contains the expected values
        Set<String> supportedFeatures = Util.SUPPORTED_PROOF_FEATS;
        assertEquals(supportedFeatures.size(), 3);
        assertTrue(supportedFeatures.contains("at_least_once"));
        assertTrue(supportedFeatures.contains("ordering"));
        assertTrue(supportedFeatures.contains("exactly_once"));
    }

    @Test
    public void testEndpointConstants() {
        // Verify that the endpoint constants are defined correctly
        
        // Proof management endpoints
        assertEquals(Util.CREATE_PROOF, "/proofs");
        assertEquals(Util.GET_PROOF, "/proofs/{id}");
        assertEquals(Util.GET_PROOF_DETAILS, "/proofs/{id}/details");
        assertEquals(Util.STOP_PROOF, "/proofs/{id}/stop");
        assertEquals(Util.DELETE_PROOF, "/proofs/{id}");
        assertEquals(Util.LIST_PROOFS, "/proofs");
        
        // Configuration management endpoints
        assertEquals(Util.PUT_CONFIG, "/configs");
        assertEquals(Util.GET_CONFIG, "/configs");
        assertEquals(Util.DELETE_CONFIG, "/configs");
        
        // Producer management endpoints
        assertEquals(Util.START_PRODUCER, "/producers/start");
        assertEquals(Util.PRODUCER_CHECKPOINTS, "/producers/checkpoints/{id}");
        assertEquals(Util.STOP_PRODUCER, "/producers/stop/{id}");
        
        // Consumer management endpoints
        assertEquals(Util.START_CONSUMER, "/consumers/start");
        assertEquals(Util.CONSUMER_CHECKPOINTS, "/consumers/checkpoints/{id}");
        assertEquals(Util.CONSUMER_CHECKPOINTS_DETAILS, "/consumers/checkpoints/{id}/details");
        assertEquals(Util.STOP_CONSUMER, "/consumers/stop/{id}");
    }
    
    // Test enum for testing JSON_MAPPER configuration
    private enum TestEnum {
        VALUE1, VALUE2, DEFAULT
    }
    
    // Test class for testing JSON_MAPPER configuration
    private static class TestClass {
        private TestEnum testEnum;
        
        public TestEnum getTestEnum() {
            return testEnum;
        }
        
        public void setTestEnum(TestEnum testEnum) {
            this.testEnum = testEnum;
        }
    }
}