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

import io.lakestream.streaming.proof.common.MessageListener;
import io.lakestream.streaming.proof.common.ProofValue;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
public class KafkaAtLeastOnceProofConsumerTest {

    @Mock
    private MessageListener messageListener;
    
    @Mock
    private KafkaConsumer<String, ProofValue> mockConsumer;
    
    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }
    
    @Test
    public void testClose() throws Exception {
        // Arrange
        String consumerName = "test-consumer";
        Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", "localhost:9092");
        configs.put("group.id", "test-group");
        
        KafkaAtLeastOnceProofConsumer consumer = new KafkaAtLeastOnceProofConsumer(
                consumerName,
                mockConsumer,
                configs,
                0L, // No consume delay
                messageListener);
        
        // Act
        consumer.close();
        
        // Assert
        Mockito.verify(mockConsumer).close(); // Verify consumer.close() was called
    }
}