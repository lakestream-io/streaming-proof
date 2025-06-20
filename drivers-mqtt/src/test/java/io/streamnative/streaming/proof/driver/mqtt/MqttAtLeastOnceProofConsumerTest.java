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
package io.streamnative.streaming.proof.driver.mqtt;

import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import io.streamnative.streaming.proof.common.MessageListener;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MqttAtLeastOnceProofConsumerTest {

    @Mock
    private MessageListener messageListener;
    
    @Mock
    private Mqtt5BlockingClient client;
    
    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }
    
    @Test
    public void testClose() throws Exception {
        // Arrange
        String consumerName = "test-consumer";

        MqttAtLeastOnceProofConsumer consumer = new MqttAtLeastOnceProofConsumer(
                client, consumerName, "topic", messageListener);

        // Act
        consumer.close();
        
        // Assert
        Mockito.verify(client).disconnect(); // Verify consumer.close() was called
    }
}