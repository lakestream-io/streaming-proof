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

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
import io.streamnative.streaming.proof.common.MessageMetadata;
import io.streamnative.streaming.proof.common.ProofProducer;
import io.streamnative.streaming.proof.common.ProofValue;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MqttAtLeastOnceProofProducer implements ProofProducer {

    private final Mqtt5BlockingClient client;

    private final String topic;

    /** Total size in bytes of each message value, including the sequence number */
    private final int messageSize;

    public MqttAtLeastOnceProofProducer(Mqtt5BlockingClient client, String topic, int messageSize) {
        this.client = client;
        this.topic = topic;
        this.messageSize = messageSize;
    }

    @Override
    public CompletableFuture<MessageMetadata> sendAsync(String key, long value) {
        CompletableFuture<MessageMetadata> future = new CompletableFuture<>();
        try {
            final Mqtt5Publish message = Mqtt5Publish.builder()
                    .topic(topic)
                    .payload(MqttProofPayload.encode(key, new ProofValue(value, messageSize)))
                    .qos(MqttQos.AT_LEAST_ONCE)
                    .build();
            final Mqtt5PublishResult publish = client.publish(message);
            future.complete(new MessageMetadata(value));
        } catch (Exception e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    @Override
    public void close() throws Exception {
        if (client != null) {
            client.disconnect();
        }
        log.info("[{}] Producer closed successfully", topic);
    }
}
