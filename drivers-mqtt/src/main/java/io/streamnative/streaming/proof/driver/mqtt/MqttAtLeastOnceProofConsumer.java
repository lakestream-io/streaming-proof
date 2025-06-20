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

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.streamnative.streaming.proof.common.MessageListener;
import io.streamnative.streaming.proof.common.MessageMetadata;
import io.streamnative.streaming.proof.common.ProofConsumer;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class MqttAtLeastOnceProofConsumer implements ProofConsumer {

    private final String name;
    private final String topic;
    private final Mqtt5BlockingClient client;
    private final MessageListener callback;

    private volatile boolean closing = false;

    public MqttAtLeastOnceProofConsumer(Mqtt5BlockingClient client,
                                        String name, String topic, MessageListener callback) {
        this.name = name;
        this.topic = topic;
        this.client = client;
        this.callback = callback;
    }

    public void start() {
        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL)) {
            this.client.subscribeWith().topicFilter(topic).qos(MqttQos.AT_LEAST_ONCE).send();
            try {
                while (!closing) {
                    try {
                        final Optional<Mqtt5Publish> receive = publishes.receive(30, TimeUnit.SECONDS);
                        if (receive.isEmpty()) {
                            continue; // No messages received, continue polling
                        }
                        final Mqtt5Publish mqtt5Publish = receive.get();
                        String payload = new String(mqtt5Publish.getPayloadAsBytes());
                        String key = payload.split(":")[0];
                        long value = Long.parseLong(payload.split(":")[1]);
                        callback.onMessage(key, value, MessageMetadata.empty());
                        mqtt5Publish.acknowledge();
                    } catch (Exception e) {
                        log.error("[{}] Exception occurred while consuming message", name, e);
                    }
                }
            } catch (Throwable t) {
                log.error("[{}] Fatal error in consumer thread", name, t);
            }
        }
        log.info("[{}] Consumer started successfully", name);
    }

    @Override
    public void close() throws Exception {
        closing = true;
        if (client != null) {
            client.disconnect();
        }
        log.info("[{}] Consumer closed successfully", name);
    }

    @Override
    public String name() {
        return this.name;
    }
}
