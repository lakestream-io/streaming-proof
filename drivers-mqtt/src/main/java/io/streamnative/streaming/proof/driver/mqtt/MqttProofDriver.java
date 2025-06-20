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

import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import io.streamnative.streaming.proof.common.MessageListener;
import io.streamnative.streaming.proof.common.ProofConsumer;
import io.streamnative.streaming.proof.common.ProofDriver;
import io.streamnative.streaming.proof.common.ProofProducer;
import java.net.URI;
import java.net.URL;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.TenantInfo;

@Slf4j
public class MqttProofDriver implements ProofDriver {

    /** Configuration key for Pulsar service URL */
    private static final String MQTT_SERVICE_URL = "mqtt.service.url";

    /** Configuration key for Pulsar admin URL */
    private static final String PULSAR_ADMIN_URL = "pulsar.admin.url";

    /** Configuration key for authentication token */
    private static final String MQTT_AUTH_TOKEN = "mqtt.auth.token";

    /** Default namespace for topics */
    private static final String DEFAULT_NAMESPACE = "public/default";

    /** Default tenant for topics */
    private static final String DEFAULT_TENANT = "public";


    private Mqtt5BlockingClient consumerClient;

    private Mqtt5AsyncClient producerClient;

    /** Pulsar admin client for topic management */
    private PulsarAdmin admin;

    /**
     * Initializes the driver with Pulsar-specific configurations.
     *
     * @param configs Configuration map containing Pulsar settings
     */
    @Override
    public void init(Map<String, Object> configs) {
        try {
            String serviceUrl = (String) configs.getOrDefault(MQTT_SERVICE_URL, "http://localhost:5683");
            String adminUrl = (String) configs.getOrDefault(PULSAR_ADMIN_URL, "http://localhost:8080");
            String authToken = (String) configs.get(MQTT_AUTH_TOKEN);

            URL url = new URI(serviceUrl).toURL();

            this.consumerClient = Mqtt5Client.builder()
                    .identifier(UUID.randomUUID().toString())
                    .serverHost(url.getHost())
                    .serverPort(url.getPort())
                    .buildBlocking();

            consumerClient.connect();
            this.producerClient = Mqtt5Client.builder()
                    .identifier(UUID.randomUUID().toString())
                    .serverHost(url.getHost())
                    .serverPort(url.getPort())
                    .buildAsync();

            producerClient.connect();

            if (authToken != null && !authToken.isEmpty()) {
                this.admin = PulsarAdmin.builder()
                        .serviceHttpUrl(adminUrl)
                        .authentication(
                                "org.apache.pulsar.client.impl.auth.AuthenticationToken",
                                authToken
                        )
                        .build();
            } else {
                this.admin = PulsarAdmin.builder()
                        .serviceHttpUrl(adminUrl)
                        .build();
            }

            // Ensure the default namespace exists
            try {
                if (!admin.tenants().getTenants().contains(DEFAULT_TENANT)) {
                    admin.tenants().createTenant(DEFAULT_TENANT,
                            TenantInfo.builder().build());
                }
                if (!admin.namespaces().getNamespaces(DEFAULT_TENANT).contains(DEFAULT_NAMESPACE)) {
                    admin.namespaces().createNamespace(DEFAULT_NAMESPACE);
                }
            } catch (PulsarAdminException e) {
                log.warn("Error checking/creating default namespace", e);
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize Pulsar driver", e);
        }
    }

    /**
     * Creates a new Pulsar topic with the specified name and partition count.
     *
     * @param topicName Name of the topic to create
     * @param partitions Number of partitions for the topic
     * @throws RuntimeException if topic creation fails
     */
    @Override
    public void createTopic(String topicName, int partitions) {
        try {
            String fullTopicName = getFullTopicName(topicName);
            if (partitions > 1) {
                admin.topics().createPartitionedTopic(fullTopicName, partitions);
            } else {
                admin.topics().createNonPartitionedTopic(fullTopicName);
            }
            log.info("Created Pulsar topic: {} with {} partitions", fullTopicName, partitions);
        } catch (PulsarAdminException e) {
            if (e.getStatusCode() == 409) {
                log.info("Topic already exists, continuing");
            } else {
                throw new RuntimeException("Failed to create topic: " + topicName, e);
            }
        }
    }

    /**
     * Deletes a Pulsar topic and all its associated resources.
     *
     * @param topicName Name of the topic to delete
     * @throws RuntimeException if topic deletion fails
     */
    @Override
    public void deleteTopic(String topicName) {
        try {
            String fullTopicName = getFullTopicName(topicName);

            // Check if topic is partitioned
            boolean isPartitioned = false;
            try {
                admin.topics().getPartitionedTopicMetadata(fullTopicName);
                isPartitioned = true;
            } catch (PulsarAdminException.NotFoundException e) {
                // Topic is not partitioned or doesn't exist
            }

            if (isPartitioned) {
                admin.topics().deletePartitionedTopic(fullTopicName, true);
            } else {
                admin.topics().delete(fullTopicName, true);
            }
            log.info("Deleted Pulsar topic: {}", fullTopicName);
        } catch (PulsarAdminException e) {
            if (e instanceof PulsarAdminException.NotFoundException) {
                log.info("Topic does not exist, nothing to delete");
            } else {
                throw new RuntimeException("Failed to delete topic: " + topicName, e);
            }
        }
    }

    /**
     * Creates a new producer with at-least-once delivery guarantees.
     *
     * @param topicName Target topic for the producer
     * @param configs Producer-specific configurations
     * @return A configured ProofProducer instance
     */
    @Override
    public ProofProducer createProducer(String topicName, Map<String, Object> configs) {
        try {
            return new MqttAtLeastOnceProofProducer(producerClient, topicName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a new consumer with the specified message listener.
     *
     * @param topicName Topic to consume from
     * @param configs Consumer-specific configurations
     * @param listener Callback for processing consumed messages
     * @return A configured ProofConsumer instance
     */
    @Override
    public ProofConsumer createConsumer(String topicName, int partitionCount, long consumeDelayMs,
                                        Map<String, Object> configs, MessageListener listener) {
        try {
            String consumerName = UUID.randomUUID().toString();
            return new MqttAtLeastOnceProofConsumer(consumerClient, consumerName, topicName, listener);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Constructs the fully qualified topic name with namespace.
     *
     * @param topicName The base topic name
     * @return The fully qualified topic name
     */
    private String getFullTopicName(String topicName) {
        return "persistent://" + DEFAULT_NAMESPACE + "/" + topicName;
    }

    /**
     * Closes the Pulsar client and admin resources.
     */
    @Override
    public void close() {
        try {
            if (admin != null) {
                admin.close();
            }
        } catch (Exception e) {
            log.error("Error closing Pulsar resources", e);
        }
    }
}

