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
package io.streamnative.streaming.proof.driver.kafka;

import io.streamnative.streaming.proof.common.MessageListener;
import io.streamnative.streaming.proof.common.ProofConsumer;
import io.streamnative.streaming.proof.common.ProofDriver;
import io.streamnative.streaming.proof.common.ProofProducer;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Kafka implementation of the ProofDriver interface that manages Kafka resources
 * and creates producers and consumers for streaming proof verification.
 *
 * <p>This driver provides:
 * <ul>
 *   <li>Topic management with configurable partitioning</li>
 *   <li>Producer creation with at-least-once delivery guarantees</li>
 *   <li>Consumer creation with partition assignment and rebalance handling</li>
 *   <li>Offset management for reliable message tracking</li>
 *   <li>Zone-aware client ID management for distributed deployment</li>
 * </ul>
 *
 * <p>The driver uses string keys and long sequential values for messages, enabling:
 * <ul>
 *   <li>Sequence-based verification of delivery guarantees</li>
 *   <li>Key-based message routing and partition assignment</li>
 *   <li>Offset-based message tracing for debugging delivery issues</li>
 *   <li>Partition-level parallelism for scalable testing</li>
 * </ul>
 *
 *
 * @see ProofDriver
 * @see KafkaAtLeastOnceProofProducer
 * @see KafkaAtLeastOnceProofConsumer
 */
@Slf4j
public class KafkaProofDriver implements ProofDriver {

    /** Configuration key for zone ID */
    private static final String ZONE_ID_CONFIG = "zone.id";
    
    /** Template placeholder for zone ID in client IDs */
    private static final String ZONE_ID_TEMPLATE = "{zone.id}";
    
    /** Configuration key for Kafka client ID */
    private static final String KAFKA_CLIENT_ID = "client.id";

    /** Kafka admin client for topic management */
    private AdminClient admin;

    /**
     * Initializes the driver with Kafka-specific configurations.
     *
     * @param configs Configuration map containing Kafka settings
     */
    @Override
    public void init(Map<String, Object> configs) {
        admin = AdminClient.create(configs);
    }

    /**
     * Creates a new Kafka topic with the specified name and partition count.
     * The replication factor is fixed at 1 for testing purposes.
     *
     * @param topicName Name of the topic to create
     * @param partitions Number of partitions for the topic
     * @throws RuntimeException if topic creation fails
     */
    @Override
    public void createTopic(String topicName, int partitions) {
        NewTopic newTopic = new NewTopic(topicName, partitions, (short) 1);
        try {
            admin.createTopics(List.of(newTopic)).all().get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Deletes a Kafka topic and all its associated resources.
     *
     * @param topicName Name of the topic to delete
     * @throws RuntimeException if topic deletion fails
     */
    @Override
    public void deleteTopic(String topicName) {
        try {
            admin.deleteTopics(List.of(topicName)).all().get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a new producer with at-least-once delivery guarantees.
     * Configures string key serialization and long value serialization.
     *
     * @param topicName Target topic for the producer
     * @param configs Producer-specific configurations
     * @return A configured ProofProducer instance
     */
    @Override
    public ProofProducer createProducer(String topicName, Map<String, Object> configs) {
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        if (configs.containsKey(KAFKA_CLIENT_ID)) {
            configs.put(
                    KAFKA_CLIENT_ID,
                    applyZoneId(
                            String.valueOf(configs.get(KAFKA_CLIENT_ID)), System.getProperty(ZONE_ID_CONFIG)));
        }
        KafkaProducer<String, Long> producer = new KafkaProducer<>(configs);
        return new KafkaAtLeastOnceProofProducer(producer, topicName);
    }

    /**
     * Creates a new consumer with the specified message listener.
     * Configures string key deserialization and long value deserialization.
     *
     * @param topicName Topic to consume from
     * @param configs Consumer-specific configurations
     * @param listener Callback for processing consumed messages
     * @return A configured ProofConsumer instance
     */
    @Override
    public ProofConsumer createConsumer(String topicName, int partitionCount, long consumeDelayMs,
                Map<String, Object> configs, MessageListener listener) {
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "streaming-proof");
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        if (configs.containsKey(KAFKA_CLIENT_ID)) {
            configs.put(
                    KAFKA_CLIENT_ID,
                    applyZoneId(
                            String.valueOf(configs.get(KAFKA_CLIENT_ID)), System.getProperty(ZONE_ID_CONFIG)));
        }
        KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(configs);
        String consumerName = RandomStringUtils.secure().nextAlphanumeric(5);
        consumer.subscribe(List.of(topicName), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                partitions.forEach(p -> log.info("[{}] Kafka partition revoked: {}", consumerName, p));
            }
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                partitions.forEach(p -> log.info("[{}] Kafka partition assigned: {}", consumerName, p));
            }
        });
        return new KafkaAtLeastOnceProofConsumer(consumerName, consumer, configs, consumeDelayMs, listener);
    }

    /**
     * Applies zone ID to client ID template if present.
     *
     * @param clientId The client ID template
     * @param zoneId The zone ID to apply
     * @return The processed client ID
     */
    private static String applyZoneId(String clientId, String zoneId) {
        return clientId.replace(ZONE_ID_TEMPLATE, zoneId);
    }

    @Override
    public void close() throws IOException {
        if (admin != null) {
            admin.close();
        }
    }
}
