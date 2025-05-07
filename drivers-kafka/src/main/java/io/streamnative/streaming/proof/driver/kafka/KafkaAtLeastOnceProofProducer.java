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

import io.streamnative.streaming.proof.common.MessageMetadata;
import io.streamnative.streaming.proof.common.ProofProducer;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Kafka implementation of the ProofProducer interface that provides at-least-once message
 * delivery guarantees. This producer wraps a Kafka producer instance and provides
 * asynchronous message sending capabilities.
 *
 * <p>Key features:
 * <ul>
 *   <li>At-least-once message delivery guarantee</li>
 *   <li>Asynchronous message production</li>
 *   <li>Key-based message routing</li>
 *   <li>Sequential value tracking per key</li>
 * </ul>
 *
 * <p>The producer sends messages with string keys and long values, where the values
 * represent sequential numbers for each key. This sequence tracking enables verification
 * of message ordering and delivery guarantees.
 *
 * <p>Example usage:
 * <pre>{@code
 * Properties props = new Properties();
 * props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
 * props.put(ProducerConfig.ACKS_CONFIG, "all");
 * props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
 * props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
 *
 * Producer<String, Long> kafkaProducer = new KafkaProducer<>(props);
 * KafkaAtLeastOnceProofProducer producer = 
 *     new KafkaAtLeastOnceProofProducer(kafkaProducer, "test-topic");
 *
 * // Send messages
 * producer.sendAsync("key-1", 1L)
 *         .thenAccept(v -> System.out.println("Message sent successfully"))
 *         .exceptionally(e -> {
 *             System.err.println("Failed to send message: " + e);
 *             return null;
 *         });
 * }</pre>
 *
 * @see ProofProducer
 * @see org.apache.kafka.clients.producer.Producer
 * @see org.apache.kafka.clients.producer.ProducerRecord
 */
public class KafkaAtLeastOnceProofProducer implements ProofProducer {

    /** The underlying Kafka producer instance */
    private final Producer<String, Long> producer;
    
    /** The topic to which messages will be sent */
    private final String topic;

    /**
     * Creates a new Kafka producer with at-least-once delivery guarantees.
     *
     * @param producer The underlying Kafka producer instance
     * @param topic The topic to which messages will be sent
     */
    public KafkaAtLeastOnceProofProducer(Producer<String, Long> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    /**
     * Asynchronously sends a message with the specified key and sequential value.
     * The method wraps the Kafka producer's send operation in a CompletableFuture
     * for better asynchronous operation handling.
     *
     * @param key The message key used for partitioning and sequence identification.
     *            Keys are used to group related messages and ensure they are sent
     *            to the same partition.
     * @param value A sequential value representing the message's position in its
     *              key's sequence. Used to verify message ordering and detect
     *              duplicates or missing messages.
     * @return A CompletableFuture that completes with the message metadata when
     *         the send operation is acknowledged by Kafka. The metadata includes
     *         the message offset in the partition. The future completes
     *         exceptionally if the send operation fails.
     * @see MessageMetadata
     * @see org.apache.kafka.clients.producer.RecordMetadata
     */
    @Override
    public CompletableFuture<MessageMetadata> sendAsync(String key, long value) {
        ProducerRecord<String, Long> record = new ProducerRecord<>(topic, key, value);
        CompletableFuture<MessageMetadata> future = new CompletableFuture<>();

        try {
            producer.send(
                    record,
                    (metadata, exception) -> {
                        if (exception != null) {
                            future.completeExceptionally(exception);
                        } else {
                            future.complete(new MessageMetadata(metadata.offset(), metadata.partition()));
                        }
                    });
        } catch (Exception e) {
            future.completeExceptionally(e);
        }

        return future;
    }

    /**
     * Closes this producer and releases all resources.
     * This method delegates to the underlying Kafka producer's close method.
     *
     * @throws Exception if an error occurs during shutdown
     */
    @Override
    public void close() throws Exception {
        producer.close();
    }
}
