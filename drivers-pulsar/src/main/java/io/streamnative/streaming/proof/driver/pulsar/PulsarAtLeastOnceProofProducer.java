/**
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
package io.streamnative.streaming.proof.driver.pulsar;

import io.streamnative.streaming.proof.common.MessageMetadata;
import io.streamnative.streaming.proof.common.ProofProducer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;

/**
 * Pulsar implementation of the ProofProducer interface that provides at-least-once message
 * delivery guarantees. This producer wraps a Pulsar producer instance and provides
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
 */
@Slf4j
public class PulsarAtLeastOnceProofProducer implements ProofProducer {

    /** The underlying Pulsar producer instance */
    private final Producer<Long> producer;

    /** The topic to which messages will be sent */
    private final String topic;

    /**
     * Creates a new Pulsar producer with at-least-once delivery guarantees.
     *
     * @param client The Pulsar client instance
     * @param topic The topic to which messages will be sent
     * @param configs Producer-specific configuration parameters
     * @throws PulsarClientException if producer creation fails
     */
    public PulsarAtLeastOnceProofProducer(PulsarClient client, String topic, Map<String, Object> configs)
            throws PulsarClientException {
        this.topic = topic;

        ProducerBuilder<Long> producerBuilder = client.newProducer(Schema.INT64)
                .topic(topic)
                .blockIfQueueFull(true)
                .sendTimeout(0, TimeUnit.SECONDS);

        this.producer = producerBuilder.create();
        log.info("Created Pulsar producer for topic: {}", topic);
    }

    /**
     * Asynchronously sends a message with the specified key and sequential value.
     * The method wraps the Pulsar producer's send operation in a CompletableFuture
     * for better asynchronous operation handling.
     *
     * @param key The message key used for partitioning and sequence identification.
     *            Keys are used to group related messages and ensure they are sent
     *            to the same partition.
     * @param value A sequential value representing the message's position in its
     *              key's sequence. Used to verify message ordering and detect
     *              duplicates or missing messages.
     * @return A CompletableFuture that completes with the message metadata when
     *         the send operation is acknowledged by Pulsar. The metadata includes
     *         the message offset in the partition. The future completes
     *         exceptionally if the send operation fails.
     */
    @Override
    public CompletableFuture<MessageMetadata> sendAsync(String key, long value) {
        TypedMessageBuilder<Long> messageBuilder = producer.newMessage()
                .key(key)
                .value(value);

        CompletableFuture<MessageMetadata> resultFuture = new CompletableFuture<>();

        messageBuilder.sendAsync()
                .thenAccept(msgId -> {
                    MessageIdAdv topicMessageIdImpl = (MessageIdAdv) msgId;
                    long ledgerId = topicMessageIdImpl.getLedgerId();
                    long entryId = topicMessageIdImpl.getEntryId();
                    resultFuture.complete(new MessageMetadata(-1, ledgerId, entryId));
                })
                .exceptionally(ex -> {
                    resultFuture.completeExceptionally(ex);
                    return null;
                });

        return resultFuture;
    }

    /**
     * Closes this producer and releases all resources.
     * This method delegates to the underlying Pulsar producer's close method.
     *
     * @throws Exception if an error occurs during shutdown
     */
    @Override
    public void close() throws Exception {
        if (producer != null) {
            producer.close();
            log.info("Closed Pulsar producer for topic: {}", topic);
        }
    }
}