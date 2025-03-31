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

import io.netty.util.concurrent.DefaultThreadFactory;
import io.streamnative.streaming.proof.common.MessageListener;
import io.streamnative.streaming.proof.common.MessageMetadata;
import io.streamnative.streaming.proof.common.ProofConsumer;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

/**
 * Pulsar implementation of the ProofConsumer interface that provides at-least-once message
 * delivery guarantees. This consumer runs in a dedicated thread and supports manual
 * acknowledgment of messages.
 *
 * <p>Key features:
 * <ul>
 *   <li>At-least-once message delivery guarantee</li>
 *   <li>Asynchronous message consumption</li>
 *   <li>Manual message acknowledgment</li>
 *   <li>Graceful shutdown support</li>
 * </ul>
 *
 * <p>The consumer uses a single-threaded executor to receive messages continuously and
 * delivers them to the provided {@link MessageListener} callback. It tracks message
 * offsets and supports asynchronous acknowledgments to Pulsar.
 */
@Slf4j
public class PulsarAtLeastOnceProofConsumer implements ProofConsumer {

    /** Unique name for this consumer instance */
    private final String name;

    /** The underlying Pulsar consumer instance */
    private final Consumer<Long> consumer;

    /** Executor service for running the consumer receive loop */
    private final ExecutorService executor;

    /** Flag indicating whether the consumer is in the process of closing */
    private final AtomicBoolean closing = new AtomicBoolean(false);

    /** The message listener to invoke for each received message */
    private final MessageListener messageListener;

    /**
     * Creates a new Pulsar consumer with at-least-once delivery guarantees.
     *
     * @param name Unique name for this consumer instance
     * @param client The Pulsar client instance
     * @param topic The topic to consume from
     * @param configs Consumer-specific configuration parameters
     * @param listener Callback that will receive consumed messages
     * @throws PulsarClientException if consumer creation fails
     */
    public PulsarAtLeastOnceProofConsumer(
            String name,
            PulsarClient client,
            String topic,
            Map<String, Object> configs,
            MessageListener listener) throws PulsarClientException {

        this.name = name;
        this.messageListener = listener;

        ConsumerBuilder<Long> consumerBuilder = client.newConsumer(Schema.INT64)
                .topic(topic)
                .subscriptionName("streaming-proof-" + name)
                .subscriptionType(SubscriptionType.Failover)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);

        this.consumer = consumerBuilder.subscribe();
        log.info("[{}] Created Pulsar consumer for topic: {}", name, topic);

        this.executor = Executors.newSingleThreadExecutor(
                new DefaultThreadFactory("pulsar-proof-consumer-" + name));

        this.executor.submit(this::consumeMessages);
    }

    /**
     * Main message consumption loop that continuously receives messages
     * and passes them to the message listener.
     */
    private void consumeMessages() {
        while (!closing.get()) {
            try {
                Message<Long> message = consumer.receive();
                if (message != null) {
                    try {
                        String key = message.getKey();
                        Long value = message.getValue();

                        MessageIdAdv messageIdImpl = (MessageIdAdv) message.getMessageId();
                        long ledgerId = messageIdImpl.getLedgerId();
                        long entryId = messageIdImpl.getEntryId();

                        MessageMetadata metadata = new MessageMetadata(-1L, ledgerId, entryId);
                        messageListener.onMessage(key, value, metadata);

                        consumer.acknowledge(message);

                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Processed message: key={}, value={}, msgId={}",
                                    name, key, value, message.getMessageId());
                        }
                    } catch (Exception e) {
                        log.error("[{}] Error processing message", name, e);
                        consumer.negativeAcknowledge(message);
                    }
                }
            } catch (Exception e) {
                if (!closing.get()) {
                    log.error("[{}] Exception in consumer loop", name, e);
                }
            }
        }
    }

    /**
     * Closes this consumer and releases all resources.
     * <p>This method:
     * <ul>
     *   <li>Signals the receiving loop to stop</li>
     *   <li>Shuts down the executor service</li>
     *   <li>Closes the underlying Pulsar consumer</li>
     * </ul>
     *
     * @throws Exception if an error occurs during shutdown
     */
    @Override
    public void close() throws Exception {
        if (closing.compareAndSet(false, true)) {
            log.info("[{}] Closing Pulsar consumer", name);

            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }

            if (consumer != null) {
                consumer.close();
            }

            log.info("[{}] Pulsar consumer closed", name);
        }
    }

    /**
     * Returns the unique name of this consumer instance.
     *
     * @return The consumer name
     */
    @Override
    public String name() {
        return name;
    }
}