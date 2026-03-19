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
package io.streamnative.streaming.proof.driver.pulsar;

import io.streamnative.streaming.proof.common.MessageListener;
import io.streamnative.streaming.proof.common.MessageMetadata;
import io.streamnative.streaming.proof.common.ProofConsumer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
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
 * <p>The consumer uses a virtual thread to receive messages continuously and
 * delivers them to the provided {@link MessageListener} callback. It tracks message
 * offsets and supports asynchronous acknowledgments to Pulsar.
 */
@Slf4j
public class PulsarAtLeastOnceProofConsumer implements ProofConsumer {

    /** Unique name for this consumer instance */
    private final String name;

    /** The underlying Pulsar consumer instance */
    private final Consumer<Long> consumer;

    /** Thread for running the consumer receive loop */
    private final Thread consumerThread;

    /** Flag indicating whether the consumer is in the process of closing */
    private final AtomicBoolean closing = new AtomicBoolean(false);

    /** The message listener to invoke for each received message */
    private final MessageListener messageListener;

    private final long consumeDelayMs;

    private final Optional<OffloadCondition> offloadCondition;

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
            long consumeDelayMs,
            MessageListener listener,
            PulsarAdmin admin) throws PulsarClientException {

        this.name = name;
        this.messageListener = listener;
        this.consumeDelayMs = consumeDelayMs;
        this.offloadCondition = OffloadCondition.getOffloadCondition(admin, topic, configs);

        ConsumerBuilder<Long> consumerBuilder = client.newConsumer(Schema.INT64)
                .topic(topic)
                .subscriptionName("streaming-proof")
                .subscriptionType(SubscriptionType.Failover)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);

        applyPulsarConsumerConfig(consumerBuilder, configs);

        this.consumer = consumerBuilder.subscribe();
        log.info("[{}] Created Pulsar consumer for topic: {}", name, topic);

        // Create and start a virtual thread for the consumer receive loop
        this.consumerThread = Thread.ofVirtual()
                .name("pulsar-proof-consumer-" + name)
                .start(this::consumeMessages);
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

                        offloadCondition.ifPresent(offloadCondition -> {
                            offloadCondition.waitOffloadConditionMeetForMessage(messageIdImpl);
                        });

                        long ledgerId = messageIdImpl.getLedgerId();
                        long entryId = messageIdImpl.getEntryId();

                        MessageMetadata metadata = new MessageMetadata(ledgerId, entryId);
                        messageListener.onMessage(key, value, metadata);

                        consumer.acknowledge(message);

                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Processed message: key={}, value={}, msgId={}",
                                    name, key, value, message.getMessageId());
                        }

                        if (consumeDelayMs > 0) {
                            long timestampDiff = System.currentTimeMillis() - message.getPublishTime();
                            if (timestampDiff < consumeDelayMs) {
                                log.debug("[{}] Sleeping for {} ms after consuming message",
                                        name, consumeDelayMs - timestampDiff);
                                Thread.sleep(consumeDelayMs - timestampDiff);

                                offloadCondition.ifPresent(OffloadCondition::waitOffloadConditionMeetForCatchupRead);
                            }
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
     *   <li>Waits for the consumer thread to complete</li>
     *   <li>Closes the underlying Pulsar consumer</li>
     * </ul>
     *
     * @throws Exception if an error occurs during shutdown
     */
    @Override
    public void close() throws Exception {
        if (closing.compareAndSet(false, true)) {
            log.info("[{}] Closing Pulsar consumer", name);

            try {
                // Interrupt the consumer thread if it's waiting for new messages
                consumerThread.interrupt();
                consumerThread.join();
            }  catch (Exception e) {
                log.error("[{}] Error while waiting for consumer thread to complete", name, e);
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

    @SuppressWarnings("unchecked")
    private void applyPulsarConsumerConfig(ConsumerBuilder<Long> builder, Map<String, Object> configs) {
        Object raw = configs.get("pulsar.consumer.config");
        if (raw instanceof Map<?, ?> consumerConfig) {
            builder.loadConf((Map<String, Object>) consumerConfig);
        }
    }
}
