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
package io.streamnative.streaming.proof.driver.kafka;

import io.netty.util.concurrent.DefaultThreadFactory;
import io.streamnative.streaming.proof.common.MessageListener;
import io.streamnative.streaming.proof.common.MessageMetadata;
import io.streamnative.streaming.proof.common.ProofConsumer;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Kafka implementation of the ProofConsumer interface that provides at-least-once message
 * delivery guarantees. This consumer runs in a dedicated thread and supports both automatic
 * and manual offset commit strategies.
 *
 * <p>Key features:
 * <ul>
 *   <li>At-least-once message delivery guarantee</li>
 *   <li>Asynchronous message consumption</li>
 *   <li>Configurable offset commit strategy</li>
 *   <li>Graceful shutdown support</li>
 * </ul>
 *
 * <p>The consumer uses a single-threaded executor to poll messages continuously and
 * delivers them to the provided {@link MessageListener} callback. It tracks message
 * offsets per partition and supports asynchronous offset commits to Kafka.
 *
 * <p>Example usage:
 * <pre>{@code
 * Map<String, Object> config = new HashMap<>();
 * config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
 * config.put(ConsumerConfig.GROUP_ID_CONFIG, "proof-group");
 * config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
 *
 * KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(config);
 * MessageListener listener = (key, value) -> {
 *     // Process message
 * };
 *
 * KafkaAtLeastOnceProofConsumer proofConsumer = 
 *     new KafkaAtLeastOnceProofConsumer(consumer, config, listener);
 * 
 * // Later...
 * proofConsumer.close();
 * }</pre>
 */
@Slf4j
public class KafkaAtLeastOnceProofConsumer implements ProofConsumer {

    private final String name;
    /** The underlying Kafka consumer instance */
    private final KafkaConsumer<String, Long> consumer;

    /** Executor service for running the consumer polling loop */
    private final ExecutorService executor;
    
    /** Future representing the running consumer task */
    private final Future<?> consumerTask;
    
    /** Flag indicating whether the consumer is in the process of closing */
    private volatile boolean closing = false;
    
    /** Whether automatic offset commits are enabled */
    private final boolean autoCommit;

    /**
     * Creates a new Kafka consumer with at-least-once delivery guarantees.
     *
     * @param name Unique name for this consumer
     * @param consumer The underlying Kafka consumer instance
     * @param consumerConfig Configuration for the consumer, including commit strategy
     * @param consumeDelayMs Optional delay after consuming each message
     * @param callback Listener that will receive consumed messages
     */
    public KafkaAtLeastOnceProofConsumer(
            String name,
            KafkaConsumer<String, Long> consumer,
            Map<String, Object> consumerConfig,
            long consumeDelayMs,
            MessageListener callback) {
        this.name = name;
        this.consumer = consumer;
        this.executor = Executors.newSingleThreadExecutor(new DefaultThreadFactory("proof-consumer"));
        this.autoCommit =
                Boolean.parseBoolean(
                        (String)
                                consumerConfig.getOrDefault(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"));
        this.consumerTask =
                this.executor.submit(
                        () -> {                            
                            while (!closing) {
                                try {
                                    if (consumeDelayMs > 0) {
                                        prioritizePartitionsBasedOnLag();
                                    }
                                    ConsumerRecords<String, Long> records =
                                            consumer.poll(Duration.ofSeconds(30));
                                    Map<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
                                    Map<String, List<Long>> offsetRange = new HashMap<>();
                                    for (ConsumerRecord<String, Long> record : records) {
                                        if (consumeDelayMs > 0) {
                                            long timestampDiff = System.currentTimeMillis() - record.timestamp();
                                            if (timestampDiff < consumeDelayMs) {
                                                long sleepTime = consumeDelayMs - timestampDiff;
                                                log.debug("[{}] Sleeping for {} ms after consuming message",
                                                        name, sleepTime);
                                                Thread.sleep(sleepTime);
                                            }
                                        }
                                        callback.onMessage(record.key(), record.value(),
                                                new MessageMetadata(record.offset(), record.partition()));

                                        offsetMap.put(
                                                new TopicPartition(record.topic(), record.partition()),
                                                new OffsetAndMetadata(record.offset() + 1));
                                        offsetRange.compute(record.topic() + "-" + record.partition(),
                                                (k, v) -> {
                                                    if (v == null) {
                                                        v = List.of(record.offset(), record.offset());
                                                    } else {
                                                        v = List.of(v.getFirst(), record.offset());
                                                    }
                                                    return v;
                                                });
                                    }

                                    offsetRange.forEach((topic, range) -> log.debug(
                                            "[{}] Polled messages in offset range {}-{} from topic {}",
                                            name,
                                            range.getFirst(),
                                            range.get(1),
                                            topic));

                                    if (!autoCommit && !offsetMap.isEmpty()) {
                                        // Async commit all messages polled so far
                                        consumer.commitAsync(offsetMap, (offsets,
                                                                         exception) -> {
                                            if (exception != null) {
                                                log.error("Failed to commit offsets", exception);
                                            }
                                        });
                                    }
                                } catch (Exception e) {
                                    log.error("exception occur while consuming message", e);
                                }
                            }
                        });
    }
    
    /**
     * Checks the lag for each partition and prioritizes consumption from partitions
     * with higher lag by pausing/resuming partitions as needed.
     */
    private void prioritizePartitionsBasedOnLag() {
        try {
            Set<TopicPartition> assignedPartitions = consumer.assignment();
            if (assignedPartitions.isEmpty()) {
                return;
            }
            
            // Get the end offsets (latest) for all assigned partitions
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignedPartitions);
            
            // Calculate lag for each partition
            Map<TopicPartition, Long> partitionLags = new HashMap<>();
            for (TopicPartition partition : assignedPartitions) {
                long currentPosition = consumer.position(partition);
                long endOffset = endOffsets.get(partition);
                long lag = endOffset - currentPosition;
                
                partitionLags.put(partition, lag);
                log.debug("[{}] Partition {}-{} has lag of {} messages",
                        name, partition.topic(), partition.partition(), lag);
            }
            
            // Find the partition with the highest lag
            TopicPartition highestLagPartition = null;
            long maxLag = -1;
            
            for (Map.Entry<TopicPartition, Long> entry : partitionLags.entrySet()) {
                if (entry.getValue() > maxLag) {
                    maxLag = entry.getValue();
                    highestLagPartition = entry.getKey();
                }
            }
            
            // Pause all partitions first
            consumer.pause(assignedPartitions);
            
            // Only resume the partition with the highest lag if it has any lag
            if (highestLagPartition != null && maxLag > 0) {
                consumer.resume(Collections.singleton(highestLagPartition));
                log.debug("[{}] Prioritizing consumption from single partition: {}-{} (lag: {})",
                        name, 
                        highestLagPartition.topic(), 
                        highestLagPartition.partition(),
                        maxLag);
            } else {
                // If no partition has lag, resume all to check for new messages
                consumer.resume(assignedPartitions);
                log.debug("[{}] No significant lag detected, resuming all partitions", name);
            }
            
        } catch (Exception e) {
            log.error("[{}] Error while checking partition lags", name, e);
        }
    }

    /**
     * Closes this consumer and releases all resources.
     * <p>This method:
     * <ul>
     *   <li>Signals the polling loop to stop</li>
     *   <li>Shuts down the executor service</li>
     *   <li>Waits for the consumer task to complete</li>
     *   <li>Closes the underlying Kafka consumer</li>
     * </ul>
     *
     * @throws Exception if an error occurs during shutdown
     */
    @Override
    public void close() throws Exception {
        this.executor.execute(() -> {
            closing = true;
            executor.shutdown();
            try {
                consumerTask.get();
            } catch (Exception e) {
                log.error("Error while waiting for consumer task to complete", e);
            }
            consumer.close();
        });
    }

    @Override
    public String name() {
        return name;
    }
}
