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

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Embedded Kafka transactional processor that implements the read-process-write pattern
 * for exactly-once semantics verification. This processor is automatically started by
 * the streaming-proof framework when the {@code exactly_once} feature is enabled.
 * 
 * <p>The processor:
 * <ul>
 *   <li>Consumes messages from input topic ({topicName}_transactional)</li>
 *   <li>Produces messages to output topic ({topicName}) in the same partition</li>
 *   <li>Commits consumer offsets and producer messages atomically</li>
 *   <li>Ensures exactly-once processing semantics</li>
 * </ul>
 * 
 * <p>Topic flow: Producer → {topic}_transactional → TransactionalProcessor → {topic} → Consumer
 * 
 * <p>This class is used internally by the framework and should not be instantiated directly.
 * Instead, enable exactly-once processing by setting the {@code exactly_once} feature in
 * the proof configuration.
 */
@Slf4j
public class KafkaTransactionalProcessor {
    private final KafkaConsumer<String, Long> consumer;
    private final KafkaProducer<String, Long> producer;
    private final String inputTopic;
    private final String outputTopic;
    private final String transactionalId;
    private final String consumerGroupId;
    private final String groupInstanceId;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicLong processedMessages = new AtomicLong(0);
    private final AtomicLong transactionCount = new AtomicLong(0);
    private final Thread processingThread;
    private static final int MAX_RETRIES = 5;
    private static final int TRANSACTION_TIMEOUT_MS = 10_000;
    private volatile int retries = 0;
    
    public KafkaTransactionalProcessor(Properties baseProps, String topicName) {
        this.inputTopic = topicName + "_transactional";
        this.outputTopic = topicName;

        // Generate unique instance identifier using UUID to ensure multi-instance safety
        String instanceId = UUID.randomUUID().toString().substring(0, 8);

        // Derive stable identifiers with unique instance ID
        String clientId = (String) baseProps.getOrDefault(ProducerConfig.CLIENT_ID_CONFIG, null);
        String baseGroupInstanceId = (String) baseProps.getOrDefault(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, null);
        String instanceComponent = clientId != null
                ? clientId + "-" + instanceId
                : (baseGroupInstanceId != null ? baseGroupInstanceId + "-" + instanceId : "processor-" + instanceId);

        this.consumerGroupId = "transactional-processor-" + topicName;
        this.groupInstanceId = "giid-" + topicName + "-" + instanceComponent;
        this.transactionalId = "txp-" + topicName + "-" + instanceComponent;
        
        // Validate identifier uniqueness
        if (this.transactionalId.length() > 255) {
            throw new IllegalArgumentException("Transactional ID too long: "
                + this.transactionalId.length() + " characters");
        }

        // Configure consumer
        Properties consumerProps = new Properties();
        consumerProps.putAll(baseProps);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerProps.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId);
        // Optimize for multi-instance scenarios
        consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        consumerProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "10000");
        consumerProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        
        this.consumer = new KafkaConsumer<>(consumerProps);
        
        // Configure transactional producer
        Properties producerProps = new Properties();
        producerProps.putAll(baseProps);
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        producerProps.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, TRANSACTION_TIMEOUT_MS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        
        this.producer = new KafkaProducer<>(producerProps);
        
        // Initialize transactional producer
        producer.initTransactions();
        
        // Subscribe to input topic
        consumer.subscribe(Collections.singletonList(inputTopic), new LoggingRebalanceListener());
        
        // Start processing thread
        this.processingThread = new Thread(this::processLoop, "kafka-tx-processor");
        this.processingThread.start();
        
        log.info("Started transactional processor: {} → {} with transaction ID: {}", 
                inputTopic, outputTopic, transactionalId);
    }
    
    private void processLoop() {
        while (running.get()) {
            try {
                ConsumerRecords<String, Long> records = consumer.poll(Duration.ofMillis(100));
                
                if (records.isEmpty()) {
                    continue;
                }

                // Only start transaction when there are records
                producer.beginTransaction();

                ProcessingResult result = processRecords(records);

                // Commit consumer offsets to the transaction
                producer.sendOffsetsToTransaction(result.offsetsToCommit, consumer.groupMetadata());

                // Commit the transaction AFTER writing records to output topic
                producer.commitTransaction();

                long committedTx = transactionCount.incrementAndGet();
                if (result.messagesInTransaction > 0) {
                    log.debug("Committed transaction #{} with {} messages", committedTx, result.messagesInTransaction);
                }

                if (processedMessages.get() % 1000 == 0) {
                    log.info("Processed {} messages in {} transactions", 
                            processedMessages.get(), transactionCount.get());
                }

                // Reset retries after a successful commit
                retries = 0;
                
            } catch (AuthorizationException | UnsupportedVersionException | ProducerFencedException
                     | FencedInstanceIdException | OutOfOrderSequenceException | SerializationException e) {
                log.error("Unrecoverable error in transactional processing. Shutting down.", e);
                safeAbortTransaction();
                running.set(false);
            } catch (OffsetOutOfRangeException | NoOffsetForPartitionException e) {
                log.warn(
                        "Offset invalid or not found, seeking to end and committing current position: {}",
                        e.getMessage());
                consumer.seekToEnd(new HashSet<>(consumer.assignment()));
                consumer.commitSync();
                retries = 0;
            } catch (KafkaException e) {
                log.warn("KafkaException during processing, aborting and retrying: {}", e.getMessage());
                safeAbortTransaction();
                restoreFetchPositionToCommitted();
                retries = maybeRetry(retries);
                backoffOnError();
            } catch (Exception e) {
                log.error("Unexpected error in processing loop", e);
                safeAbortTransaction();
                restoreFetchPositionToCommitted();
                retries = maybeRetry(retries);
                backoffOnError();
            }
        }
    }
    
    private ProcessingResult processRecords(ConsumerRecords<String, Long> records) {
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        int messagesInTx = 0;

        for (ConsumerRecord<String, Long> record : records) {
            // Identity processing in this proof: forward the same key/value to the same partition
            String outputKey = record.key();
            Long outputValue = record.value();

            ProducerRecord<String, Long> outputRecord =
                    new ProducerRecord<>(outputTopic, record.partition(), outputKey, outputValue);
            outputRecord.headers().add("originalOffset", String.valueOf(record.offset()).getBytes());
            producer.send(outputRecord);

            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            offsetsToCommit.put(tp, new OffsetAndMetadata(record.offset() + 1));

            messagesInTx++;
            processedMessages.incrementAndGet();
        }

        return new ProcessingResult(offsetsToCommit, messagesInTx);
    }
    
    public long getProcessedMessageCount() {
        return processedMessages.get();
    }
    
    public long getTransactionCount() {
        return transactionCount.get();
    }
    
    public void close() {
        if (running.compareAndSet(true, false)) {
            try {
                // Wait for processing thread to finish
                processingThread.join(5000);
                
                // Close resources
                consumer.close();
                producer.close();
                
                log.info("Closed transactional processor. Processed {} messages in {} transactions", 
                        processedMessages.get(), transactionCount.get());
            } catch (Exception e) {
                log.error("Error closing transactional processor", e);
            }
        }
    }

    private void safeAbortTransaction() {
        try {
            producer.abortTransaction();
        } catch (Exception abortEx) {
            log.error("Failed to abort transaction", abortEx);
        }
    }

    private void restoreFetchPositionToCommitted() {
        try {
            Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(consumer.assignment());
            for (TopicPartition tp : consumer.assignment()) {
                OffsetAndMetadata om = committed.get(tp);
                if (om != null) {
                    consumer.seek(tp, om.offset());
                } else {
                    consumer.seekToBeginning(Collections.singleton(tp));
                }
            }
        } catch (Exception e) {
            log.warn("Failed to restore fetch position to committed offsets", e);
        }
    }

    private int maybeRetry(int currentRetries) {
        if (currentRetries < 0) {
            log.error("The number of retries must be greater than or equal to zero");
            running.set(false);
            return 0;
        }
        if (currentRetries < MAX_RETRIES) {
            return currentRetries + 1;
        } else {
            log.error("Skipping records after {} retries", MAX_RETRIES);
            try {
                consumer.commitSync();
            } catch (Exception e) {
                log.warn("Failed to commit sync when skipping records", e);
            }
            return 0;
        }
    }

    private void backoffOnError() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private class LoggingRebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.info("Revoked partitions: {}", partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.info("Assigned partitions: {}", partitions);
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            log.warn("Lost partitions: {}", partitions);
        }
    }

    private static class ProcessingResult {
        final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit;
        final int messagesInTransaction;

        ProcessingResult(Map<TopicPartition, OffsetAndMetadata> offsetsToCommit, int messagesInTransaction) {
            this.offsetsToCommit = offsetsToCommit;
            this.messagesInTransaction = messagesInTransaction;
        }
    }
}
