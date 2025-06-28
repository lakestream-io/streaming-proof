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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
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
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicLong processedMessages = new AtomicLong(0);
    private final AtomicLong transactionCount = new AtomicLong(0);
    private final Thread processingThread;
    
    public KafkaTransactionalProcessor(Properties baseProps, String topicName) {
        this.inputTopic = topicName + "_transactional";
        this.outputTopic = topicName;
        this.transactionalId = "tx-processor-" + topicName + "-" + UUID.randomUUID();
        
        // Configure consumer
        Properties consumerProps = new Properties();
        consumerProps.putAll(baseProps);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "transactional-processor");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
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
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        
        this.producer = new KafkaProducer<>(producerProps);
        
        // Initialize transactional producer
        producer.initTransactions();
        
        // Subscribe to input topic
        consumer.subscribe(Collections.singletonList(inputTopic));
        
        // Start processing thread
        this.processingThread = new Thread(this::processLoop, "kafka-tx-processor");
        this.processingThread.start();
        
        log.info("Started transactional processor: {} → {} with transaction ID: {}", 
                inputTopic, outputTopic, transactionalId);
    }
    
    private void processLoop() {
        while (running.get()) {
            try {
                // Begin transaction BEFORE consuming records
                producer.beginTransaction();
                long txnId = transactionCount.incrementAndGet();
                log.debug("Started transaction #{}", txnId);
                
                ConsumerRecords<String, Long> records = consumer.poll(Duration.ofMillis(100));
                
                if (!records.isEmpty()) {
                    processRecordsInTransaction(records, txnId);
                } else {
                    // No records, abort empty transaction
                    producer.abortTransaction();
                    transactionCount.decrementAndGet(); // Adjust counter for aborted transaction
                }
                
            } catch (Exception e) {
                if (running.get()) {
                    log.error("Error in processing loop", e);
                    try {
                        producer.abortTransaction();
                    } catch (Exception abortEx) {
                        log.error("Failed to abort transaction", abortEx);
                    }
                    try {
                        Thread.sleep(1000); // Back off on error
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
    }
    
    private void processRecordsInTransaction(ConsumerRecords<String, Long> records, long txnId) {
        try {
            // Process each record within the already-started transaction
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
            int messagesInTx = 0;
            
            for (ConsumerRecord<String, Long> record : records) {
                // Process the message (identity function in this case)
                String outputKey = record.key();
                Long outputValue = record.value();
                
                // Produce to output topic, SAME PARTITION as consumed from
                ProducerRecord<String, Long> outputRecord = 
                    new ProducerRecord<>(outputTopic, record.partition(), outputKey, outputValue);
                producer.send(outputRecord);
                
                // Track offset to commit
                TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                offsetsToCommit.put(tp, new OffsetAndMetadata(record.offset() + 1));
                
                messagesInTx++;
                processedMessages.incrementAndGet();
            }
            
            // Commit consumer offsets to the transaction
            // This is the key part - offsets are committed atomically with produced messages
            producer.sendOffsetsToTransaction(offsetsToCommit, consumer.groupMetadata());
            
            // Commit the transaction AFTER writing records to output topic
            producer.commitTransaction();
            
            log.debug("Committed transaction #{} with {} messages", txnId, messagesInTx);
            
            if (processedMessages.get() % 1000 == 0) {
                log.info("Processed {} messages in {} transactions", 
                        processedMessages.get(), transactionCount.get());
            }
            
        } catch (ProducerFencedException e) {
            log.error("Producer fenced, shutting down", e);
            running.set(false);
        } catch (Exception e) {
            log.error("Error processing records, aborting transaction", e);
            try {
                producer.abortTransaction();
            } catch (Exception abortEx) {
                log.error("Failed to abort transaction", abortEx);
            }
            throw e; // Re-throw to trigger outer exception handling
        }
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
}