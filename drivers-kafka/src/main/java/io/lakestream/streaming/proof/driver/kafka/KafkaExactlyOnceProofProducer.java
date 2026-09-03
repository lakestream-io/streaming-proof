/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package io.lakestream.streaming.proof.driver.kafka;

import io.lakestream.streaming.proof.common.MessageMetadata;
import io.lakestream.streaming.proof.common.ProofProducer;
import io.lakestream.streaming.proof.common.ProofValue;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Kafka exactly-once producer implementation that combines message production with
 * embedded transactional processing for exactly-once semantics verification.
 *
 * <p>This producer implements the complete exactly-once pattern:
 * <ul>
 *   <li>Produces messages to input topic ({topicName}_transactional)</li>
 *   <li>Runs embedded {@link KafkaTransactionalProcessor} for atomic read-process-write</li>
 *   <li>Transactional processor reads from input topic and writes to output topic atomically</li>
 *   <li>Consumers verify exactly-once semantics from the output topic</li>
 * </ul>
 *
 * <p>Message flow:
 * <pre>
 * Producer → {topic}_transactional → TransactionalProcessor → {topic} → Consumer
 * </pre>
 *
 * <p>This approach ensures true exactly-once processing by using Kafka's transactional
 * features to atomically commit consumer offsets and producer messages together.
 *
 * @see KafkaTransactionalProcessor
 * @see ProofProducer
 */
@Slf4j
public class KafkaExactlyOnceProofProducer implements ProofProducer {

    /** The underlying Kafka producer for writing to input topic */
    private final KafkaProducer<String, ProofValue> producer;

    /** The input topic where this producer writes messages */
    private final String inputTopic;

    /** The output topic where consumers read processed messages */
    private final String outputTopic;

    /** Total size in bytes of each message value, including the sequence number */
    private final int messageSize;

    /** Embedded transactional processor for exactly-once processing */
    private final KafkaTransactionalProcessor transactionalProcessor;

    /**
     * Creates a new exactly-once producer with embedded transactional processing.
     *
     * @param producer The Kafka producer instance for writing to input topic
     * @param baseProps Base Kafka properties for configuring the transactional processor
     * @param outputTopicName The output topic name (input topic will be {outputTopicName}_transactional)
     * @param messageSize Total size in bytes of each message value
     */
    public KafkaExactlyOnceProofProducer(KafkaProducer<String, ProofValue> producer,
                                         Properties baseProps,
                                         String outputTopicName,
                                         int messageSize) {
        this.producer = producer;
        this.outputTopic = outputTopicName;
        this.inputTopic = outputTopicName + "_transactional";
        this.messageSize = messageSize;
        
        // Start embedded transactional processor
        this.transactionalProcessor = new KafkaTransactionalProcessor(baseProps, outputTopicName);
        
        log.info("Created exactly-once producer: {} → {} → {}", 
                getClass().getSimpleName(), inputTopic, outputTopic);
    }

    /**
     * Sends a message to the input topic for transactional processing.
     * The message will be processed by the embedded transactional processor
     * and written to the output topic with exactly-once guarantees.
     *
     * @param key The message key
     * @param value The message value (sequence number)
     * @return CompletableFuture containing the message metadata
     */
    @Override
    public CompletableFuture<MessageMetadata> sendAsync(String key, long value) {
        ProducerRecord<String, ProofValue> record =
                new ProducerRecord<>(inputTopic, key, new ProofValue(value, messageSize));
        
        CompletableFuture<MessageMetadata> future = new CompletableFuture<>();
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                future.completeExceptionally(exception);
            } else {
                // Convert Kafka RecordMetadata to MessageMetadata
                MessageMetadata messageMetadata = new MessageMetadata(
                    metadata.offset(),
                    metadata.partition()
                );
                future.complete(messageMetadata);
            }
        });
        
        return future;
    }

    /**
     * Closes the producer and stops the embedded transactional processor.
     * This ensures graceful shutdown of both the producer and the transactional processing.
     *
     * @throws IOException if an error occurs during shutdown
     */
    @Override
    public void close() throws IOException {
        try {
            // Stop transactional processor first
            if (transactionalProcessor != null) {
                transactionalProcessor.close();
                log.info("Stopped embedded transactional processor for topic: {}", outputTopic);
            }
            
            // Close producer
            if (producer != null) {
                producer.close();
                log.info("Closed exactly-once producer for topic: {}", outputTopic);
            }
        } catch (Exception e) {
            throw new IOException("Failed to close exactly-once producer", e);
        }
    }

    /**
     * Gets statistics from the embedded transactional processor.
     *
     * @return Number of messages processed by the transactional processor
     */
    public long getProcessedMessageCount() {
        return transactionalProcessor != null ? transactionalProcessor.getProcessedMessageCount() : 0;
    }

    /**
     * Gets transaction count from the embedded transactional processor.
     *
     * @return Number of transactions committed by the transactional processor
     */
    public long getTransactionCount() {
        return transactionalProcessor != null ? transactionalProcessor.getCommitCount() : 0;
    }
}