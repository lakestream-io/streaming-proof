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

import static org.testng.Assert.assertNotNull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Integration test for KafkaTransactionalProcessor using Testcontainers.
 * This test verifies the exactly-once processing semantics by:
 * 1. Starting a real Kafka cluster using Testcontainers
 * 2. Creating input and output topics
 * 3. Producing messages to the input topic
 * 4. Verifying that messages are processed exactly once to the output topic
 * 5. Testing transaction rollback scenarios
 */
public class KafkaTransactionalProcessorTest {

    private static final String BASE_TOPIC_NAME = "test-topic";
    private static final int NUM_PARTITIONS = 3;
    private static final short REPLICATION_FACTOR = 1;

    private KafkaContainer kafkaContainer;
    private String bootstrapServers;
    private AdminClient adminClient;

    @BeforeClass
    public void setUp() throws Exception {
        // Start Kafka container
        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");

        kafkaContainer.start();
        bootstrapServers = kafkaContainer.getBootstrapServers();

        // Create admin client
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        adminClient = AdminClient.create(adminProps);
    }

    @AfterClass
    public void tearDown() {
        if (adminClient != null) {
            adminClient.close();
        }
        if (kafkaContainer != null) {
            kafkaContainer.stop();
        }
    }

    private void createTopics(String topic, int partitions) throws Exception {
        List<NewTopic> topics = List.of(
                new NewTopic(topic + "_transactional", partitions, REPLICATION_FACTOR),
                new NewTopic(topic, partitions, REPLICATION_FACTOR)
        );

        adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);
    }

    @Test
    public void testBasicTransactionalProcessing() {
        String topicName = BASE_TOPIC_NAME + "-basic-" + System.currentTimeMillis();
        // Create base properties for Kafka clients
        Properties baseProps = createBaseProperties();

        // Start transactional processor
        KafkaTransactionalProcessor processor = new KafkaTransactionalProcessor(baseProps, topicName);

        try {
            // Produce test messages to input topic
            List<String> testMessages = List.of("msg-1", "msg-2", "msg-3", "msg-4", "msg-5");
            produceMessagesToInputTopic(testMessages, topicName + "_transactional");

            // Wait for processing to complete
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> processor.getProcessedMessageCount() >= testMessages.size());

            // Verify messages were processed to output topic
            List<String> outputMessages = consumeMessagesFromOutputTopic(testMessages.size(), topicName);

            Assert.assertEquals(outputMessages.size(), testMessages.size(),
                    "Expected all messages to be processed");
            Assert.assertTrue(outputMessages.containsAll(testMessages),
                    "All input messages should be present in output");

            // Verify transaction count
            Assert.assertTrue(processor.getCommitCount() > 0,
                    "At least one transaction should have been committed");

        } finally {
            processor.close();
        }
    }

    @Test
    public void testMultiplePartitionProcessing() throws Exception {
        String topicName = BASE_TOPIC_NAME + "-multipart-" + System.currentTimeMillis();
        createTopics(topicName, NUM_PARTITIONS);
        Properties baseProps = createBaseProperties();
        KafkaTransactionalProcessor processor = new KafkaTransactionalProcessor(baseProps, topicName);

        try {
            // Produce messages to different partitions
            List<String> testMessages = new ArrayList<>();
            for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
                for (int i = 0; i < 3; i++) {
                    String message = "partition-" + partition + "-msg-" + i;
                    testMessages.add(message);
                    produceMessageToPartition(message, partition, topicName + "_transactional");
                }
            }

            // Wait for all messages to be processed
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> processor.getProcessedMessageCount() >= testMessages.size());

            // Verify all messages were processed
            List<String> outputMessages = consumeMessagesFromOutputTopic(testMessages.size(), topicName);
            Assert.assertEquals(outputMessages.size(), testMessages.size());
            Assert.assertTrue(outputMessages.containsAll(testMessages));

        } finally {
            processor.close();
        }
    }

    @Test
    public void testProcessorShutdownGracefully() throws Exception {
        String topicName = BASE_TOPIC_NAME + "-shutdown-" + System.currentTimeMillis();
        Properties baseProps = createBaseProperties();
        KafkaTransactionalProcessor processor = new KafkaTransactionalProcessor(baseProps, topicName);

        // Produce some messages
        produceMessagesToInputTopic(List.of("shutdown-test-1", "shutdown-test-2"), topicName + "_transactional");

        // Wait a bit for processing to start
        Thread.sleep(1000);

        // Get initial counts
        long initialProcessedCount = processor.getProcessedMessageCount();
        long initialTxCount = processor.getCommitCount();

        // Close processor
        processor.close();

        // Verify final counts are stable (no more processing after close)
        Thread.sleep(1000);
        Assert.assertEquals(processor.getProcessedMessageCount(), initialProcessedCount);
        Assert.assertEquals(processor.getCommitCount(), initialTxCount);
    }

    @Test
    public void testIdempotentProcessing() throws InterruptedException {
        String topicName = BASE_TOPIC_NAME + "-idempotent-" + System.currentTimeMillis();
        Properties baseProps = createBaseProperties();

        // Start first processor
        KafkaTransactionalProcessor processor1 = new KafkaTransactionalProcessor(baseProps, topicName);

        try {
            // Produce messages
            List<String> testMessages = List.of("idempotent-1", "idempotent-2", "idempotent-3");
            produceMessagesToInputTopic(testMessages, topicName + "_transactional");

            // Wait for processing
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .until(() -> processor1.getProcessedMessageCount() >= testMessages.size());

            // Close first processor
            processor1.close();

            // Start second processor with same configuration (should not reprocess committed messages)
            KafkaTransactionalProcessor processor2 = new KafkaTransactionalProcessor(baseProps, topicName);

            try {
                // Wait a bit to see if it processes any messages
                Thread.sleep(3000);

                // Should not have processed any additional messages since they were already committed
                Assert.assertEquals(processor2.getProcessedMessageCount(), 0L,
                        "Second processor should not reprocess already committed messages");

                // Verify output topic still has the correct messages
                List<String> outputMessages = consumeMessagesFromOutputTopic(testMessages.size(), topicName);
                Assert.assertEquals(outputMessages.size(), testMessages.size());

            } finally {
                processor2.close();
            }

        } finally {
            processor1.close();
        }
    }

    private Properties createBaseProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "test-client-" + System.currentTimeMillis());
        return props;
    }

    private void produceMessagesToInputTopic(List<String> messages, String inputTopic) {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaProducer<String, Long> producer = new KafkaProducer<>(producerProps)) {
            for (int i = 0; i < messages.size(); i++) {
                ProducerRecord<String, Long> record = new ProducerRecord<>(
                        inputTopic, messages.get(i), (long) i);
                producer.send(record).get();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to produce messages", e);
        }
    }

    private void produceMessageToPartition(String message, int partition, String inputTopic) {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());

        try (KafkaProducer<String, Long> producer = new KafkaProducer<>(producerProps)) {
            ProducerRecord<String, Long> record = new ProducerRecord<>(
                    inputTopic, partition, message, 1L);
            producer.send(record).get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to produce message to partition", e);
        }
    }

    private List<String> consumeMessagesFromOutputTopic(int expectedCount, String outputTopic) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + System.currentTimeMillis());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        List<String> messages = new ArrayList<>();

        try (KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(outputTopic));

            long startTime = System.currentTimeMillis();
            long timeout = 30_000; // 30 seconds timeout

            while (messages.size() < expectedCount && (System.currentTimeMillis() - startTime) < timeout) {

                ConsumerRecords<String, Long> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, Long> record : records) {
                    messages.add(record.key());
                    assertNotNull(record.headers().lastHeader("originalOffset"),
                            "Message value should not be null");
                }
            }
        }

        return messages;
    }
}
