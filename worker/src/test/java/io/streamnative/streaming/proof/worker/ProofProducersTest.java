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
package io.streamnative.streaming.proof.worker;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.streamnative.streaming.proof.common.MessageListener;
import io.streamnative.streaming.proof.common.MessageMetadata;
import io.streamnative.streaming.proof.common.ProofConsumer;
import io.streamnative.streaming.proof.common.ProofDriver;
import io.streamnative.streaming.proof.common.ProofProducer;
import io.streamnative.streaming.proof.common.records.Driver;
import io.streamnative.streaming.proof.common.records.ErrorOccurrence;
import io.streamnative.streaming.proof.common.records.NewProducers;
import io.streamnative.streaming.proof.common.records.ProducerCheckpoint;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.Test;

public class ProofProducersTest {

    @Test
    public void testStartWithFewerKeysThanProducers() throws Exception {
        StubProofDriver driver = new StubProofDriver();
        NewProducers newProducers = new NewProducers(
                "proof-1",
                "topic-1",
                4,
                2,
                1000,
                "stub",
                new Driver("stub", Map.of()),
                false);

        ProofProducers proofProducers = new ProofProducers(newProducers, driver);
        try {
            proofProducers.start();

            ProducerCheckpoint checkpoint = waitForCheckpoint(proofProducers);
            assertEquals(driver.getCreatedProducerCount(), 2);
            assertTrue(checkpoint.getPublished().size() > 0);
            assertTrue(checkpoint.getPublished().size() <= 2);
        } finally {
            proofProducers.stop();
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testStartRequiresAtLeastOneKey() {
        StubProofDriver driver = new StubProofDriver();
        NewProducers newProducers = new NewProducers(
                "proof-2",
                "topic-2",
                1,
                0,
                1000,
                "stub",
                new Driver("stub", Map.of()),
                false);

        ProofProducers proofProducers = new ProofProducers(newProducers, driver);
        try {
            proofProducers.start();
        } finally {
            proofProducers.stop();
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testProducerTaskRequiresAtLeastOneKey() {
        new ProofProducerTask(new StubProofProducer(), 0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testStartRequiresAtLeastOneProducer() {
        StubProofDriver driver = new StubProofDriver();
        NewProducers newProducers = new NewProducers(
                "proof-3",
                "topic-3",
                0,
                1,
                1000,
                "stub",
                new Driver("stub", Map.of()),
                false);

        ProofProducers proofProducers = new ProofProducers(newProducers, driver);
        try {
            proofProducers.start();
        } finally {
            proofProducers.stop();
        }
    }

    @Test
    public void testCheckpointIncludesProducerErrorTimingDetails() throws Exception {
        FailingProofDriver driver = new FailingProofDriver();
        NewProducers newProducers = new NewProducers(
                "proof-4",
                "topic-4",
                1,
                1,
                1000,
                "stub",
                new Driver("stub", Map.of()),
                false);

        ProofProducers proofProducers = new ProofProducers(newProducers, driver);
        try {
            proofProducers.start();

            ProducerCheckpoint checkpoint = waitForErrorCheckpoint(proofProducers);
            ErrorOccurrence occurrence = checkpoint.getErrorDetails().get("simulated failure");
            assertTrue(occurrence.getCount() >= 1);
            assertTrue(occurrence.getFirstSeenAtMillis() > 0L);
            assertTrue(occurrence.getLastSeenAtMillis() >= occurrence.getFirstSeenAtMillis());
        } finally {
            proofProducers.stop();
        }
    }

    private ProducerCheckpoint waitForCheckpoint(ProofProducers proofProducers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 1_000;
        ProducerCheckpoint checkpoint = proofProducers.checkPoint();
        while (checkpoint.getPublished().isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
            checkpoint = proofProducers.checkPoint();
        }
        return checkpoint;
    }

    private ProducerCheckpoint waitForErrorCheckpoint(ProofProducers proofProducers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 1_000;
        ProducerCheckpoint checkpoint = proofProducers.checkPoint();
        while (checkpoint.getErrorDetails().isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
            checkpoint = proofProducers.checkPoint();
        }
        return checkpoint;
    }

    private static final class StubProofDriver implements ProofDriver {
        private final List<StubProofProducer> producers = new CopyOnWriteArrayList<>();

        @Override
        public void init(Map<String, Object> configs) {
        }

        @Override
        public void createTopic(String topicName, int partitions, Map<String, String> configs) {
        }

        @Override
        public void deleteTopic(String topicName) {
        }

        @Override
        public ProofProducer createProducer(String topicName, Map<String, Object> configs) {
            StubProofProducer producer = new StubProofProducer();
            producers.add(producer);
            return producer;
        }

        @Override
        public ProofConsumer createConsumer(String topicName, int partitions, long consumeDelayMs,
                                            Map<String, Object> configs, MessageListener listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }

        int getCreatedProducerCount() {
            return producers.size();
        }
    }

    private static final class StubProofProducer implements ProofProducer {
        private final AtomicLong offset = new AtomicLong();

        @Override
        public CompletableFuture<MessageMetadata> sendAsync(String key, long value) {
            return CompletableFuture.completedFuture(new MessageMetadata(offset.getAndIncrement()));
        }

        @Override
        public void close() {
        }
    }

    private static final class FailingProofDriver implements ProofDriver {
        @Override
        public void init(Map<String, Object> configs) {
        }

        @Override
        public void createTopic(String topicName, int partitions, Map<String, String> configs) {
        }

        @Override
        public void deleteTopic(String topicName) {
        }

        @Override
        public ProofProducer createProducer(String topicName, Map<String, Object> configs) {
            return new FailingProofProducer();
        }

        @Override
        public ProofConsumer createConsumer(String topicName, int partitions, long consumeDelayMs,
                                            Map<String, Object> configs, MessageListener listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }

    private static final class FailingProofProducer implements ProofProducer {
        @Override
        public CompletableFuture<MessageMetadata> sendAsync(String key, long value) {
            return CompletableFuture.failedFuture(new RuntimeException("simulated failure"));
        }

        @Override
        public void close() {
        }
    }
}
