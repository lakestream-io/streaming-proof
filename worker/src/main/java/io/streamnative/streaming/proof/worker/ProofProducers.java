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
package io.streamnative.streaming.proof.worker;

import io.streamnative.streaming.proof.common.LongSeq;
import io.streamnative.streaming.proof.common.ProofDriver;
import io.streamnative.streaming.proof.common.ProofProducer;
import io.streamnative.streaming.proof.common.records.Checkpoint;
import io.streamnative.streaming.proof.common.records.Driver;
import io.streamnative.streaming.proof.common.records.NewProducers;
import io.streamnative.streaming.proof.driver.kafka.KafkaProofDriver;
import io.streamnative.streaming.proof.driver.pulsar.PulsarProofDriver;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages a group of producer tasks for streaming proof tests. This class coordinates
 * multiple producers to generate messages at a controlled rate while maintaining
 * sequence tracking for verification.
 *
 * <p>Key features:
 * <ul>
 *   <li>Rate-limited message production using {@link UniformRateLimiter}</li>
 *   <li>Fair distribution of keys across multiple producers</li>
 *   <li>Sequence tracking per message key</li>
 *   <li>Aggregated producer statistics collection</li>
 * </ul>
 *
 * <p>The class uses a single executor thread to coordinate message production across
 * all producer tasks, ensuring controlled message rates and fair task scheduling.
 *
 * <p>Example usage:
 * <pre>{@code
 * Driver kafkaDriver = new Driver("kafka", kafkaConfigs);
 * NewProducers config = new NewProducers(
 *     "test-producers",  // unique group ID
 *     "test-topic",     // topic to produce to
 *     4,                // number of producers
 *     100,              // number of unique keys
 *     5000,             // messages per second
 *     kafkaDriver       // messaging system driver
 * );
 * 
 * ProofProducers producers = new ProofProducers(config);
 * producers.start();
 * 
 * // Later, check production progress
 * CheckPoint checkpoint = producers.checkPoint();
 * 
 * // Finally, stop all producers
 * producers.stop();
 * }</pre>
 *
 * @see ProofProducerTask
 * @see UniformRateLimiter
 * @see ProofDriver
 */
public class ProofProducers {

    /** Configuration for creating new producers */
    private final NewProducers newProducers;
    
    /** The messaging system driver instance */
    private ProofDriver driver;
    
    /** List of running producer tasks */
    private final List<ProofProducerTask> tasks;
    
    /** Single-threaded executor for coordinated message production */
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    
    /** Counter for round-robin task selection */
    private final AtomicLong index = new AtomicLong(0);
    
    /** Flag controlling the producer group's lifecycle */
    private final AtomicBoolean running = new AtomicBoolean(true);
    
    /** Rate limiter for controlling message production speed */
    private final UniformRateLimiter rateLimiter;

    /**
     * Creates a new ProofProducers instance with the specified configuration.
     *
     * @param newProducers Configuration specifying the number of producers,
     *                     message rate, and driver settings
     */
    public ProofProducers(NewProducers newProducers) {
        this.newProducers = newProducers;
        this.tasks = new ArrayList<>(newProducers.producers());
        this.rateLimiter = new UniformRateLimiter(newProducers.msgRate());
    }

    /**
     * Initializes the driver and creates producer tasks with evenly distributed keys.
     * This method is called during start-up to prepare the producer group for operation.
     *
     * @throws IllegalArgumentException if the specified driver type is not supported
     */
    private void init() {
        Driver d = newProducers.driver();
        if (null == driver) {
            if ("kafka".equals(d.driverType())) {
                this.driver = new KafkaProofDriver();
            } else if ("pulsar".equals(d.driverType())) {
                this.driver = new PulsarProofDriver();
            } else {
                throw new IllegalArgumentException("Unsupported driver: " + d.driverType());
            }
        }

        // Calculate keys per producer
        int totalKeys = newProducers.keys();
        int producerCount = newProducers.producers();
        int baseKeysPerProducer = totalKeys / producerCount;
        int remainder = totalKeys % producerCount;

        // Create tasks with their respective key counts
        for (int i = 0; i < producerCount; i++) {
            ProofProducer producer = driver.createProducer(newProducers.topic(), d.driverConfigs());
            int keyCount = baseKeysPerProducer + (i == producerCount - 1 ? remainder : 0);
            tasks.add(new ProofProducerTask(producer, keyCount));
        }
    }

    /**
     * Collects and aggregates checkpoints from all producer tasks.
     * The checkpoint includes:
     * <ul>
     *   <li>Latest sequence number per key</li>
     *   <li>Count of failed sends</li>
     *   <li>List of failed sequence numbers</li>
     * </ul>
     *
     * @return A checkpoint containing aggregated producer statistics
     */
    public Checkpoint checkPoint() {
        Map<String, LongSeq> keySeqs = new HashMap<>();
        Map<String, Integer> errors = new HashMap<>();
        Map<String, List<List<LongSeq>>> outOfOrderOffsets = new HashMap<>();
        for (ProofProducerTask task : tasks) {
            keySeqs.putAll(task.getLastPublished());
            task.getErrors().forEach((k, v) -> errors.merge(k, v, Integer::sum));
            outOfOrderOffsets.putAll(task.getOutOfOrderOffsets());
        }
        return new Checkpoint(keySeqs, null, errors, null, outOfOrderOffsets);
    }

    /**
     * Stops all producer tasks and releases associated resources.
     * This method ensures graceful shutdown of the executor and all producer tasks.
     *
     * @throws RuntimeException if an error occurs while closing producers
     */
    public void stop() {
        running.set(false);
        executor.shutdown();
        try {
            for (ProofProducerTask task : tasks) {
                task.close();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Starts the producer group and begins message production.
     * Messages are produced at the configured rate using the rate limiter,
     * with tasks selected in round-robin fashion.
     */
    public void start() {
        init();
        executor.execute(() -> {
            while (running.get()) {
                final long intendedSendTime = rateLimiter.acquire();
                UniformRateLimiter.uninterruptibleSleepNs(intendedSendTime);
                tasks.get((int) (index.getAndIncrement() % tasks.size())).sendAsync();
            }
        });
    }
}
