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

import io.streamnative.streaming.proof.common.records.Checkpoint;
import io.streamnative.streaming.proof.common.records.NewConsumers;
import io.streamnative.streaming.proof.common.records.NewProducers;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages producer and consumer groups for streaming proof tests on a worker node.
 * This class acts as a container for multiple producer and consumer instances,
 * providing lifecycle management and monitoring capabilities.
 *
 * <p>The Worker class is responsible for:
 * <ul>
 *   <li>Managing multiple producer and consumer groups</li>
 *   <li>Starting and stopping producer/consumer instances</li>
 *   <li>Collecting checkpoints for verification</li>
 *   <li>Thread-safe operation management</li>
 * </ul>
 *
 * <p>Each producer and consumer group is identified by a unique ID, allowing
 * multiple test scenarios to run concurrently on the same worker node.
 *
 * <p>Example usage:
 * <pre>{@code
 * Worker worker = new Worker();
 * 
 * // Start producers
 * NewProducers producers = NewProducers.builder()
 *     .id("test-1")
 *     .topic("test-topic")
 *     .producers(4)
 *     .keys(100)
 *     .msgRate(5000)
 *     .driver(kafkaDriver)
 *     .build();
 * worker.startProducers(producers);
 * 
 * // Start consumers
 * NewConsumers consumers = NewConsumers.builder()
 *     .id("test-1")
 *     .topic("test-topic")
 *     .consumers(4)
 *     .driver(kafkaDriver)
 *     .build();
 * worker.startConsumers(consumers);
 * 
 * // Monitor progress
 * CheckPoint producerStats = worker.producerCheckPoint("test-1");
 * CheckPoint consumerStats = worker.consumerCheckPoint("test-1");
 * 
 * // Clean up
 * worker.stopProducers("test-1");
 * worker.stopConsumers("test-1");
 * }</pre>
 *
 * @see ProofProducers
 * @see ProofConsumers
 * @see Checkpoint
 */
public class Worker {

    /** Thread-safe map of producer groups indexed by their IDs */
    private final Map<String, ProofProducers> producers = new ConcurrentHashMap<>();

    /** Thread-safe map of consumer groups indexed by their IDs */
    private final Map<String, ProofConsumers> consumers = new ConcurrentHashMap<>();

    /**
     * Starts a new group of producers with the specified configuration.
     *
     * @param newProducers Configuration for the producer group to start
     */
    public void startProducers(NewProducers newProducers) {
        ProofProducers producer = new ProofProducers(newProducers);
        producers.put(newProducers.id(), producer);
        producer.start();
    }

    /**
     * Stops a group of producers identified by the given ID.
     *
     * @param id The unique identifier of the producer group to stop
     */
    public void stopProducers(String id) {
        ProofProducers producer = producers.get(id);
        producer.stop();
    }

    /**
     * Retrieves the current checkpoint for a producer group.
     *
     * @param id The unique identifier of the producer group
     * @return A checkpoint containing producer statistics and sequence information
     */
    public Checkpoint producerCheckPoint(String id) {
        ProofProducers producer = producers.get(id);
        return producer.checkPoint();
    }

    /**
     * Starts a new group of consumers with the specified configuration.
     *
     * @param newConsumers Configuration for the consumer group to start
     */
    public void startConsumers(NewConsumers newConsumers) {
        ProofConsumers consumer = new ProofConsumers(newConsumers);
        consumers.put(newConsumers.id(), consumer);
        consumer.start();
    }

    /**
     * Stops a group of consumers identified by the given ID.
     *
     * @param id The unique identifier of the consumer group to stop
     */
    public void stopConsumers(String id) {
        ProofConsumers consumer = consumers.get(id);
        consumer.stop();
    }

    /**
     * Retrieves the current checkpoint for a consumer group.
     *
     * @param id The unique identifier of the consumer group
     * @return A checkpoint containing consumer statistics and sequence information
     */
    public Checkpoint consumerCheckPoint(String id) {
        ProofConsumers consumer = consumers.get(id);
        return consumer.checkPoint();
    }

    /**
     * Performs cleanup by stopping all producer and consumer groups managed by this worker.
     * This method ensures proper shutdown by iterating through all registered producers
     * and consumers and stopping them.
     *
     * <p>This method should be called when shutting down the worker node to ensure
     * all resources are properly released and all message processing is stopped.
     */
    public void close() {
        producers.values().forEach(ProofProducers::stop);
        consumers.values().forEach(ProofConsumers::stop);
    }
}
