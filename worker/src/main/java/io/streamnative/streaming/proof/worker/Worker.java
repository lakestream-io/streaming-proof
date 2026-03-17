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

import io.streamnative.streaming.proof.common.records.ConsumerCheckPoint;
import io.streamnative.streaming.proof.common.records.NewConsumers;
import io.streamnative.streaming.proof.common.records.NewProducers;
import io.streamnative.streaming.proof.common.records.ProducerCheckpoint;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages producer and consumer instances for streaming proof verification on a worker node.
 * 
 * <p>The Worker is a distributed component that:
 * <ul>
 *   <li>Manages multiple concurrent producer and consumer groups</li>
 *   <li>Creates, configures, and monitors messaging clients</li>
 *   <li>Collects detailed checkpoints for verification analysis</li>
 *   <li>Provides thread-safe operation in multi-tenant environments</li>
 *   <li>Optimizes resource usage through driver instance caching</li>
 *   <li>Supports horizontal scaling across multiple worker nodes</li>
 * </ul>
 *
 * <p>Workers operate as part of a distributed verification system where:
 * <ul>
 *   <li>Each worker can be deployed in a different zone/region</li>
 *   <li>Multiple verification tasks can run concurrently on each worker</li>
 *   <li>Producers generate sequentially numbered messages with unique keys</li>
 *   <li>Consumers track received sequences using range-based checkpoints</li>
 *   <li>The coordinator periodically collects these checkpoints for analysis</li>
 * </ul>
 *
 * <p>Each producer and consumer group is identified by a unique ID, allowing
 * multiple verification scenarios to run concurrently on the same worker node.
 *
 * @see ProofProducers
 * @see ProofConsumers
 * @see io.streamnative.streaming.proof.common.records.ProducerCheckpoint
 * @see io.streamnative.streaming.proof.common.records.ConsumerCheckPoint
 * @see io.streamnative.streaming.proof.coordinator.Coordinator
 */
public class Worker {

    /** Thread-safe map of producer groups indexed by their IDs */
    private final Map<String, ProofProducers> producers = new ConcurrentHashMap<>();

    /** Thread-safe map of consumer groups indexed by their IDs */
    private final Map<String, ProofConsumers> consumers = new ConcurrentHashMap<>();

    /** Cache for driver instances to avoid redundant initializations */
    private final DriverCache driverCache = new DriverCache();

    /**
     * Starts a new group of producers with the specified configuration.
     *
     * @param newProducers Configuration for the producer group to start
     */
    public void startProducers(NewProducers newProducers) {
        ProofProducers producer = new ProofProducers(newProducers,
                driverCache.getDriver(newProducers.driverName(), newProducers.driver()));
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
        if (producer != null) {
            producer.stop();
            producers.remove(id);
        }
    }

    /**
     * Retrieves the current checkpoint for a producer group.
     *
     * @param id The unique identifier of the producer group
     * @return A checkpoint containing producer statistics and sequence information
     */
    public ProducerCheckpoint producerCheckPoint(String id) {
        ProofProducers producer = producers.get(id);
        return producer.checkPoint();
    }

    /**
     * Starts a new group of consumers with the specified configuration.
     *
     * @param newConsumers Configuration for the consumer group to start
     */
    public void startConsumers(NewConsumers newConsumers) {
        ProofConsumers consumer = new ProofConsumers(newConsumers,
                driverCache.getDriver(newConsumers.driverName(), newConsumers.driver()));
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
        if (consumer != null) {
            consumer.stop();
            consumers.remove(id);
        }
    }

    /**
     * Retrieves the current checkpoint for a consumer group.
     *
     * @param id The unique identifier of the consumer group
     * @return A checkpoint containing consumer statistics and sequence information
     */
    public ConsumerCheckPoint consumerCheckPoint(String id) {
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
        driverCache.close();
    }

    /**
     * Retrieves detailed checkpoints for each consumer in a consumer group.
     * This provides more granular information than the aggregated checkpoint,
     * allowing analysis of consumption patterns per individual consumer.
     *
     * @param id The unique identifier of the consumer group
     * @return A map of consumer names to their individual checkpoints
     */
    public Map<String, ConsumerCheckPoint> consumerCheckPointDetails(String id) {
        ProofConsumers consumer = consumers.get(id);
        return consumer.checkPointDetails();
    }
}
