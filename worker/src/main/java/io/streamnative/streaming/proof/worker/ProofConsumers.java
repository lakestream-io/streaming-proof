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

import io.streamnative.streaming.proof.common.ProofConsumer;
import io.streamnative.streaming.proof.common.ProofDriver;
import io.streamnative.streaming.proof.common.records.Checkpoint;
import io.streamnative.streaming.proof.common.records.NewConsumers;
import io.streamnative.streaming.proof.driver.kafka.KafkaProofDriver;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manages a group of consumer tasks for streaming proof tests. This class is responsible
 * for creating, starting, monitoring, and stopping multiple consumer instances that verify
 * message delivery guarantees.
 *
 * <p>The class provides functionality for:
 * <ul>
 *   <li>Creating and managing multiple consumer tasks</li>
 *   <li>Initializing appropriate messaging system driver</li>
 *   <li>Collecting and aggregating consumer statistics</li>
 *   <li>Monitoring message delivery guarantees</li>
 * </ul>
 *
 * <p>Each consumer task tracks:
 * <ul>
 *   <li>Message sequences per key</li>
 *   <li>Duplicate messages</li>
 *   <li>Out-of-order messages</li>
 *   <li>Missing messages in sequences</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * Driver kafkaDriver = new Driver("kafka", kafkaConfigs);
 * NewConsumers config = new NewConsumers(
 *     "consumer-group-1",  // unique group ID
 *     "test-topic",       // topic to consume from
 *     4,                  // number of consumers
 *     kafkaDriver         // messaging system driver
 * );
 * 
 * ProofConsumers consumers = new ProofConsumers(config);
 * consumers.start();
 * 
 * // Later, check consumption progress
 * CheckPoint checkpoint = consumers.checkPoint();
 * 
 * // Finally, stop all consumers
 * consumers.stop();
 * }</pre>
 *
 * @see ProofConsumerTask
 * @see ProofDriver
 * @see Checkpoint
 */
public class ProofConsumers {

    /** Configuration for creating new consumers */
    private final NewConsumers newConsumers;
    
    /** The messaging system driver instance */
    private ProofDriver driver;
    
    /** List of running consumer tasks */
    private final List<ProofConsumerTask> tasks;

    /**
     * Creates a new ProofConsumers instance with the specified configuration.
     *
     * @param newConsumers Configuration specifying the number of consumers,
     *                    topic, and driver settings
     */
    public ProofConsumers(NewConsumers newConsumers) {
        this.newConsumers = newConsumers;
        tasks = new ArrayList<>(newConsumers.consumers());
    }

    /**
     * Initializes the driver and starts all consumer tasks.
     * Creates the specified number of consumers and assigns them to consume
     * from the configured topic.
     *
     * @throws IllegalArgumentException if the specified driver type is not supported
     */
    public void start() {
        String driverType = newConsumers.driver().driverType();
        if (null == driver) {
            if ("kafka".equals(driverType)) {
                this.driver = new KafkaProofDriver();
            } else {
                throw new IllegalArgumentException("Unsupported driver: " + driverType);
            }
        }
        for (int i = 0; i < newConsumers.consumers(); i++) {
            ProofConsumerTask task = new ProofConsumerTask();
            ProofConsumer proofConsumer = driver.createConsumer(newConsumers.topic(), newConsumers.partitions(),
                    newConsumers.driver().driverConfigs(), task);
            task.setConsumer(proofConsumer);
            tasks.add(task);
        }
    }

    /**
     * Collects and aggregates checkpoints from all consumer tasks.
     * The checkpoint includes:
     * <ul>
     *   <li>Latest sequence number per key</li>
     *   <li>Count of duplicate messages</li>
     *   <li>Count of out-of-order messages</li>
     *   <li>Count of missed messages</li>
     * </ul>
     *
     * @return A checkpoint containing aggregated consumer statistics
     */
    public Checkpoint checkPoint() {
        Checkpoint checkPoint = Checkpoint.empty();
        for (ProofConsumerTask task : tasks) {
            Checkpoint c = new Checkpoint(task.getKeySeq(), task.getDups(), null,
                    task.getMissedSeqs(), task.getOutOfOrderSeqs());
            checkPoint.merge(c);
        }
        return checkPoint;
    }

    public Map<String, Checkpoint> checkPointDetails() {
        Map<String, Checkpoint> result = new HashMap<>();
        for (ProofConsumerTask task : tasks) {
            result.put(task.getConsumerName(), new Checkpoint(task.getKeySeq(), task.getDups(), null,
                    task.getMissedSeqs(), task.getOutOfOrderSeqs()));
        }
        return result;
    }

    /**
     * Stops all consumer tasks and releases associated resources.
     *
     * @throws RuntimeException if an error occurs while closing consumers
     */
    public void stop() {
        try {
            for (ProofConsumerTask task : tasks) {
                task.close();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
