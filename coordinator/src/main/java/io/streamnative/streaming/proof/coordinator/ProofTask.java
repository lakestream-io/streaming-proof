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
package io.streamnative.streaming.proof.coordinator;

import io.streamnative.streaming.proof.common.LongSeq;
import io.streamnative.streaming.proof.common.ProofDriver;
import io.streamnative.streaming.proof.common.WorkerHttpClient;
import io.streamnative.streaming.proof.common.records.Checkpoint;
import io.streamnative.streaming.proof.common.records.Configs;
import io.streamnative.streaming.proof.common.records.Driver;
import io.streamnative.streaming.proof.common.records.NewConsumers;
import io.streamnative.streaming.proof.common.records.NewProducers;
import io.streamnative.streaming.proof.common.records.Proof;
import io.streamnative.streaming.proof.common.records.ProofSummary;
import io.streamnative.streaming.proof.driver.kafka.KafkaProofDriver;
import io.streamnative.streaming.proof.driver.pulsar.PulsarProofDriver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.asynchttpclient.Dsl;

/**
 * Manages the execution and monitoring of a single streaming proof test.
 * This class coordinates the producers and consumers across multiple workers,
 * tracks message delivery progress, and verifies messaging guarantees.
 *
 * <p>The task is responsible for:
 * <ul>
 *   <li>Initializing and managing the messaging system driver</li>
 *   <li>Distributing producers and consumers across worker nodes</li>
 *   <li>Collecting and aggregating checkpoints from all workers</li>
 *   <li>Verifying message delivery guarantees</li>
 *   <li>Tracking test progress and failures</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * Proof proof = Proof.builder()
 *     .name("ordering-test")
 *     .driver("kafka")
 *     .topic("test-topic")
 *     .build();
 * Configs configs = new Configs(...);
 * 
 * ProofTask task = new ProofTask(proof, configs);
 * task.start();
 * 
 * // Later...
 * task.stop();
 * task.remove();
 * }</pre>
 */
@Slf4j
@Getter
public class ProofTask {

    /** The proof test configuration */
    private final Proof proof;
    
    /** Executor for scheduling periodic checkpoint verification */
    private final ScheduledExecutorService executor;
    
    /** System-wide configuration for workers and drivers */
    private final Configs configs;
    
    /** The messaging system driver implementation */
    private ProofDriver proofDriver;
    
    /** List of HTTP clients for communicating with worker nodes */
    private final List<WorkerHttpClient> clients;
    
    /** Current checkpoint being verified */
    private Checkpoint inCheck = Checkpoint.empty();
    
    /** Last successfully verified producer checkpoint */
    private Checkpoint lastVerifiedProducerCheckpoint = Checkpoint.empty();
    
    /** Last successfully verified consumer checkpoint */
    private Checkpoint lastVerifiedConsumerCheckpoint = Checkpoint.empty();
    
    /** Latest producer checkpoint received */
    private Checkpoint latestProducerCheckpoint = Checkpoint.empty();
    
    /** Latest consumer checkpoint received */
    @Setter(AccessLevel.PACKAGE)
    private Checkpoint latestConsumerCheckpoint = Checkpoint.empty();
    
    /** Last failed producer checkpoint */
    private Checkpoint lastFailedProducerCheckpoint = Checkpoint.empty();
    
    /** Last failed consumer checkpoint */
    private Checkpoint lastFailedConsumerCheckpoint = Checkpoint.empty();
    
    /** Number of checkpoint verification timeouts */
    private int timeouts;
    
    /** Timestamp of when the current checkpoint verification started */
    private long checkPointInCheckTimeStamps;

    private boolean failed = false;

    /**
     * Creates a new proof task with the specified configuration.
     *
     * @param proof The proof test configuration
     * @param configs System-wide configuration for workers and drivers
     */
    public ProofTask(Proof proof, Configs configs) {
        this.proof = proof;
        this.executor = Executors.newSingleThreadScheduledExecutor();
        this.configs = configs;
        this.clients = new ArrayList<>(configs.workers().size());
        configs.workers().forEach((k, v) -> {
            WorkerHttpClient client = new WorkerHttpClient(Dsl.asyncHttpClient(), v);
            clients.add(client);
        });
    }

    /**
     * Initializes the messaging system driver based on the proof configuration.
     * Currently supports Kafka driver type.
     *
     * @throws IllegalArgumentException if the specified driver type is not supported
     */
    private void init() {
        String driverName = proof.getDriver();
        Driver driver = configs.drivers().get(driverName);
        if (null == proofDriver) {
            if ("kafka".equals(driver.driverType())) {
                this.proofDriver = new KafkaProofDriver();
            } else if ("pulsar".equals(driver.driverType())) {
                this.proofDriver = new PulsarProofDriver();
            } else {
                throw new IllegalArgumentException("Unsupported driver: " + driver.driverType());
            }
        }
        proofDriver.init(driver.driverConfigs());
    }

    /**
     * Starts the proof test execution.
     * This method:
     * <ul>
     *   <li>Initializes the messaging system driver</li>
     *   <li>Creates the required topic</li>
     *   <li>Starts consumers and producers across workers</li>
     *   <li>Begins periodic checkpoint verification</li>
     * </ul>
     */
    public void start() {
        init();
        proofDriver.createTopic(proof.getTopic(), proof.getPartitions());
        startConsumers();
        startProducers();
        scheduleCheckpoint();
        log.info("Started the proof {}", proof);
        proof.setStartTime(System.currentTimeMillis());
    }

    /**
     * Distributes consumers across available worker nodes.
     * Attempts to evenly distribute the configured number of consumers
     * across all available workers.
     */
    private void startConsumers() {
        int workerCount = clients.size();
        // Distribute consumers to workers
        int totalConsumers = proof.getConsumers();
        int baseConsumersPerWorker = totalConsumers / workerCount;
        int consumerRemainder = totalConsumers % workerCount;

        for (int i = 0; i < clients.size(); i++) {
            int consumerCount = baseConsumersPerWorker + (i == workerCount - 1 ? consumerRemainder : 0);
            NewConsumers record = new NewConsumers(
                    proof.getId(),
                    proof.getTopic(),
                    proof.getPartitions(),
                    consumerCount,
                    configs.drivers().get(proof.getDriver())
            );
            try {
                clients.get(i).startConsumers(record).join();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Distributes producers across available worker nodes.
     * Attempts to evenly distribute producers, keys, and message rates
     * across all available workers.
     */
    private void startProducers() {
        int workerCount = clients.size();
        // Distribute producers to workers
        int totalProducers = proof.getProducers();
        int totalKeys = proof.getKeys();
        int totalMsgRate = proof.getMsgRate();

        // Calculate base values per worker
        int baseProducersPerWorker = totalProducers / workerCount;
        int producerRemainder = totalProducers % workerCount;
        int baseKeysPerWorker = totalKeys / workerCount;
        int keysRemainder = totalKeys % workerCount;
        int baseMsgRatePerWorker = totalMsgRate / workerCount;
        int msgRateRemainder = totalMsgRate % workerCount;

        for (int i = 0; i < clients.size(); i++) {
            int producerCount = baseProducersPerWorker + (i == workerCount - 1 ? producerRemainder : 0);
            int keyCount = baseKeysPerWorker + (i == workerCount - 1 ? keysRemainder : 0);
            int msgRate = baseMsgRatePerWorker + (i == workerCount - 1 ? msgRateRemainder : 0);

            NewProducers record = new NewProducers(
                    proof.getId(),
                    proof.getTopic(),
                    producerCount,
                    keyCount,
                    msgRate,
                    configs.drivers().get(proof.getDriver())
            );
            try {
                clients.get(i).startProducers(record).join();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Schedules periodic checkpoint verification.
     * This method runs at fixed intervals to:
     * <ul>
     *   <li>Collect checkpoints from all workers</li>
     *   <li>Aggregate producer and consumer checkpoints</li>
     *   <li>Verify message delivery guarantees</li>
     *   <li>Track verification successes and failures</li>
     * </ul>
     */
    private void scheduleCheckpoint() {
        executor.scheduleAtFixedRate(() -> {
            try {
                Pair<Checkpoint, Checkpoint> checkpoints = aggregateCheckpoints();
                ProofTask.this.latestProducerCheckpoint = checkpoints.getLeft();
                ProofTask.this.latestConsumerCheckpoint = checkpoints.getRight();
                if (ProofTask.this.inCheck == null) {
                    ProofTask.this.inCheck = ProofTask.this.latestProducerCheckpoint;
                    ProofTask.this.checkPointInCheckTimeStamps = System.currentTimeMillis();
                }

                boolean fulfilled = true;
                for (Map.Entry<String, LongSeq> entry : ProofTask.this.inCheck.getKeys().entrySet()) {
                    LongSeq expectedSeq = entry.getValue();
                    LongSeq actualSeq = ProofTask.this.latestConsumerCheckpoint.getKeys().get(entry.getKey());
                    if (actualSeq == null || actualSeq.compareTo(expectedSeq) < 0) {
                        fulfilled = false;
                        break;
                    }
                }

                if (fulfilled) {
                    log.info("[{}] checkpoint verify succeed: {}", proof.getId(), getSummary());
                    failed = false;
                    ProofTask.this.lastVerifiedProducerCheckpoint = ProofTask.this.inCheck;
                    ProofTask.this.lastVerifiedConsumerCheckpoint = ProofTask.this.latestConsumerCheckpoint;
                    ProofTask.this.inCheck = ProofTask.this.latestProducerCheckpoint;
                    ProofTask.this.checkPointInCheckTimeStamps = System.currentTimeMillis();
                } else if (Duration.ofMillis(System.currentTimeMillis() - ProofTask.this.checkPointInCheckTimeStamps)
                        .compareTo(Duration.ofSeconds(proof.getTimeout())) > 0) {
                    log.error("[{}] checkpoint verify failed: {}", proof.getId(), getSummary());
                    failed = true;
                    ProofTask.this.lastFailedProducerCheckpoint = ProofTask.this.inCheck;
                    ProofTask.this.lastFailedConsumerCheckpoint = ProofTask.this.latestConsumerCheckpoint;
                    timeouts++;
                    ProofTask.this.checkPointInCheckTimeStamps = System.currentTimeMillis();
                }
            } catch (Exception e) {
                log.error("Unexpected proof task {} failed", proof.getId(), e);
            }

        }, proof.getCheckPointInterval(), proof.getCheckPointInterval(), TimeUnit.SECONDS);
    }

    Pair<Checkpoint, Checkpoint> aggregateCheckpoints() {
        Checkpoint aggregatedProducerCheckpoint = Checkpoint.empty();
        Checkpoint aggregatedConsumerCheckpoint = Checkpoint.empty();
        for (WorkerHttpClient client : clients) {
            try {
                Checkpoint producerCheckpoint = client.producerCheckpoint(proof.getId()).join();
                aggregatedProducerCheckpoint.merge(producerCheckpoint);
                Checkpoint consumerCheckpoint = client.consumerCheckpoint(proof.getId()).join();
                aggregatedConsumerCheckpoint.merge(consumerCheckpoint);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return Pair.of(aggregatedProducerCheckpoint, aggregatedConsumerCheckpoint);
    }

    /**
     * Generates a summary of the proof test execution.
     *
     * @return A ProofSummary containing verification statistics
     */
    public ProofSummary getSummary() {
        long verified = this.getLastVerifiedProducerCheckpoint().getKeys().values().stream()
                .mapToLong(LongSeq::seq).sum();
        Map<String, List<LongSeq>> failedKeys = new HashMap<>();
        if (failed) {
            inCheck.getKeys().forEach((k, v) -> {
                LongSeq consumerLongSeq = latestConsumerCheckpoint.getKeys().get(k);
                if (consumerLongSeq != null
                        && v.compareTo(consumerLongSeq) > 0) {
                    failedKeys.put(k, List.of(v, consumerLongSeq));
                }
            });
        }
        return new ProofSummary(
                verified,
                this.getLastVerifiedProducerCheckpoint().getErrors().values().stream()
                        .mapToInt(Integer::intValue).sum(),
                this.getLastVerifiedConsumerCheckpoint().getOutOfOrderSeqs().values()
                        .stream().mapToInt(List::size).sum(),
                this.getLastVerifiedConsumerCheckpoint().getMissedSeqs().values().stream()
                        .flatMap(ranges -> ranges.stream()
                                .map(range -> range.get(1) - range.getFirst() + 1))
                        .mapToInt(Long::intValue)
                        .sum(),
                this.getLastVerifiedConsumerCheckpoint().getDuplicates().values().stream().reduce(0, Integer::sum),
                this.getTimeouts(),
                failedKeys,
                this.getLastVerifiedConsumerCheckpoint().getMissedSeqs(),
                this.getLastVerifiedConsumerCheckpoint().getOutOfOrderSeqs());
    }

    /**
     * Removes all resources associated with this proof test.
     * Deletes the messaging system topic and performs cleanup.
     */
    public void remove() {
        proofDriver.deleteTopic(proof.getTopic());
        System.out.println("ProofTask removed");
    }

    /**
     * Stops the proof test execution.
     * Stops all producers and shuts down the executor service.
     */
    public void stop() {
        clients.forEach(client -> {
            try {
                client.stopProducers(proof.getId()).join();
                client.stopConsumers(proof.getId()).join();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        executor.shutdown();
        System.out.println("ProofTask stopped");
    }
}
