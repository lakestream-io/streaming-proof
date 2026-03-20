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
package io.streamnative.streaming.proof.coordinator;

import io.streamnative.streaming.proof.common.LongSeq;
import io.streamnative.streaming.proof.common.ProofDriver;
import io.streamnative.streaming.proof.common.WebhookNotificationService;
import io.streamnative.streaming.proof.common.WorkerHttpClient;
import io.streamnative.streaming.proof.common.records.Checkpoints;
import io.streamnative.streaming.proof.common.records.Configs;
import io.streamnative.streaming.proof.common.records.ConsumerCheckPoint;
import io.streamnative.streaming.proof.common.records.NewConsumers;
import io.streamnative.streaming.proof.common.records.NewProducers;
import io.streamnative.streaming.proof.common.records.ProducerCheckpoint;
import io.streamnative.streaming.proof.common.records.Proof;
import io.streamnative.streaming.proof.common.records.ProofDetails;
import io.streamnative.streaming.proof.common.records.ProofSummary;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.asynchttpclient.Dsl;

/**
 * Manages a single streaming proof verification task across distributed workers.
 *
 * <p>ProofTask is the operational core of the verification framework, responsible for:
 * <ul>
 *   <li>Initializing and managing the messaging system driver</li>
 *   <li>Creating and configuring topics with appropriate partitioning</li>
 *   <li>Distributing producers and consumers across worker nodes</li>
 *   <li>Collecting and aggregating checkpoints from all workers</li>
 *   <li>Verifying message delivery guarantees through checkpoint analysis</li>
 *   <li>Detecting violations like message loss, duplication, or reordering</li>
 *   <li>Providing detailed diagnostics for any detected issues</li>
 * </ul>
 *
 * <p>The verification process works by:
 * <ul>
 *   <li>Producers send sequentially numbered messages with unique keys</li>
 *   <li>Consumers track received sequences using range-based checkpoints</li>
 *   <li>The coordinator periodically collects and aggregates these checkpoints</li>
 *   <li>Aggregated checkpoints are analyzed to detect guarantee violations</li>
 *   <li>Results are summarized with detailed diagnostics for any issues</li>
 * </ul>
 *
 *
 * @see io.streamnative.streaming.proof.common.records.Proof
 * @see io.streamnative.streaming.proof.common.records.ConsumerCheckPoint
 * @see io.streamnative.streaming.proof.common.records.ProducerCheckpoint
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
    private final ProofDriver proofDriver;

    /** List of HTTP clients for communicating with worker nodes */
    private final List<WorkerHttpClient> clients;
    
    /** Service for sending webhook notifications */
    private final WebhookNotificationService webhookService;

    /** Whether this proof uses Pulsar Shared subscription mode */
    private final boolean sharedMode;

    /** High watermarks per key for shared-mode verification */
    private Map<String, Long> highWatermarks = new HashMap<>();

    /** Current checkpoint being verified */
    private ProducerCheckpoint inCheck = new ProducerCheckpoint();

    /** Last successfully verified producer checkpoint */
    private ProducerCheckpoint lastVerifiedProducerCheckpoint = new ProducerCheckpoint();

    /** Last successfully verified consumer checkpoint */
    private ConsumerCheckPoint lastVerifiedConsumerCheckpoint = new ConsumerCheckPoint();

    /** Latest producer checkpoint received */
    private ProducerCheckpoint latestProducerCheckpoint = new ProducerCheckpoint();

    /** Latest consumer checkpoint received */
    @Setter(AccessLevel.PACKAGE)
    private ConsumerCheckPoint latestConsumerCheckpoint = new ConsumerCheckPoint();

    /** Last failed producer checkpoint */
    private ProducerCheckpoint lastFailedProducerCheckpoint = new ProducerCheckpoint();

    /** Last failed consumer checkpoint */
    private ConsumerCheckPoint lastFailedConsumerCheckpoint = new ConsumerCheckPoint();

    /** Number of checkpoint verification timeouts */
    private int timeouts;

    /** Timestamp of when the current checkpoint verification started */
    private long checkPointInCheckTimeStamps;

    private boolean failed = false;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean completionHandled = new AtomicBoolean(false);

    public boolean isRunning() {
        return running.get();
    }

    /**
     * Creates a new proof task with the specified configuration.
     *
     * @param proof The proof test configuration
     * @param configs System-wide configuration for workers and drivers
     */
    public ProofTask(Proof proof, Configs configs, ProofDriver driver) {
        this.proof = proof;
        this.executor = Executors.newSingleThreadScheduledExecutor();
        this.configs = configs;
        this.clients = new ArrayList<>(configs.workers().size());
        this.proofDriver = driver;
        this.webhookService = new WebhookNotificationService();
        this.sharedMode = proof.getPulsar() != null
                && proof.getPulsar().isSharedSubscription();
        configs.workers().forEach((k, v) -> {
            WorkerHttpClient client = new WorkerHttpClient(Dsl.asyncHttpClient(), v);
            clients.add(client);
        });
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
        running.set(true);
        // Create output topic (consumers read from this)
        proofDriver.createTopic(proof.getTopic(), proof.getPartitions());
        
        // For exactly-once verification, also create input topic for transactional processing
        if (proof.getFeatures().contains("exactly_once")) {
            proofDriver.createTopic(proof.getTopic() + "_transactional", proof.getPartitions());
        }
        
        startConsumers();
        startProducers();
        String formattedTimestamp = DateTimeFormatter.ISO_LOCAL_DATE_TIME
                .format(LocalDateTime.now());
        proof.setStartTime(formattedTimestamp);
        scheduleCheckpoint();
        log.info("Started the proof {}", proof);
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
        String driverName = proof.getDrivers() == null ? proof.getDriver() : proof.getDrivers().consumer();
        Map<String, Object> pulsarConsumerConfig = proof.getPulsar() != null
                ? proof.getPulsar().getConsumerConfig() : null;
        for (int i = 0; i < clients.size(); i++) {
            int consumerCount = baseConsumersPerWorker + (i == workerCount - 1 ? consumerRemainder : 0);
            NewConsumers record = new NewConsumers(
                    proof.getId(),
                    proof.getTopic(),
                    proof.getPartitions(),
                    consumerCount,
                    TimeUnit.SECONDS.toMillis(proof.getConsumeDelay()),
                    driverName,
                    configs.drivers().get(driverName),
                    pulsarConsumerConfig
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
        String driverName = proof.getDrivers() == null ? proof.getDriver() : proof.getDrivers().producer();
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
                    driverName,
                    configs.drivers().get(driverName),
                    proof.getFeatures().contains("exactly_once")
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
                if (!running.get()) {
                    return;
                }
                // Check if the proof task has reached its duration limit
                if (proof.getDuration() > 0 && proof.getStartTime() != null) {
                    // Parse the formatted timestamp back to a LocalDateTime
                    LocalDateTime startDateTime = LocalDateTime.parse(
                            proof.getStartTime(), DateTimeFormatter.ISO_LOCAL_DATE_TIME);

                    // Calculate elapsed time
                    long elapsedSeconds = Duration.between(
                            startDateTime, LocalDateTime.now()).getSeconds();

                    if (elapsedSeconds >= proof.getDuration()) {
                        completeProofAfterDuration();
                        return;
                    }
                }

                Pair<ProducerCheckpoint, ConsumerCheckPoint> checkpoints = aggregateCheckpoints();
                ProofTask.this.latestProducerCheckpoint = checkpoints.getLeft();
                ProofTask.this.latestConsumerCheckpoint = checkpoints.getRight();
                if (ProofTask.this.inCheck == null) {
                    ProofTask.this.inCheck = ProofTask.this.latestProducerCheckpoint;
                    ProofTask.this.checkPointInCheckTimeStamps = System.currentTimeMillis();
                }

                if (sharedMode) {
                    verifyShared();
                    return;
                }

                boolean fulfilled = true;
                for (Map.Entry<String, LongSeq> entry : ProofTask.this.inCheck.getPublished().entrySet()) {
                    LongSeq expectedSeq = entry.getValue();
                    LongSeq actualSeq = ProofTask.this.latestConsumerCheckpoint.getLastSeq(entry.getKey());
                    if (actualSeq == null || actualSeq.compareTo(expectedSeq) < 0) {
                        log.info("[{}] checkpoint verify in progress | {} | {} <= {} ",
                            proof.getId(), entry.getKey(), expectedSeq.seq(),
                            actualSeq == null ? -1L : actualSeq.seq());
                        fulfilled = false;
                        break;
                    }
                }

                if (fulfilled) {
                    // Compute watermarks before mutating state
                    Map<String, Long> newWatermarks =
                            latestConsumerCheckpoint.computeHighWatermarks(highWatermarks);
                    log.info("[{}] checkpoint verify succeed: {}", proof.getId(), getSummary());
                    failed = false;
                    ProofTask.this.lastVerifiedProducerCheckpoint = ProofTask.this.inCheck;
                    ProofTask.this.lastVerifiedConsumerCheckpoint = ProofTask.this.latestConsumerCheckpoint;
                    highWatermarks = newWatermarks;
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

    private void verifyShared() {
        if (ProofTask.this.inCheck.getPublished().isEmpty()) {
            ProofTask.this.inCheck = ProofTask.this.latestProducerCheckpoint;
            ProofTask.this.checkPointInCheckTimeStamps = System.currentTimeMillis();
        }

        Map<String, Long> newWatermarks = latestConsumerCheckpoint.computeHighWatermarks(highWatermarks);
        highWatermarks = newWatermarks;

        boolean fulfilled = true;
        for (Map.Entry<String, LongSeq> entry : inCheck.getPublished().entrySet()) {
            long expectedSeq = entry.getValue().seq();
            Long watermark = newWatermarks.get(entry.getKey());
            if (watermark == null || watermark < expectedSeq) {
                log.info("[{}] shared checkpoint verify in progress | {} | expected {} <= watermark {} ",
                        proof.getId(), entry.getKey(), expectedSeq,
                        watermark == null ? -1L : watermark);
                fulfilled = false;
                break;
            }
        }

        if (fulfilled) {
            log.info("[{}] shared checkpoint verify succeed: {}", proof.getId(), getSummary());
            failed = false;
            lastVerifiedProducerCheckpoint = inCheck;
            lastVerifiedConsumerCheckpoint = latestConsumerCheckpoint;
            inCheck = latestProducerCheckpoint;
            checkPointInCheckTimeStamps = System.currentTimeMillis();
        } else if (Duration.ofMillis(System.currentTimeMillis() - checkPointInCheckTimeStamps)
                .compareTo(Duration.ofSeconds(proof.getTimeout())) > 0) {
            log.error("[{}] shared checkpoint verify failed: {}", proof.getId(), getSummary());
            failed = true;
            lastFailedProducerCheckpoint = inCheck;
            lastFailedConsumerCheckpoint = latestConsumerCheckpoint;
            timeouts++;
            checkPointInCheckTimeStamps = System.currentTimeMillis();
        }
    }

    private void completeProofAfterDuration() {
        if (!completionHandled.compareAndSet(false, true)) {
            return;
        }

        log.info("Stopping proof task {} after reaching the specified duration of {} seconds",
                proof.getId(), proof.getDuration());
        sendCompletionNotification(getSummary());
        stop();
    }

    Pair<ProducerCheckpoint, ConsumerCheckPoint> aggregateCheckpoints() {
        ProducerCheckpoint aggregatedProducerCheckpoint = new ProducerCheckpoint();
        ConsumerCheckPoint aggregatedConsumerCheckpoint = new ConsumerCheckPoint();
        for (WorkerHttpClient client : clients) {
            try {
                ProducerCheckpoint producerCheckpoint = client.producerCheckpoint(proof.getId()).join();
                aggregatedProducerCheckpoint.merge(producerCheckpoint);
                ConsumerCheckPoint consumerCheckpoint = client.consumerCheckpoint(proof.getId(),
                        highWatermarks).join();
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
        if (sharedMode) {
            long verified = highWatermarks.values().stream()
                    .filter(w -> w >= 0)
                    .mapToLong(w -> w + 1)
                    .sum();
            ConsumerCheckPoint cp = this.getLastVerifiedConsumerCheckpoint();
            cp.calculate();
            return new ProofSummary(
                    verified,
                    this.getLastVerifiedProducerCheckpoint().getErrors().values().stream()
                            .mapToInt(Integer::intValue).sum(),
                    0, // out-of-order: not applicable for Shared
                    cp.getMissedSeqs().values().stream()
                            .flatMap(ranges -> ranges.stream()
                                    .map(range -> range.getEnd().seq() - range.getStart().seq() - 1))
                            .mapToInt(Long::intValue)
                            .sum(),
                    cp.getDuplicatedCount().values().stream().reduce(0L, Long::sum),
                    0, // write duplicates: not tracked in shared mode
                    this.getTimeouts());
        }
        long verified = this.getLastVerifiedProducerCheckpoint().getPublished().values().stream()
                .mapToLong(LongSeq::seq).sum();
        ConsumerCheckPoint lastVerifiedConsumerCheckpoint = this.getLastVerifiedConsumerCheckpoint();
        lastVerifiedConsumerCheckpoint.calculate();
        return new ProofSummary(
                verified,
                this.getLastVerifiedProducerCheckpoint().getErrors().values().stream()
                        .mapToInt(Integer::intValue).sum(),
                lastVerifiedConsumerCheckpoint.getOutOfOrderSeqs().values().stream()
                        .flatMap(ranges -> ranges.stream()
                                .map(range -> range.getRight() - range.getLeft() + 1))
                        .mapToInt(Long::intValue)
                        .sum(),
                lastVerifiedConsumerCheckpoint.getMissedSeqs().values().stream()
                        .flatMap(ranges -> ranges.stream()
                                .map(range -> range.getEnd().seq() - range.getStart().seq() - 1))
                        .mapToInt(Long::intValue)
                        .sum(),
                lastVerifiedConsumerCheckpoint.getDuplicatedCount().values().stream().reduce(0L, Long::sum),
                lastVerifiedConsumerCheckpoint.getWriteDuplicatesSeqs().values().stream()
                        .flatMap(ranges -> ranges.stream()
                                .map(range -> range.getEnd().seq() - range.getStart().seq() - 1))
                        .mapToInt(Long::intValue)
                        .sum(),
                this.getTimeouts());
    }

    /**
     * Generates detailed proof test execution results including summary and detailed error information.
     *
     * @return A ProofDetails containing comprehensive verification data
     */
    public ProofDetails getDetails() {
        ProofSummary summary = getSummary();
        Map<String, List<LongSeq>> failedKeys = new HashMap<>();
        if (failed) {
            if (sharedMode) {
                inCheck.getPublished().forEach((k, v) -> {
                    Long watermark = highWatermarks.get(k);
                    if (watermark == null || watermark < v.seq()) {
                        failedKeys.put(k, List.of(v,
                                new LongSeq(watermark == null ? -1 : watermark, null)));
                    }
                });
            } else {
                latestConsumerCheckpoint.calculate();
                inCheck.getPublished().forEach((k, v) -> {
                    LongSeq consumerLongSeq = latestConsumerCheckpoint.getLastSeq(k);
                    if (consumerLongSeq != null
                            && v.compareTo(consumerLongSeq) > 0) {
                        failedKeys.put(k, List.of(v, consumerLongSeq));
                    }
                });
            }
        }
        ConsumerCheckPoint lastVerifiedConsumerCheckpoint = this.getLastVerifiedConsumerCheckpoint();
        lastVerifiedConsumerCheckpoint.calculate();
        
        Checkpoints checkpoints = new Checkpoints(
                this.inCheck,
                this.latestProducerCheckpoint,
                this.latestConsumerCheckpoint,
                this.getLastVerifiedProducerCheckpoint(),
                lastVerifiedConsumerCheckpoint,
                this.lastFailedProducerCheckpoint,
                this.lastFailedConsumerCheckpoint
        );
        
        return new ProofDetails(
                proof,
                summary,
                checkpoints,
                failedKeys,
                lastVerifiedConsumerCheckpoint.getMissedSeqs(),
                lastVerifiedConsumerCheckpoint.getOutOfOrderSeqs(),
                lastVerifiedConsumerCheckpoint.getWriteDuplicatesSeqs());
    }

    /**
     * Removes all resources associated with this proof test.
     * Deletes the messaging system topic and performs cleanup.
     */
    public void remove() {
        if (proof.getFeatures() != null && proof.getFeatures().contains("exactly_once")) {
            try {
                proofDriver.deleteTopic(proof.getTopic() + "_transactional");
            } catch (Exception e) {
                log.warn("Failed to delete transactional topic {} for proof {}",
                        proof.getTopic() + "_transactional", proof.getId(), e);
            }
        }
        proofDriver.deleteTopic(proof.getTopic());
        System.out.println("ProofTask removed");
    }

    /**
     * Sends webhook notification when proof test completes with results.
     *
     * @param summary The proof execution summary
     */
    public void sendCompletionNotification(ProofSummary summary) {
        if (proof.getWebhookConfig() != null && proof.getWebhookConfig().isEnabled()) {
            try {
                webhookService.sendProofCompletionNotification(proof, summary)
                        .whenComplete((result, throwable) -> {
                            if (throwable != null) {
                                log.error("Failed to send completion webhook notification for proof {}",
                                        proof.getId(), throwable);
                            } else {
                                log.info("Successfully sent completion webhook notification for proof {}",
                                        proof.getId());
                            }
                        });
            } catch (Exception e) {
                log.error("Error sending completion webhook notification for proof {}",
                        proof.getId(), e);
            }
        }
    }

    /**
     * Stops the proof test execution.
     * Stops all producers and shuts down the executor service.
     */
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        executor.shutdown();

        clients.forEach(client -> {
            try {
                client.stopProducers(proof.getId()).join();
            } catch (Exception e) {
                log.warn("Failed to stop producers for proof {}", proof.getId(), e);
            }
            try {
                client.stopConsumers(proof.getId()).join();
            } catch (Exception e) {
                log.warn("Failed to stop consumers for proof {}", proof.getId(), e);
            }
        });

        if (webhookService != null) {
            webhookService.close();
        }

        log.info("ProofTask {} stopped", proof.getId());
    }
}
