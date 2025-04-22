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

import io.streamnative.streaming.proof.common.Util;
import io.streamnative.streaming.proof.common.records.Checkpoints;
import io.streamnative.streaming.proof.common.records.Configs;
import io.streamnative.streaming.proof.common.records.Proof;
import io.streamnative.streaming.proof.common.records.ProofDetails;
import io.streamnative.streaming.proof.worker.DriverCache;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * The Coordinator class manages the lifecycle and execution of streaming proof tests.
 * It acts as the central component that coordinates workers, manages configurations,
 * and orchestrates proof tasks across the distributed system.
 *
 * <p>The coordinator is responsible for:
 * <ul>
 *   <li>Managing system-wide configurations for workers and drivers</li>
 *   <li>Creating and managing proof tasks</li>
 *   <li>Coordinating test execution across distributed workers</li>
 *   <li>Monitoring and reporting test progress and results</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * Coordinator coordinator = new Coordinator();
 * 
 * // Configure the system
 * Configs configs = new Configs(
 *     Map.of("worker1", "http://worker1:8080"),
 *     Map.of("kafka", new Driver("kafka", kafkaConfigs))
 * );
 * coordinator.updateConfigs(configs);
 * 
 * // Create and run a proof test
 * Proof proof = Proof.builder()
 *     .name("ordering-test")
 *     .driver("kafka")
 *     .features(List.of("ordering"))
 *     .build();
 * coordinator.createProof(proof);
 * }</pre>
 *
 * @see ProofTask
 * @see Configs
 * @see Proof
 */
@Slf4j
@Getter
public class Coordinator {

    /** System-wide configuration for workers and drivers */
    private Configs configs;
    
    /** Active proof tasks mapped by their unique identifiers */
    private final Map<String, ProofTask> proofs = new ConcurrentHashMap<>();

    private final DriverCache driverCache = new DriverCache();

    /**
     * Updates the system configuration with new worker and driver settings.
     * If no configuration exists, initializes a new one. Otherwise, merges
     * the new configuration with the existing one.
     *
     * @param configs The new configuration to apply
     */
    public void updateConfigs(Configs configs) {
        if (this.configs == null) {
            this.configs = configs;
        } else {
            this.configs.workers().putAll(configs.workers());
            this.configs.drivers().putAll(configs.drivers());
        }
    }

    /**
     * Removes specified worker and driver configurations from the system.
     *
     * @param configs Configuration containing workers and drivers to remove
     */
    public void deleteConfigs(Configs configs) {
        configs.workers().forEach((k, v) -> this.configs.workers().remove(k));
        configs.drivers().forEach((k, v) -> this.configs.drivers().remove(k));
    }

    /**
     * Removes all worker and driver configurations from the system.
     */
    public void clearAllConfigs() {
        if (this.configs != null) {
            this.configs.workers().clear();
            this.configs.drivers().clear();
            log.info("All configurations have been cleared");
        }
    }

    /**
     * Creates and starts a new proof test with the specified configuration.
     * The method validates the driver and features before creating the test,
     * assigns a unique identifier, and initializes a new {@link ProofTask}.
     *
     * @param proof The proof test configuration
     * @throws IllegalArgumentException if the specified driver is not found or
     *         if any requested features are not supported
     */
    public void createProof(Proof proof) {
        String driverName = proof.getDrivers() == null ? proof.getDriver() : proof.getDrivers().admin();
        if (!configs.drivers().containsKey(driverName)) {
            throw new IllegalArgumentException("Driver " + driverName + " not found. "
                    + "Available drivers: " + configs.drivers().keySet());
        }
        if (!Util.SUPPORTED_PROOF_FEATS.containsAll(proof.getFeatures())) {
            throw new IllegalArgumentException("Unsupported proof features: " + proof.getFeatures()
                    + ". Supported features: " + Util.SUPPORTED_PROOF_FEATS);
        }
        if (proof.getConsumeDelay() > 0 && proof.getConsumeDelay() >= proof.getTimeout()) {
            throw new IllegalArgumentException("Consume delay must be less than timeout.");
        }
        String proofID = RandomStringUtils.secure().nextAlphanumeric(5);
        proof.setId(proofID);
        ProofTask task = new ProofTask(proof, configs, driverCache.getDriver(driverName,
                configs.drivers().get(driverName)));
        proofs.put(proof.getId(), task);
        task.start();
    }

    /**
     * Stops a running proof test without removing it from the system.
     * This method halts the test execution but preserves the test configuration
     * and results for later reference.
     *
     * @param proofID The unique identifier of the proof test to stop
     * @throws IllegalStateException if the proof test with the given ID is not found
     */
    public void stopProof(String proofID) {
        ProofTask task = proofs.get(proofID);
        task.stop();
    }

    /**
     * Completely removes a proof test from the system.
     * This method stops the test if it's running, cleans up all associated resources
     * including the messaging topic, and removes the test from the coordinator's registry.
     *
     * @param proofID The unique identifier of the proof test to remove
     * @throws IllegalStateException if the proof test with the given ID is not found
     */
    public void removeProof(String proofID) {
        ProofTask task = proofs.get(proofID);
        task.stop();
        task.remove();
        proofs.remove(proofID);
    }

    /**
     * Retrieves a list of all proof tests in the system.
     * The returned list is an immutable copy of the current proof tests,
     * ensuring thread safety and preventing external modifications.
     *
     * @return An immutable list of all {@link Proof} configurations currently
     *         registered in the system
     */
    public List<Proof> listProofs() {
        return List.copyOf(proofs.values().stream().map(ProofTask::getProof).toList());
    }

    /**
     * Retrieves detailed information about a specific proof test.
     * This method provides comprehensive information including:
     * <ul>
     *   <li>The test configuration</li>
     *   <li>Current execution status</li>
     *   <li>Checkpoints for both producers and consumers</li>
     *   <li>Latest verification results</li>
     *   <li>Any failed verification attempts</li>
     * </ul>
     *
     * @param proofID The unique identifier of the proof test to retrieve
     * @return A {@link ProofDetails} object containing the test configuration,
     *         summary, and checkpoint information
     * @throws IllegalStateException if the proof test with the given ID is not found
     */
    public ProofDetails getProofDetails(String proofID) {
        ProofTask task = proofs.get(proofID);
        Checkpoints checkpoints = new Checkpoints(
                task.getInCheck(),
                task.getLatestProducerCheckpoint(),
                task.getLatestConsumerCheckpoint(),
                task.getLastVerifiedProducerCheckpoint(),
                task.getLastVerifiedConsumerCheckpoint(),
                task.getLastFailedProducerCheckpoint(),
                task.getLastFailedConsumerCheckpoint());
        return new ProofDetails(task.getProof(), task.getSummary(), checkpoints);
    }

    public ProofDetails getProofSummary(String proofID) {
        ProofTask task = proofs.get(proofID);
        return new ProofDetails(task.getProof(), task.getSummary(), null);
    }

    /**
     * Gracefully shuts down the coordinator by stopping all running proof tests.
     * This method ensures that all tests are properly stopped and resources are
     * cleaned up before the coordinator is terminated.
     */
    public void close() {
        proofs.values().forEach(ProofTask::stop);
        driverCache.close();
    }
}
