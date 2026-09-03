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
/**
 * Central coordination package for the streaming proof verification framework that orchestrates
 * distributed verification tasks across worker nodes.
 *
 * <p>The coordinator serves as the control plane for the verification framework:
 * <ul>
 *   <li><strong>Task Management:</strong> Creating, configuring, and monitoring verification tasks</li>
 *   <li><strong>Worker Orchestration:</strong> Distributing producers and consumers across workers</li>
 *   <li><strong>Checkpoint Collection:</strong> Aggregating sequence checkpoints from all consumers</li>
 *   <li><strong>Guarantee Verification:</strong> Analyzing checkpoints to detect messaging violations</li>
 *   <li><strong>REST API:</strong> Providing HTTP endpoints for task management and monitoring</li>
 * </ul>
 *
 * <p>Key components:
 * <ul>
 *   <li>{@link io.lakestream.streaming.proof.coordinator.Coordinator} - Central orchestration service</li>
 *   <li>{@link io.lakestream.streaming.proof.coordinator.ProofTask} - Individual verification task</li>
 *   <li>{@link io.lakestream.streaming.proof.coordinator.CoordinatorHandler} - REST API handler</li>
 *   <li>{@link io.lakestream.streaming.proof.coordinator.WorkerHttpClient} - Worker communication</li>
 * </ul>
 *
 * <p>Verification workflow:
 * <ol>
 *   <li>Create a verification task with specific messaging guarantees to test</li>
 *   <li>Coordinator provisions topic and distributes producers/consumers across workers</li>
 *   <li>Producers generate sequentially numbered messages with unique keys</li>
 *   <li>Consumers track received sequences and report checkpoints to coordinator</li>
 *   <li>Coordinator analyzes checkpoints to verify messaging guarantees</li>
 *   <li>Results are summarized with detailed diagnostics for any detected issues</li>
 * </ol>
 *
 *
 * @see io.lakestream.streaming.proof.coordinator.Coordinator
 * @see io.lakestream.streaming.proof.coordinator.ProofTask
 * @see io.lakestream.streaming.proof.common.ProofDriver
 * @see io.lakestream.streaming.proof.worker.Worker
 */
package io.lakestream.streaming.proof.coordinator;