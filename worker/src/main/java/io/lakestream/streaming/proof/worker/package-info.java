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
 * Worker package for the streaming proof verification framework that executes producer and consumer
 * tasks in a distributed verification environment.
 *
 * <p>The worker nodes serve as the data plane for the verification framework:
 * <ul>
 *   <li><strong>Message Production:</strong> Generating sequentially numbered messages at configurable rates</li>
 *   <li><strong>Message Consumption:</strong> Receiving and tracking message sequences for verification</li>
 *   <li><strong>Checkpoint Generation:</strong> Creating sequence range checkpoints for verification analysis</li>
 *   <li><strong>Driver Management:</strong> Caching and configuring messaging system clients</li>
 *   <li><strong>REST API:</strong> Providing HTTP endpoints for task management and monitoring</li>
 * </ul>
 *
 * <p>Key components:
 * <ul>
 *   <li>{@link io.lakestream.streaming.proof.worker.Worker} - Container for producer and consumer groups</li>
 *   <li>{@link io.lakestream.streaming.proof.worker.ProofProducers} - Group of producer instances</li>
 *   <li>{@link io.lakestream.streaming.proof.worker.ProofConsumers} - Group of consumer instances</li>
 *   <li>{@link io.lakestream.streaming.proof.worker.DefaultMessageListener} - Sequence tracking implementation</li>
 *   <li>{@link io.lakestream.streaming.proof.worker.DriverCache} - Messaging system client cache</li>
 * </ul>
 *
 * <p>Operational workflow:
 * <ol>
 *   <li>Worker nodes register with the coordinator service</li>
 *   <li>Coordinator assigns producer and consumer tasks to workers</li>
 *   <li>Workers create and manage messaging system clients through driver instances</li>
 *   <li>Producers generate sequentially numbered messages at configured rates</li>
 *   <li>Consumers track received sequences using range-based checkpoints</li>
 *   <li>Workers periodically report checkpoints to the coordinator for analysis</li>
 * </ol>
 *
 * @see io.lakestream.streaming.proof.worker.Worker
 * @see io.lakestream.streaming.proof.worker.WorkerHandler
 * @see io.lakestream.streaming.proof.common.ProofDriver
 * @see io.lakestream.streaming.proof.coordinator.Coordinator
 */
package io.lakestream.streaming.proof.worker;