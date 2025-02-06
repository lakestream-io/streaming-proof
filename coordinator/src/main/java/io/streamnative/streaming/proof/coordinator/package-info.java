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
/**
 * Coordinator implementation for the streaming proof system that manages and validates
 * messaging system guarantees across distributed workers.
 *
 * <p>The coordinator package provides:
 * <ul>
 *   <li>HTTP REST API endpoints for managing proof tests and configurations</li>
 *   <li>Distributed worker coordination and task management</li>
 *   <li>Message delivery verification and guarantee validation</li>
 *   <li>System-wide configuration management</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * // Start the coordinator service
 * CoordinatorStarter starter = new CoordinatorStarter();
 * starter.start(8080);
 *
 * // Create a new proof test via REST API
 * POST /proofs
 * {
 *   "name": "kafka-ordering-test",
 *   "driver": "kafka",
 *   "features": ["ordering", "at_least_once"],
 *   "topic": "test-topic"
 * }
 * }</pre>
 *
 * @see io.streamnative.streaming.proof.coordinator.Coordinator
 * @see io.streamnative.streaming.proof.coordinator.CoordinatorHandler
 * @see io.streamnative.streaming.proof.coordinator.ProofTask
 */
package io.streamnative.streaming.proof.coordinator;