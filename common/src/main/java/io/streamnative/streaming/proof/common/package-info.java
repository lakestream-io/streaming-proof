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
/**
 * Core package for the streaming proof verification framework that validates messaging system
 * guarantees through distributed sequence tracking.
 *
 * <p>This package provides the foundation for messaging system verification with:
 * <ul>
 *   <li><strong>System Abstraction:</strong> Unified interfaces for different messaging platforms</li>
 *   <li><strong>Verification Primitives:</strong> Sequence-based tracking for guarantee validation</li>
 *   <li><strong>Driver Architecture:</strong> Extensible framework for messaging system integration</li>
 *   <li><strong>Metadata Handling:</strong> System-agnostic position tracking for verification</li>
 * </ul>
 *
 * <p>Key interfaces and components:
 * <ul>
 *   <li>{@link io.streamnative.streaming.proof.common.ProofDriver} - Messaging system abstraction layer</li>
 *   <li>{@link io.streamnative.streaming.proof.common.ProofProducer} - Sequence-generating message producer</li>
 *   <li>{@link io.streamnative.streaming.proof.common.ProofConsumer} - Message consumer with verification</li>
 *   <li>{@link io.streamnative.streaming.proof.common.MessageListener} - Sequence tracking and verification</li>
 *   <li>{@link io.streamnative.streaming.proof.common.MessageMetadata} - System-agnostic position tracking</li>
 * </ul>
 *
 * <p>Verification capabilities:
 * <ul>
 *   <li><strong>Exactly-once processing:</strong> Detecting duplicate message delivery</li>
 *   <li><strong>At-least-once delivery:</strong> Identifying message loss through sequence gaps</li>
 *   <li><strong>Ordering guarantees:</strong> Verifying sequence integrity within partitions</li>
 *   <li><strong>Cross-partition consistency:</strong> Validating global ordering properties</li>
 *   <li><strong>Performance characteristics:</strong> Measuring throughput and latency under load</li>
 * </ul>
 *
 * <p>Architecture overview:
 * <pre>
 *   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
 *   │ ProofDriver │────▶│ProofProducer│────▶│ Messaging   │
 *   └─────────────┘     └─────────────┘     │ System      │
 *          │                                 │ (Kafka,     │
 *          │                                 │  Pulsar)    │
 *          │            ┌─────────────┐     │             │
 *          └───────────▶│ProofConsumer│◀────│             │
 *                       └─────────────┘     └─────────────┘
 *                              │
 *                              ▼
 *                      ┌─────────────┐
 *                      │MessageListener
 *                      └─────────────┘
 * </pre>
 *
 * @see io.streamnative.streaming.proof.common.ProofDriver
 * @see io.streamnative.streaming.proof.common.ProofProducer
 * @see io.streamnative.streaming.proof.common.ProofConsumer
 * @see io.streamnative.streaming.proof.common.MessageListener
 * @see io.streamnative.streaming.proof.coordinator.Coordinator
 * @see io.streamnative.streaming.proof.worker.Worker
 */
package io.streamnative.streaming.proof.common;