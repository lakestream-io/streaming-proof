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
 * Base package for the streaming proof system that validates messaging system guarantees
 * and performance characteristics through distributed testing.
 *
 * <p>This package provides core interfaces and abstractions for:
 * <ul>
 *   <li>Message Production - Interfaces and base classes for message generators</li>
 *   <li>Message Consumption - Consumer abstractions for message verification</li>
 *   <li>Driver Integration - Extensible framework for different messaging systems</li>
 *   <li>Verification Logic - Tools for validating messaging guarantees</li>
 * </ul>
 *
 * <p>Key components:
 * <ul>
 *   <li>{@link io.streamnative.streaming.proof.common.ProofProducer} - Message production interface</li>
 *   <li>{@link io.streamnative.streaming.proof.common.ProofConsumer} - Message consumption interface</li>
 *   <li>{@link io.streamnative.streaming.proof.common.ProofDriver} - Messaging system driver interface</li>
 *   <li>{@link io.streamnative.streaming.proof.common.MessageListener} - Message verification callback</li>
 * </ul>
 *
 * <p>Supported messaging guarantees:
 * <ul>
 *   <li>Exactly-once delivery</li>
 *   <li>At-least-once delivery</li>
 *   <li>Message ordering</li>
 *   <li>Performance under load</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * // Implement a custom proof producer
 * public class CustomProofProducer implements ProofProducer {
 *     @Override
 *     public CompletableFuture<Void> sendAsync(String key, long value) {
 *         // Implementation for message production
 *     }
 * }
 *
 * // Implement a custom proof consumer
 * public class CustomProofConsumer implements ProofConsumer {
 *     private final MessageListener listener;
 *
 *     public CustomProofConsumer(MessageListener listener) {
 *         this.listener = listener;
 *     }
 *
 *     // Implementation for message consumption
 * }
 * }</pre>
 *
 * @see io.streamnative.streaming.proof.common.ProofProducer
 * @see io.streamnative.streaming.proof.common.ProofConsumer
 * @see io.streamnative.streaming.proof.common.ProofDriver
 */
package io.streamnative.streaming.proof.common;