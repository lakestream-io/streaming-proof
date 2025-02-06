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
 * Package containing record classes that define the core data structures and configurations
 * for the streaming proof system.
 *
 * <p>The records in this package serve several purposes:
 * <ul>
 *   <li>Test Configuration - Defining proof test parameters and requirements</li>
 *   <li>Runtime State - Tracking execution progress and verification status</li>
 *   <li>System Configuration - Managing worker and driver settings</li>
 *   <li>Results Collection - Aggregating test results and statistics</li>
 * </ul>
 *
 * <p>Key records include:
 * <ul>
 *   <li>{@link io.streamnative.streaming.proof.common.records.Proof} - Core test configuration</li>
 *   <li>{@link io.streamnative.streaming.proof.common.records.ProofDetails} - Comprehensive test state</li>
 *   <li>{@link io.streamnative.streaming.proof.common.records.Checkpoints} - Message verification tracking</li>
 *   <li>{@link io.streamnative.streaming.proof.common.records.ProofSummary} - Test execution results</li>
 *   <li>{@link io.streamnative.streaming.proof.common.records.Configs} - System-wide settings</li>
 *   <li>{@link io.streamnative.streaming.proof.common.records.NewProducers} - Producer initialization</li>
 *   <li>{@link io.streamnative.streaming.proof.common.records.NewConsumers} - Consumer initialization</li>
 * </ul>
 *
 * <p>These records are designed to be immutable and thread-safe, making them suitable
 * for use in concurrent operations across the distributed streaming proof system.
 *
 * @see io.streamnative.streaming.proof.common.records.Proof
 * @see io.streamnative.streaming.proof.common.records.ProofDetails
 * @see io.streamnative.streaming.proof.common.records.Checkpoints
 */
package io.streamnative.streaming.proof.common.records;