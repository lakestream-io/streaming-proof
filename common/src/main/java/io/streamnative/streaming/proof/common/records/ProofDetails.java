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
package io.streamnative.streaming.proof.common.records;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * A comprehensive record that encapsulates the complete state and results of a streaming proof test.
 * This immutable record combines the test configuration, execution summary, and detailed checkpoints,
 * providing a complete view of a proof test's execution and results.
 *
 * <p>The record serves three main purposes:
 * <ul>
 *   <li>Configuration reference through the {@link Proof} component</li>
 *   <li>High-level results through the {@link ProofSummary} component</li>
 *   <li>Detailed verification data through the {@link Checkpoints} component</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * ProofDetails details = new ProofDetails(
 *     proof,           // test configuration
 *     summary,         // execution results
 *     checkpoints      // verification data
 * );
 * }</pre>
 *
 * @param proof The configuration and parameters of the streaming proof test,
 *             including test duration, message rates, and feature requirements
 * @param summary A high-level summary of the test execution results, including
 *               counts of verified messages, errors, and timing statistics
 * @param checkpoints Detailed checkpoint information tracking message processing
 *                   progress and verification states across producers and consumers
 *
 * @see Proof
 * @see ProofSummary
 * @see Checkpoints
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ProofDetails(Proof proof,
                           ProofSummary summary,
                           Checkpoints checkpoints) {
}
