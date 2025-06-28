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
import io.streamnative.streaming.proof.common.LongSeq;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;

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
 *   <li>Detailed error and sequence information for debugging and analysis</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * ProofDetails details = new ProofDetails(
 *     proof,           // test configuration
 *     summary,         // execution results
 *     checkpoints,     // verification data
 *     failedKeys,      // detailed error information
 *     missedSeqs,      // missed sequence ranges
 *     outOfOrderSeqs,  // out-of-order sequence pairs
 *     writeDuplicatesSeqs // duplicate sequence ranges
 * );
 * }</pre>
 *
 * @param proof The configuration and parameters of the streaming proof test,
 *             including test duration, message rates, and feature requirements
 * @param summary A high-level summary of the test execution results, including
 *               counts of verified messages, errors, and timing statistics
 * @param checkpoints Detailed checkpoint information tracking message processing
 *                   progress and verification states across producers and consumers
 * @param failedKeys Detailed mapping of failed keys and their associated sequence numbers,
 *                   providing granular error information for debugging
 * @param missedSeqs Mapping of consumer identifiers to ranges of missed sequence numbers,
 *                   indicating potential message loss patterns
 * @param outOfOrderSeqs Mapping of consumer identifiers to pairs of out-of-order sequence numbers,
 *                       showing specific ordering violations
 * @param writeDuplicatesSeqs Mapping of producer identifiers to ranges of duplicate sequence numbers,
 *                            indicating write-side duplication patterns
 *
 * @see Proof
 * @see ProofSummary
 * @see Checkpoints
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ProofDetails(Proof proof,
                           ProofSummary summary,
                           Checkpoints checkpoints,
                           Map<String, List<LongSeq>> failedKeys,
                           Map<String, List<ConsumerCheckPoint.SeqRange>> missedSeqs,
                           Map<String, List<Pair<Long, Long>>> outOfOrderSeqs,
                           Map<String, List<ConsumerCheckPoint.SeqRange>> writeDuplicatesSeqs) {
}
