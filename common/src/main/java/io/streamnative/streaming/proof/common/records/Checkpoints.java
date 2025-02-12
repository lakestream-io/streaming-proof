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
package io.streamnative.streaming.proof.common.records;

import java.util.Map;

/**
 * A record class that maintains different checkpoint states for message processing in a streaming system.
 * This immutable record provides a comprehensive snapshot of various checkpoint states, enabling tracking
 * and verification of message processing progress and reliability.
 *
 * <p>The record maintains four distinct checkpoint states:
 * <ul>
 *   <li>Current processing state</li>
 *   <li>Latest recorded state</li>
 *   <li>Last verified state</li>
 *   <li>Last failed state</li>
 * </ul>
 *
 * <p>This structure allows for:
 * <ul>
 *   <li>Real-time monitoring of message processing</li>
 *   <li>Historical tracking of processing states</li>
 *   <li>Verification of message delivery guarantees</li>
 *   <li>Failure analysis and debugging</li>
 * </ul>
 *
 * @param inCheck The checkpoint currently being processed, representing the active state
 *               of message processing across all producers and consumers
 * @param latest A map containing the most recent checkpoints for each component (producer/consumer),
 *               providing the latest known state of message processing
 * @param lastVerified A map of the most recent successfully verified checkpoints for each component,
 *                     used to track confirmed message delivery and processing
 * @param lastFailed A map of the most recent failed checkpoints for each component,
 *                   useful for debugging and error analysis
 *
 * @see Checkpoint
 */
public record Checkpoints(Checkpoint inCheck,
                          Map<String, Checkpoint> latest,
                          Map<String, Checkpoint> lastVerified,
                          Map<String, Checkpoint> lastFailed) {
}
