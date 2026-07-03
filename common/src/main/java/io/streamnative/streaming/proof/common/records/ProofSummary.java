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
 * A summary record that captures the results and statistics of a streaming proof test execution.
 * This immutable record provides key metrics about message delivery reliability, ordering
 * guarantees, and error conditions encountered during the test.
 *
 * <p>The summary includes:
 * <ul>
 *   <li>Message verification counts</li>
 *   <li>Error statistics</li>
 *   <li>Ordering violation metrics</li>
 *   <li>Message delivery reliability indicators</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * ProofSummary summary = new ProofSummary(
 *     1000000L,  // verified messages
 *     0,         // errors
 *     2,         // out-of-order messages
 *     0,         // missed messages
 *     1,         // duplicate messages
 *     0,         // write duplicates
 *     0,         // timeouts
 *     0L,        // verifiedStallSeconds
 *     0L         // maxVerifiedStallSeconds
 * );
 * }</pre>
 *
 * @param verified The total number of messages that were successfully verified,
 *                indicating the volume of messages that passed all validation checks
 * @param errors The count of errors encountered during message processing,
 *              including producer and consumer errors
 * @param outOfOrders The number of messages received out of their expected sequence,
 *                    relevant for tests validating message ordering guarantees
 * @param missed The count of messages that were expected but never received,
 *              indicating potential message loss
 * @param duplicates The number of messages that were received multiple times,
 *                   relevant for exactly-once delivery validation
 * @param timeouts The count of timeout events that occurred during message
 *                processing or verification
 * @param verifiedStallSeconds Wall-clock seconds since the verified count last increased;
 *                accumulates from test start and freezes once the run completes
 * @param maxVerifiedStallSeconds Peak wall-clock stall observed during the run, in seconds;
 *                retained across recoveries and frozen at completion
 *
 * @see ProofDetails
 * @see Checkpoints
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ProofSummary(long verified,
                           int errors,
                           int outOfOrders,
                           int missed,
                           long duplicates,
                           long writeDuplicates,
                           int timeouts,
                           long verifiedStallSeconds,
                           long maxVerifiedStallSeconds) {
}
