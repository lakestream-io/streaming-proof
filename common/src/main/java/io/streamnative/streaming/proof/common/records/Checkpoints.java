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

/**
 * A record class that maintains different checkpoint states for message processing in a streaming system.
 * This immutable record provides a comprehensive snapshot of various checkpoint states, enabling tracking
 *
 * @param inCheck The producer checkpoint currently being processed, representing the active state
 *                of message production
 * @param latestProducer The most recent producer checkpoint, providing the latest known state of message production
 * @param latestConsumer The most recent consumer checkpoint, providing the latest known state of message consumption
 * @param verifiedProducer
 *              The most recent successfully verified producer checkpoint, used to track confirmed message delivery
 * @param verifiedConsumer
 *              The most recent successfully verified consumer checkpoint, used to track confirmed message processing
 * @param failedProducer The most recent failed producer checkpoint, useful for debugging and error analysis
 * @param failedConsumer The most recent failed consumer checkpoint, useful for debugging and error analysis
 *
 * @see ProducerCheckpoint
 * @see ConsumerCheckPoint
 */
public record Checkpoints(ProducerCheckpoint inCheck,
                          ProducerCheckpoint latestProducer,
                          ConsumerCheckPoint latestConsumer,
                          ProducerCheckpoint verifiedProducer,
                          ConsumerCheckPoint verifiedConsumer,
                          ProducerCheckpoint failedProducer,
                          ConsumerCheckPoint failedConsumer) {
}
