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
package io.lakestream.streaming.proof.common;

/**
 * A consumer interface for the streaming proof verification framework that receives
 * messages and verifies delivery guarantees.
 *
 * <p>The ProofConsumer is responsible for:
 * <ul>
 *   <li>Consuming messages from the underlying messaging system</li>
 *   <li>Delivering messages to a {@link MessageListener} for verification</li>
 *   <li>Managing partition assignment and rebalancing</li>
 *   <li>Handling acknowledgments and offset management</li>
 *   <li>Maintaining consumer state and resources</li>
 * </ul>
 *
 * <p>The verification process works through:
 * <ul>
 *   <li>Receiving sequentially numbered messages with unique keys</li>
 *   <li>Passing messages to the MessageListener for sequence tracking</li>
 *   <li>Building checkpoints that track sequence ranges for each key</li>
 *   <li>Detecting delivery issues like missed messages, duplicates, or reordering</li>
 *   <li>Reporting checkpoint data to the Coordinator for system-wide verification</li>
 * </ul>
 *
 * <p>Each implementation handles system-specific details like consumer groups,
 * offset management, and partition assignment strategies. The interface is
 * deliberately minimal to allow flexibility in implementation.
 *
 *
 * @see MessageListener
 * @see ProofDriver
 * @see io.lakestream.streaming.proof.common.records.ConsumerCheckPoint
 * @see io.lakestream.streaming.proof.coordinator.ProofTask
 */
public interface ProofConsumer extends AutoCloseable {

    String name();
}
