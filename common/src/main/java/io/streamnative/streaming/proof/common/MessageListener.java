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
package io.streamnative.streaming.proof.common;

/**
 * A listener interface for handling messages in the streaming proof system.
 * Implementations of this interface receive messages from a {@link ProofConsumer}
 * and can perform message validation, ordering checks, and other verification tasks.
 *
 * <p>The listener is notified for each message consumed from the messaging system,
 * receiving both the message key and a sequential value. The sequential value is
 * typically used to verify message ordering and detect duplicates or missing messages.
 *
 * @see ProofConsumer
 * @see ProofConsumerTask
 */
public interface MessageListener {

    /**
     * Called when a message is received from the messaging system.
     *
     * @param key The message key, used for message grouping and partition assignment
     * @param value A sequential value that can be used to verify message ordering
     *              and detect duplicates or missing messages within a key's sequence
     */
    void onMessage(String key, long value);

}
