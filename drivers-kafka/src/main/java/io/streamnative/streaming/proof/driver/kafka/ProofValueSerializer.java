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
package io.streamnative.streaming.proof.driver.kafka;

import io.streamnative.streaming.proof.common.ProofValue;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Kafka serializer for {@link ProofValue}, writing the sequence number followed by
 * padding up to the configured message size.
 *
 * <p>At the default size this produces the same bytes as Kafka's own
 * {@code LongSerializer}.
 *
 * @see ProofValueDeserializer
 */
public class ProofValueSerializer implements Serializer<ProofValue> {

    @Override
    public byte[] serialize(String topic, ProofValue data) {
        return data == null ? null : data.encode();
    }
}
