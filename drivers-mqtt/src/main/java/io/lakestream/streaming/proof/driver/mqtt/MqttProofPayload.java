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
package io.lakestream.streaming.proof.driver.mqtt;

import io.lakestream.streaming.proof.common.ProofValue;
import java.nio.charset.StandardCharsets;

/**
 * Wire format for MQTT proof messages, which have no separate key field and so must
 * carry the key inside the payload.
 *
 * <p>The layout is {@code <key>:<encoded ProofValue>}: the key as UTF-8, a single
 * colon, then the raw encoded value. The value part is binary and may itself contain
 * colon bytes, so decoding splits on the <em>first</em> colon only rather than
 * tokenizing the whole payload.
 */
final class MqttProofPayload {

    private static final byte SEPARATOR = (byte) ':';

    private MqttProofPayload() {
    }

    /**
     * Encodes a key and value into a single MQTT payload.
     *
     * @param key The message key, which must not contain a colon
     * @param value The value to encode after the separator
     * @return The encoded payload
     */
    static byte[] encode(String key, ProofValue value) {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = value.encode();
        byte[] payload = new byte[keyBytes.length + 1 + valueBytes.length];

        System.arraycopy(keyBytes, 0, payload, 0, keyBytes.length);
        payload[keyBytes.length] = SEPARATOR;
        System.arraycopy(valueBytes, 0, payload, keyBytes.length + 1, valueBytes.length);
        return payload;
    }

    /**
     * Extracts the key from an encoded payload.
     *
     * @param payload The encoded payload
     * @return The key
     * @throws IllegalArgumentException if the payload has no separator
     */
    static String decodeKey(byte[] payload) {
        return new String(payload, 0, separatorIndex(payload), StandardCharsets.UTF_8);
    }

    /**
     * Extracts the value from an encoded payload.
     *
     * @param payload The encoded payload
     * @return The decoded value
     * @throws IllegalArgumentException if the payload has no separator or the value
     *                                  is too short
     */
    static ProofValue decodeValue(byte[] payload) {
        int start = separatorIndex(payload) + 1;
        byte[] valueBytes = new byte[payload.length - start];
        System.arraycopy(payload, start, valueBytes, 0, valueBytes.length);
        return ProofValue.decode(valueBytes);
    }

    private static int separatorIndex(byte[] payload) {
        for (int i = 0; i < payload.length; i++) {
            if (payload[i] == SEPARATOR) {
                return i;
            }
        }
        throw new IllegalArgumentException("MQTT payload has no key separator");
    }
}
