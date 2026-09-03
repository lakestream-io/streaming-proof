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

import java.nio.ByteBuffer;
import java.util.Random;

/**
 * The value carried by every proof message: a per-key sequence number plus enough
 * padding to reach a configured total size.
 *
 * <p>The wire format is an 8-byte big-endian sequence number followed by
 * {@code size - 8} padding bytes. Only the sequence number is meaningful; the
 * padding exists solely to inflate the message so that storage-path behaviour
 * (offload triggers, tiered-storage reads, bandwidth) can be exercised at a
 * realistic scale.
 *
 * <p>Padding content is never verified. It is derived deterministically from the
 * sequence number rather than being a constant so that it does not compress away:
 * a run of zeroes would shrink to nothing on any compression-enabled path, and the
 * inflation the test depends on would silently vanish.
 *
 * <p>The padding bytes are not held by this record. They are produced by
 * {@link #encode} and discarded by {@link #decode}, so a {@code ProofValue} stays
 * small regardless of the configured size.
 *
 * @param seq The per-key sequence number used to verify ordering and detect gaps
 *            and duplicates
 * @param size The total encoded length in bytes, including the 8-byte sequence
 *             number. Never smaller than {@link #MIN_SIZE}.
 */
public record ProofValue(long seq, int size) {

    /** Length of the sequence number prefix, and therefore the smallest legal size. */
    public static final int MIN_SIZE = Long.BYTES;

    public ProofValue {
        if (size < MIN_SIZE) {
            throw new IllegalArgumentException(
                    "Message size must be at least " + MIN_SIZE + " bytes, got " + size);
        }
    }

    /**
     * Creates a value with no padding, encoding to exactly the sequence number.
     *
     * @param seq The per-key sequence number
     * @return A value whose encoded form is 8 bytes
     */
    public static ProofValue of(long seq) {
        return new ProofValue(seq, MIN_SIZE);
    }

    /**
     * Encodes this value to its wire form.
     *
     * <p>A fresh array is allocated per call. The buffer cannot be pooled: Kafka and
     * Pulsar send asynchronously and keep the reference past the send call, so a
     * shared buffer would be overwritten underneath in-flight messages.
     *
     * @return A byte array of exactly {@link #size} bytes
     */
    public byte[] encode() {
        byte[] payload = new byte[size];
        ByteBuffer.wrap(payload).putLong(seq);
        if (size > MIN_SIZE) {
            byte[] padding = new byte[size - MIN_SIZE];
            new Random(seq).nextBytes(padding);
            System.arraycopy(padding, 0, payload, MIN_SIZE, padding.length);
        }
        return payload;
    }

    /**
     * Decodes a value from its wire form, reading the sequence number and ignoring
     * any padding that follows.
     *
     * @param payload The encoded bytes, at least {@link #MIN_SIZE} long
     * @return The decoded value, whose size reflects the payload actually received
     * @throws IllegalArgumentException if the payload is null or too short to hold
     *                                  a sequence number
     */
    public static ProofValue decode(byte[] payload) {
        if (payload == null || payload.length < MIN_SIZE) {
            throw new IllegalArgumentException(
                    "Payload must be at least " + MIN_SIZE + " bytes, got "
                            + (payload == null ? "null" : payload.length));
        }
        return new ProofValue(ByteBuffer.wrap(payload).getLong(), payload.length);
    }
}
