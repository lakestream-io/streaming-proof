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
package io.streamnative.streaming.proof.worker;

import io.streamnative.streaming.proof.common.LongSeq;
import io.streamnative.streaming.proof.common.MessageMetadata;
import io.streamnative.streaming.proof.common.ProofProducer;
import io.streamnative.streaming.proof.common.records.LatencyMetricSnapshot;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * A task that manages message production for streaming proof tests. This class handles
 * the generation and sending of messages with sequential values for a set of unique keys.
 * It implements {@link AutoCloseable} for proper resource cleanup.
 *
 * <p>Key features:
 * <ul>
 *   <li>Round-robin message distribution across keys</li>
 *   <li>Sequential value tracking per key</li>
 *   <li>Asynchronous message production</li>
 *   <li>Error tracking and failed sequence recording</li>
 * </ul>
 *
 * <p>The task maintains:
 * <ul>
 *   <li>A fixed set of unique message keys</li>
 *   <li>Sequence counters for each key</li>
 *   <li>Error counts for failed sends</li>
 *   <li>List of sequence numbers that failed to send</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * ProofProducer producer = driver.createProducer("test-topic", configs);
 * ProofProducerTask task = new ProofProducerTask(producer, 10); // 10 unique keys
 *
 * // Send messages asynchronously
 * task.sendAsync()
 *     .thenAccept(v -> System.out.println("Message sent successfully"))
 *     .exceptionally(e -> {
 *         System.err.println("Send failed: " + e);
 *         return null;
 *     });
 *
 * // Later, check the metrics
 * System.out.println("Errors: " + task.getErrors().get());
 * System.out.println("Failed sequences: " + task.getFailedSeqs());
 * }</pre>
 *
 * @see ProofProducer
 * @see ProofProducers
 */
@Slf4j
public class ProofProducerTask implements AutoCloseable {

    /** The underlying producer for sending messages */
    private final ProofProducer producer;

    /** Number of unique keys managed by this task */
    private final int keys;

    /** Array of unique keys for round-robin message distribution */
    private final String[] keyArray;

    /** Maps each key to its current sequence number */
    private final Map<String, AtomicLong> keySeq;

    /** 
     * Maps keys to their most recently published messages. Each key maps to a LongMessage 
     * containing the sequence number and metadata of the last successfully published message.
     * This map is updated whenever a message is successfully sent.
     */
    private final Map<String, LongSeq> lastPublished;

    /** Counter for failed message sends */
    private final Map<String, Integer> errors = new HashMap<>();

    /** Counter for round-robin key selection */
    private final AtomicLong index = new AtomicLong(0);

    /** Total send attempts */
    private final AtomicLong sendAttempts = new AtomicLong(0);

    /** Total acknowledged sends */
    private final AtomicLong acknowledged = new AtomicLong(0);

    /** Estimated logical bytes acknowledged by the producer */
    private final AtomicLong acknowledgedBytes = new AtomicLong(0);

    /** Publish latency samples (cumulative, never reset) */
    private final LatencyRecorder publishLatency = new LatencyRecorder();

    /** Publish latency samples for windowed metrics (reset on each snapshot read) */
    private final LatencyRecorder publishLatencyWindow = new LatencyRecorder();

    /**
     * Creates a new producer task with the specified number of unique keys.
     *
     * @param producer The underlying producer to use for sending messages
     * @param keys The number of unique keys to manage
     */
    public ProofProducerTask(ProofProducer producer, int keys) {
        if (keys <= 0) {
            throw new IllegalArgumentException("Producer tasks require at least one key");
        }
        this.producer = producer;
        this.keys = keys;
        this.keyArray = new String[keys];
        Map<String, AtomicLong> map = new HashMap<>();
        Map<String, LongSeq> last = new HashMap<>();
        for (int i = 0; i < keys; i++) {
            String key = RandomStringUtils.secure().nextAlphanumeric(5);
            map.put(key, new AtomicLong(0));
            last.put(key, LongSeq.empty());
            keyArray[i] = key;
        }
        this.keySeq = Collections.unmodifiableMap(map);
        this.lastPublished = last;
    }

    /**
     * Asynchronously sends a message with the next sequence number for a key.
     * Keys are selected in round-robin fashion, and sequence numbers are
     * incremented atomically for each key.
     *
     * @return A future that completes when the send operation is done
     */
    public CompletableFuture<MessageMetadata> sendAsync() {
        String key = keyArray[(int) (index.getAndIncrement() % keys)];
        long seq = keySeq.get(key).getAndIncrement();
        long startedAtNanos = System.nanoTime();
        sendAttempts.incrementAndGet();
        return producer.sendAsync(key, seq).whenComplete((metadata, e) -> {
            if (e != null) {
                synchronized (errors) {
                    errors.compute(e.getMessage(), (k, v) -> v == null ? 1 : v + 1);
                }
            } else {
                acknowledged.incrementAndGet();
                acknowledgedBytes.addAndGet(estimateMessageBytes(key));
                long latencyMillis = (System.nanoTime() - startedAtNanos) / 1_000_000L;
                publishLatency.record(latencyMillis);
                publishLatencyWindow.record(latencyMillis);
                synchronized (lastPublished) {
                    LongSeq newMsg = new LongSeq(seq, metadata);
                    LongSeq previousMsg = this.lastPublished.get(key);
                    if (previousMsg != null) {
                        if (newMsg.compareTo(previousMsg) <= 0) {
                            log.error("Seq out of order writes | key: {} | new message: {} | previous message: {}",
                                    key, newMsg, previousMsg);
                            errors.compute("Seq out of order writes", (k, v) -> v == null ? 1 : v + 1);
                        }
                        if (newMsg.seq() - previousMsg.seq() > 1) {
                            log.error("Seq writes gap | key: {} | new message: {} | previous message: {}",
                                    key, newMsg, previousMsg);
                            errors.compute("Seq writes gap", (k, v) -> v == null ? 1 : v + 1);
                        }
                        if (!metadata.isAfter(previousMsg.metadata())) {
                            log.error("Offset out of order writes | key: {} | new message: {} | previous message: {}",
                                    key, newMsg, previousMsg);
                            errors.compute("Offset out of order writes", (k, v) -> v == null ? 1 : v + 1);
                        }
                    }
                    lastPublished.put(key, newMsg);
                }
            }
        });
    }

    public long getSendAttempts() {
        return sendAttempts.get();
    }

    public long getAcknowledged() {
        return acknowledged.get();
    }

    public long getAcknowledgedBytes() {
        return acknowledgedBytes.get();
    }

    public LatencyMetricSnapshot getPublishLatencySnapshot() {
        return publishLatency.snapshot();
    }

    /**
     * Returns the windowed publish latency snapshot and resets the window recorder.
     * Each call only reflects samples recorded since the previous call.
     */
    public LatencyMetricSnapshot getPublishLatencyWindowSnapshot() {
        return publishLatencyWindow.snapshotAndReset();
    }

    public synchronized Map<String, LongSeq> getLastPublished() {
        return Collections.unmodifiableMap(lastPublished);
    }

    public synchronized Map<String, Integer> getErrors() {
        return  Collections.unmodifiableMap(errors);
    }

    private static int estimateMessageBytes(String key) {
        return (key == null ? 0 : key.getBytes(StandardCharsets.UTF_8).length) + Long.BYTES;
    }

    /**
     * Closes the underlying producer and releases associated resources.
     *
     * @throws Exception if an error occurs while closing the producer
     */
    @Override
    public void close() throws Exception {
        producer.close();
    }
}
