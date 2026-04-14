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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.streamnative.streaming.proof.common.WebhookConfig;
import java.util.List;
import lombok.Builder;
import lombok.Data;

/**
 * Represents a streaming proof test configuration that validates messaging system guarantees.
 * This class uses the Builder pattern to create test configurations with customizable parameters
 * for validating different messaging system features and performance characteristics.
 *
 * <p>A proof test can validate various messaging guarantees such as:
 * <ul>
 *   <li>Exactly-once delivery</li>
 *   <li>At-least-once delivery</li>
 *   <li>Message ordering</li>
 *   <li>Performance under different loads</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * Proof proof = Proof.builder()
 *     .name("kafka-ordering-test")
 *     .driver("kafka")
 *     .features(List.of("ordering", "at_least_once"))
 *     .topic("test-topic")
 *     .partitions(8)
 *     .msgRate(5000)
 *     .build();
 * }</pre>
 *
 * @see ProofDetails
 * @see Driver
 */
@Data
@Builder
@lombok.AllArgsConstructor
@lombok.NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Proof {

    /** Unique identifier for the proof test */
    private String id;

    /** Human-readable name for the proof test */
    private String name;

    /**
     * The messaging system driver to use for this proof.
     * The driver configuration is stored separately in {@link Configs}.
     */
    private String driver;

    /**
     * The driver configuration for the proof.
     * This includes driver name for the admin, producer, and consumer.
     */
    private Drivers drivers;

    /**
     * List of messaging guarantees to validate.
     * Supported features include: exactly-once, at-least-once, ordering
     */
    private List<String> features;

    /** Detailed description of the proof test's purpose and expectations */
    private String description;

    /** The topic name to use for message production and consumption */
    private String topic;

    /**
     * Number of partitions for the topic.
     * Higher partition count enables greater parallelism but may affect ordering guarantees.
     */
    @Builder.Default
    private int partitions = 10;

    /**
     * Number of producer instances to create.
     * Multiple producers can increase throughput and test concurrent message production.
     */
    @Builder.Default
    private int producers = 4;

    /**
     * Number of consumer instances to create.
     * Multiple consumers enable parallel message processing and consumption pattern testing.
     */
    @Builder.Default
    private int consumers = 4;

    /**
     * Target message production rate in messages per second.
     * Used to control the load on the messaging system.
     */
    @Builder.Default
    private int msgRate = 1000;

    /**
     * Number of unique message keys to use.
     * Affects message distribution across partitions and ordering guarantees.
     */
    @Builder.Default
    private int keys = 100;

    /**
     * Interval between checkpoints in seconds.
     * Checkpoints track message delivery progress and verify guarantees.
     */
    @Builder.Default
    private int checkPointInterval = 60;

    /**
     * Maximum time in seconds to wait for message verification.
     * Test fails if verification exceeds this timeout.
     */
    @Builder.Default
    private int timeout = 180;

    /**
     * Delay in seconds to simulate catchup reading.
     * The value must be 0 or greater than timeout.
     * Default is 0, means no delay.
     */
    @Builder.Default
    private int consumeDelay = 0;

    /**
     * Total duration of the proof test in seconds.
     * Default is 8 hours (28800 seconds).
     */
    @Builder.Default
    private int duration = 28800;

    /**
     * Additional seconds to wait during final verification after producers stop.
     * Null uses coordinator/global defaults. A value of 0 means no retry wait.
     */
    private Integer finalWaitSeconds;

    /** Human-readable timestamp when the proof test started */
    private String startTime;

    /**
     * Optional report sampling interval in seconds.
     * A value of 0 lets the coordinator derive a suitable interval automatically.
     */
    @Builder.Default
    private int timeSeriesInterval = 0;
    
    /** Webhook configuration for notifications */
    private WebhookConfig webhookConfig;

    /** Per-proof Pulsar-specific configuration */
    private PulsarProofConfig pulsar;
}
