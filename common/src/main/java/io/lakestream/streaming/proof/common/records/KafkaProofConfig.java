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
package io.lakestream.streaming.proof.common.records;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Per-proof Kafka-specific configuration.
 *
 * <p>Holds optional topic configuration that is passed to Kafka's
 * {@code NewTopic.configs()} when proof topics are created.
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class KafkaProofConfig {

    /**
     * Topic configuration map passed to {@code NewTopic.configs()}.
     * Example keys: "unclean.leader.election.enable", "retention.ms", "cleanup.policy".
     */
    private Map<String, String> topicConfig;
}
