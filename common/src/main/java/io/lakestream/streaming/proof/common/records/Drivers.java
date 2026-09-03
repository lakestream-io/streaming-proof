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

import lombok.Builder;

/**
 * A record representing the drivers for admin, producer, and consumer in a streaming proof verification.
 * This record will be used for geo-replication verification which allows producers and consumers connect to different
 * clusters.
 *
 * @param admin The driver name for admin operations such as topic creation, deletion, etc.
 * @param producer The driver name for producers such as producer creation, message publish, etc.
 * @param consumer The driver name for consumers such as consumer creation, message consumption, etc.
 */
@Builder
public record Drivers(String admin, String producer, String consumer) {
}
