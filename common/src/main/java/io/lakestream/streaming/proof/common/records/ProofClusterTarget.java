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

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.Map;

/**
 * Sanitized endpoint-oriented view of a resolved driver target.
 *
 * @param role Logical role for this target, such as {@code default}, {@code admin},
 *             {@code producer}, or {@code consumer}
 * @param driverName Configured driver alias selected for the role
 * @param driverType Driver implementation type, such as {@code pulsar} or {@code kafka}
 * @param endpoints Non-sensitive endpoint fields extracted from the driver config
 * @param metadata Optional descriptive metadata copied from the driver config
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ProofClusterTarget(
        String role,
        String driverName,
        String driverType,
        Map<String, Object> endpoints,
        Map<String, Object> metadata) {
}
