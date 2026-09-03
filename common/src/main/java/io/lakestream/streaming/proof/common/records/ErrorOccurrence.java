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

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Aggregated timing details for a producer error message.
 */
@Data
@NoArgsConstructor
public class ErrorOccurrence {

    private int count;
    private long firstSeenAtMillis;
    private long lastSeenAtMillis;

    public ErrorOccurrence(int count, long firstSeenAtMillis, long lastSeenAtMillis) {
        this.count = count;
        this.firstSeenAtMillis = firstSeenAtMillis;
        this.lastSeenAtMillis = lastSeenAtMillis;
    }

    public static ErrorOccurrence firstSeen(long timestampMillis) {
        return new ErrorOccurrence(1, timestampMillis, timestampMillis);
    }

    public void recordOccurrence(long timestampMillis) {
        if (count == 0) {
            count = 1;
            firstSeenAtMillis = timestampMillis;
            lastSeenAtMillis = timestampMillis;
            return;
        }
        count++;
        firstSeenAtMillis = Math.min(firstSeenAtMillis, timestampMillis);
        lastSeenAtMillis = Math.max(lastSeenAtMillis, timestampMillis);
    }

    public void merge(ErrorOccurrence other) {
        if (other == null || other.count <= 0) {
            return;
        }
        if (count <= 0) {
            count = other.count;
            firstSeenAtMillis = other.firstSeenAtMillis;
            lastSeenAtMillis = other.lastSeenAtMillis;
            return;
        }
        count += other.count;
        firstSeenAtMillis = Math.min(firstSeenAtMillis, other.firstSeenAtMillis);
        lastSeenAtMillis = Math.max(lastSeenAtMillis, other.lastSeenAtMillis);
    }
}
