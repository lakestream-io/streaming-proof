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

import io.streamnative.streaming.proof.common.records.LatencyMetricSnapshot;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Records latency samples using bounded reservoir sampling.
 */
final class LatencyRecorder {
    private static final int MAX_SAMPLES = 4096;

    private final List<Long> samples = new ArrayList<>(MAX_SAMPLES);
    private long count;
    private double sumMillis;
    private long maxMillis;

    synchronized void record(long latencyMillis) {
        long normalizedLatency = Math.max(0L, latencyMillis);
        count++;
        sumMillis += normalizedLatency;
        maxMillis = Math.max(maxMillis, normalizedLatency);

        if (samples.size() < MAX_SAMPLES) {
            samples.add(normalizedLatency);
            return;
        }

        long replacementIndex = ThreadLocalRandom.current().nextLong(count);
        if (replacementIndex < MAX_SAMPLES) {
            samples.set((int) replacementIndex, normalizedLatency);
        }
    }

    synchronized LatencyMetricSnapshot snapshot() {
        return new LatencyMetricSnapshot(count, sumMillis, maxMillis, List.copyOf(samples));
    }

    synchronized void mergeFrom(LatencyMetricSnapshot snapshot) {
        if (snapshot == null) {
            return;
        }
        count += snapshot.count();
        sumMillis += snapshot.sumMillis();
        maxMillis = Math.max(maxMillis, snapshot.maxMillis());
        if (snapshot.samplesMillis() == null) {
            return;
        }
        for (Long sample : snapshot.samplesMillis()) {
            if (sample == null) {
                continue;
            }
            if (samples.size() < MAX_SAMPLES) {
                samples.add(sample);
                continue;
            }
            long replacementIndex = ThreadLocalRandom.current().nextLong(Math.max(1L, count));
            if (replacementIndex < MAX_SAMPLES) {
                samples.set((int) replacementIndex, sample);
            }
        }
    }
}
