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
package io.streamnative.streaming.proof.driver.pulsar;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.shade.com.google.gson.Gson;

@Slf4j
public class OffloadCondition {

    private final Gson gson = new Gson();
    private final PulsarAdmin admin;
    private final String topic;
    private final int waitOffloadedForCatchupRead;
    private Map<String, PersistentTopicInternalStats> topicInternalStatsCache = new HashMap<>();
    private Set<Long> offloadedLedgersCache = new HashSet<>();

    public static Optional<OffloadCondition> getOffloadCondition(
            PulsarAdmin admin, String topicName, Map<String, Object> configs) {
        if (configs == null || configs.isEmpty()) {
            return Optional.empty();
        }

        var waitOffloadedForEachLedger = (Boolean) configs
            .getOrDefault("offload.condition.waitOffloadedForEachLedger", false);
        var waitOffloadedForCatchupRead = (Integer) configs
            .getOrDefault("offload.condition.offloadedLedgersBeforeCatchupRead", 0);

        if (!waitOffloadedForEachLedger && waitOffloadedForCatchupRead == 0) {
            return Optional.empty();
        }

        return  Optional.of(new OffloadCondition(admin, topicName, waitOffloadedForCatchupRead));
    }

    public OffloadCondition(PulsarAdmin admin, String topic, int waitOffloadedForCatchupRead) {
        this.admin = admin;
        this.topic = topic;
        this.waitOffloadedForCatchupRead = waitOffloadedForCatchupRead;
    }

    public void waitOffloadConditionMeetForMessage(MessageIdAdv messageIdAdv) {
        long ledgerId = messageIdAdv.getLedgerId();
        if (offloadedLedgersCache.contains(ledgerId)) {
            return;
        }
        while (true) {
            try {
                log.info("Checking offload condition for topic: {}, ledgerId: {}", topic, ledgerId);
                var offloadedLedgers = topicInternalStatsCache.values().stream()
                    .flatMap(stats -> stats.ledgers.stream())
                    .filter(l -> l.offloaded)
                    .map(l -> l.ledgerId)
                    .collect(Collectors.toSet());
                offloadedLedgersCache.addAll(offloadedLedgers);
                if (offloadedLedgers.contains(ledgerId)) {
                    unloadTopics();
                    return;
                }
                refreshTopicInternalStats();
                TimeUnit.SECONDS.sleep(30);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for offload condition for topic: {}", topic);
                return;
            } catch (PulsarAdminException e) {
                log.error("Failed to refresh topic internal stats for topic: {}", topic, e);
            }
        }
    }

    public void waitOffloadConditionMeetForCatchupRead() {
        while (true) {
            log.info("Waiting for the offload condition for topic: {} to start the catchup read", topic);
            boolean isAllTopicsMeetCondition = true;
            for (String topic : topicInternalStatsCache.keySet()) {
                var stats = topicInternalStatsCache.get(topic);
                if (stats != null) {
                    var offloadedLedgers = stats.ledgers.stream()
                        .filter(l -> l.offloaded)
                        .map(l -> l.ledgerId)
                        .collect(Collectors.toSet());
                    if (offloadedLedgers.size() < waitOffloadedForCatchupRead) {
                        isAllTopicsMeetCondition = false;
                    }
                }
            }
            try {
                if (isAllTopicsMeetCondition) {
                    unloadTopics();
                    return;
                }
                refreshTopicInternalStats();
                TimeUnit.SECONDS.sleep(30); // Wait before checking again
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for offload condition for topic: {}", topic);
                return;
            } catch (PulsarAdminException e) {
                log.error("Failed to refresh topic internal stats for topic: {}", topic, e);
            }
        }
    }

    private void unloadTopics() throws PulsarAdminException {
        if (topicInternalStatsCache.isEmpty()) {
            refreshTopicInternalStats();
        }
        for (String topic : topicInternalStatsCache.keySet()) {
            admin.topics().unload(topic);
        }
    }

    private void refreshTopicInternalStats() throws PulsarAdminException {
        if (topicInternalStatsCache.isEmpty()) {
            var topicMetadata = admin.topics().getPartitionedTopicMetadata(topic);
            if (topicMetadata.partitions > 0) {
                for (int i = 0; i < topicMetadata.partitions; i++) {
                    String partitionedTopic = TopicName.get(topic).getPartition(i).toString();
                    topicInternalStatsCache.put(partitionedTopic, null);
                }
            } else {
                topicInternalStatsCache.put(topic, null);
            }
        }

        for (String topic : topicInternalStatsCache.keySet()) {
            PersistentTopicInternalStats stats = admin.topics().getInternalStats(topic);
            topicInternalStatsCache.put(topic, stats);
        }
    }
}
