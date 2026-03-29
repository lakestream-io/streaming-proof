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
package io.streamnative.streaming.proof.coordinator;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import io.streamnative.streaming.proof.common.LongSeq;
import io.streamnative.streaming.proof.common.MessageMetadata;
import io.streamnative.streaming.proof.common.ProofDriver;
import io.streamnative.streaming.proof.common.records.Configs;
import io.streamnative.streaming.proof.common.records.ConsumerCheckPoint;
import io.streamnative.streaming.proof.common.records.Driver;
import io.streamnative.streaming.proof.common.records.ProducerCheckpoint;
import io.streamnative.streaming.proof.common.records.Proof;
import io.streamnative.streaming.proof.common.records.ProofClusterTarget;
import io.streamnative.streaming.proof.common.records.ProofSummary;
import io.streamnative.streaming.proof.common.records.PulsarProofConfig;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.tuple.Pair;
import org.mockito.InOrder;
import org.testng.annotations.Test;

public class ProofTaskTest {

    @Test
    public void testRemoveDeletesProofTopicForAtLeastOnceProof() {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder()
                .topic("test-topic")
                .features(List.of("at_least_once"))
                .build();
        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), driver);
        try {
            task.remove();

            verify(driver, never()).deleteTopic("test-topic_transactional");
            verify(driver, times(1)).deleteTopic("test-topic");
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testRemoveDeletesProofAndTransactionalTopicsForExactlyOnceProof() {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder()
                .topic("test-topic")
                .features(List.of("exactly_once"))
                .build();
        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), driver);
        try {
            task.remove();

            InOrder inOrder = inOrder(driver);
            inOrder.verify(driver, times(1)).deleteTopic("test-topic_transactional");
            inOrder.verify(driver, times(1)).deleteTopic("test-topic");
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testRemoveDeletesMainTopicWhenTransactionalCleanupFails() {
        ProofDriver driver = mock(ProofDriver.class);
        doThrow(new RuntimeException("missing topic"))
                .when(driver).deleteTopic("test-topic_transactional");
        Proof proof = Proof.builder()
                .topic("test-topic")
                .features(List.of("exactly_once"))
                .build();
        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), driver);
        try {
            task.remove();

            verify(driver, times(1)).deleteTopic("test-topic_transactional");
            verify(driver, times(1)).deleteTopic("test-topic");
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testSharedModeDetection() {
        ProofDriver driver = mock(ProofDriver.class);
        Proof sharedProof = Proof.builder()
                .topic("test-topic")
                .features(List.of("at_least_once"))
                .pulsar(PulsarProofConfig.builder()
                        .consumerConfig(Map.of("subscriptionType", "Shared"))
                        .build())
                .build();
        ProofTask task = new ProofTask(sharedProof, new Configs(Map.of(), Map.of()), driver);
        try {
            assertTrue(task.isSharedMode());
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testNonSharedModeByDefault() {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder()
                .topic("test-topic")
                .features(List.of("at_least_once"))
                .build();
        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), driver);
        try {
            assertFalse(task.isSharedMode());
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testGetSummaryVerifiedCountUsesSeqPlusOne() {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder()
                .topic("test-topic")
                .features(List.of("at_least_once", "ordering"))
                .build();
        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), driver);
        try {
            // Simulate 4 keys each with 125 messages (seq 0-124)
            ProducerCheckpoint producerCp = task.getLastVerifiedProducerCheckpoint();
            producerCp.addPublished("key0", new LongSeq(124, MessageMetadata.empty()));
            producerCp.addPublished("key1", new LongSeq(124, MessageMetadata.empty()));
            producerCp.addPublished("key2", new LongSeq(124, MessageMetadata.empty()));
            producerCp.addPublished("key3", new LongSeq(124, MessageMetadata.empty()));

            ProofSummary summary = task.getSummary();

            // With the fix: (124+1)*4 = 500, not 124*4 = 496
            assertEquals(summary.verified(), 500L);
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testGetSummaryVerifiedCountSingleKey() {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder()
                .topic("test-topic")
                .features(List.of("at_least_once"))
                .build();
        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), driver);
        try {
            // Single key with 1000 messages (seq 0-999)
            task.getLastVerifiedProducerCheckpoint()
                    .addPublished("key0", new LongSeq(999, MessageMetadata.empty()));

            ProofSummary summary = task.getSummary();
            assertEquals(summary.verified(), 1000L);
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testReportPrefersFrozenClusterTargetSnapshotAfterStop() throws Exception {
        ProofDriver driver = mock(ProofDriver.class);
        Map<String, Object> brokerMetadata = new HashMap<>();
        brokerMetadata.put("replicas", 2);
        brokerMetadata.put("limits", new HashMap<>(Map.of("cpu", "2", "memory", "4Gi")));

        Map<String, Object> metadata = new HashMap<>();
        metadata.put("clusterResources", new HashMap<>(Map.of("broker", brokerMetadata)));

        Map<String, Object> driverConfigs = new HashMap<>();
        driverConfigs.put("pulsar.service.url", "pulsar://cluster-a:6650");
        driverConfigs.put("pulsar.admin.url", "http://cluster-a:8080");

        Configs configs = new Configs(
                Map.of(),
                Map.of("local_pulsar", new Driver("pulsar", driverConfigs, metadata)));
        Proof proof = Proof.builder()
                .topic("test-topic")
                .driver("local_pulsar")
                .features(List.of("at_least_once"))
                .build();
        ProofTask task = new ProofTask(proof, configs, driver);
        try {
            ProofClusterTarget liveTarget = task.getReport().clusterTargets().getFirst();
            assertEquals(liveTarget.endpoints().get("pulsar.service.url"), "pulsar://cluster-a:6650");
            Map<String, Object> liveClusterResources =
                    (Map<String, Object>) liveTarget.metadata().get("clusterResources");
            Map<String, Object> liveBroker = (Map<String, Object>) liveClusterResources.get("broker");
            assertEquals(liveBroker.get("replicas"), 2);

            ((Map<String, Object>) brokerMetadata.get("limits")).put("memory", "8Gi");
            driverConfigs.put("pulsar.service.url", "pulsar://cluster-b:6650");

            ProofClusterTarget updatedLiveTarget = task.getReport().clusterTargets().getFirst();
            assertEquals(updatedLiveTarget.endpoints().get("pulsar.service.url"), "pulsar://cluster-b:6650");
            Map<String, Object> updatedLiveClusterResources =
                    (Map<String, Object>) updatedLiveTarget.metadata().get("clusterResources");
            Map<String, Object> updatedLiveBroker = (Map<String, Object>) updatedLiveClusterResources.get("broker");
            Map<String, Object> updatedLiveLimits = (Map<String, Object>) updatedLiveBroker.get("limits");
            assertEquals(updatedLiveLimits.get("memory"), "8Gi");

            AtomicBoolean running = getRunningFlag(task);
            running.set(true);
            task.stop();

            ((Map<String, Object>) brokerMetadata.get("limits")).put("memory", "16Gi");
            driverConfigs.put("pulsar.service.url", "pulsar://cluster-c:6650");

            ProofClusterTarget frozenTarget = task.getReport().clusterTargets().getFirst();
            assertEquals(frozenTarget.endpoints().get("pulsar.service.url"), "pulsar://cluster-b:6650");
            assertNotNull(frozenTarget.metadata());
            Map<String, Object> frozenClusterResources =
                    (Map<String, Object>) frozenTarget.metadata().get("clusterResources");
            Map<String, Object> frozenBroker = (Map<String, Object>) frozenClusterResources.get("broker");
            Map<String, Object> frozenLimits = (Map<String, Object>) frozenBroker.get("limits");
            assertEquals(frozenLimits.get("memory"), "8Gi");
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testCompleteProofAfterDurationRunsFinalVerification() throws Exception {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder()
                .id("test-proof")
                .topic("test-topic")
                .features(List.of("at_least_once", "ordering"))
                .duration(20)
                .checkPointInterval(5)
                .timeout(30)
                .build();
        proof.setStartTime(DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(
                LocalDateTime.now().minusSeconds(25)));

        ProofTask task = spy(new ProofTask(proof, new Configs(Map.of(), Map.of()), driver));

        try {
            // stop() requires running=true to proceed past the CAS guard
            getRunningFlag(task).set(true);

            // Build producer checkpoint: 4 keys, each with seq 0-249 (250 msgs each)
            ProducerCheckpoint producerCp = new ProducerCheckpoint();
            producerCp.addPublished("key0", new LongSeq(249, MessageMetadata.empty()));
            producerCp.addPublished("key1", new LongSeq(249, MessageMetadata.empty()));
            producerCp.addPublished("key2", new LongSeq(249, MessageMetadata.empty()));
            producerCp.addPublished("key3", new LongSeq(249, MessageMetadata.empty()));

            // Build consumer checkpoint: consumer has received all messages
            ConsumerCheckPoint consumerCp = new ConsumerCheckPoint();
            for (String key : List.of("key0", "key1", "key2", "key3")) {
                ConsumerCheckPoint.SeqRange range = new ConsumerCheckPoint.SeqRange();
                range.setStart(new LongSeq(0, MessageMetadata.empty()));
                range.setEnd(new LongSeq(249, MessageMetadata.empty()));
                TreeMap<String, ConsumerCheckPoint.SeqRange> rangeMap = new TreeMap<>();
                rangeMap.put("t0", range);
                consumerCp.addKey(key, rangeMap);
            }

            doReturn(Pair.of(producerCp, consumerCp)).when(task).aggregateCheckpoints();

            // Invoke private completeProofAfterDuration via reflection
            Method method = ProofTask.class.getDeclaredMethod("completeProofAfterDuration");
            method.setAccessible(true);
            method.invoke(task);

            // After final verification, lastVerifiedProducerCheckpoint should be updated
            ProofSummary summary = task.getSummary();
            // (249+1) * 4 = 1000
            assertEquals(summary.verified(), 1000L);
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testCompleteProofAfterDurationWhenConsumerLagsBehind() throws Exception {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder()
                .id("test-proof")
                .topic("test-topic")
                .features(List.of("at_least_once", "ordering"))
                .duration(20)
                .checkPointInterval(5)
                .timeout(30)
                .build();
        proof.setStartTime(DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(
                LocalDateTime.now().minusSeconds(25)));

        ProofTask task = spy(new ProofTask(proof, new Configs(Map.of(), Map.of()), driver));

        try {
            getRunningFlag(task).set(true);

            // Simulate a prior verified checkpoint at seq 124 per key
            task.getLastVerifiedProducerCheckpoint()
                    .addPublished("key0", new LongSeq(124, MessageMetadata.empty()));
            task.getLastVerifiedProducerCheckpoint()
                    .addPublished("key1", new LongSeq(124, MessageMetadata.empty()));

            // Latest producer checkpoint has seq 249 but consumer only reached 200
            ProducerCheckpoint producerCp = new ProducerCheckpoint();
            producerCp.addPublished("key0", new LongSeq(249, MessageMetadata.empty()));
            producerCp.addPublished("key1", new LongSeq(249, MessageMetadata.empty()));

            ConsumerCheckPoint consumerCp = new ConsumerCheckPoint();
            for (String key : List.of("key0", "key1")) {
                ConsumerCheckPoint.SeqRange range = new ConsumerCheckPoint.SeqRange();
                range.setStart(new LongSeq(0, MessageMetadata.empty()));
                range.setEnd(new LongSeq(200, MessageMetadata.empty()));
                TreeMap<String, ConsumerCheckPoint.SeqRange> rangeMap = new TreeMap<>();
                rangeMap.put("t0", range);
                consumerCp.addKey(key, rangeMap);
            }

            doReturn(Pair.of(producerCp, consumerCp)).when(task).aggregateCheckpoints();

            Method method = ProofTask.class.getDeclaredMethod("completeProofAfterDuration");
            method.setAccessible(true);
            method.invoke(task);

            // Consumer hasn't caught up (200 < 249), so lastVerified stays at prior checkpoint
            ProofSummary summary = task.getSummary();
            // Prior checkpoint: (124+1) * 2 = 250
            assertEquals(summary.verified(), 250L);
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testFinalVerificationUsesDedicatedTimeoutSetting() throws Exception {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder()
                .id("test-proof")
                .topic("test-topic")
                .features(List.of("at_least_once", "ordering"))
                .timeout(180)
                .build();

        ProofTask task = spy(new ProofTask(
                proof,
                new Configs(Map.of(), Map.of(), Map.of("finalVerificationTimeoutSeconds", 0)),
                driver));

        try {
            ProducerCheckpoint producerCp = new ProducerCheckpoint();
            producerCp.addPublished("key0", new LongSeq(10, MessageMetadata.empty()));

            ConsumerCheckPoint consumerCp = new ConsumerCheckPoint();
            ConsumerCheckPoint.SeqRange range = new ConsumerCheckPoint.SeqRange();
            range.setStart(new LongSeq(0, MessageMetadata.empty()));
            range.setEnd(new LongSeq(5, MessageMetadata.empty()));
            TreeMap<String, ConsumerCheckPoint.SeqRange> rangeMap = new TreeMap<>();
            rangeMap.put("t0", range);
            consumerCp.addKey("key0", rangeMap);

            doReturn(Pair.of(producerCp, consumerCp)).when(task).aggregateCheckpoints();

            Method method = ProofTask.class.getDeclaredMethod("runFinalVerification");
            method.setAccessible(true);
            method.invoke(task);

            verify(task, times(1)).aggregateCheckpoints();
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testManualStopReportsStoppedStatus() throws Exception {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder()
                .id("test-proof")
                .topic("test-topic")
                .features(List.of("at_least_once", "ordering"))
                .timeout(30)
                .build();

        ProofTask task = new ProofTask(proof, new Configs(Map.of(), Map.of()), driver);

        try {
            getRunningFlag(task).set(true);

            task.requestStop();

            assertEquals(task.getReport().status(), "stopped");
            assertEquals(task.getReport().resultReason(), "The run was stopped manually.");
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    @Test
    public void testCompleteProofAfterDurationSharedMode() throws Exception {
        ProofDriver driver = mock(ProofDriver.class);
        Proof proof = Proof.builder()
                .id("test-proof")
                .topic("test-topic")
                .features(List.of("at_least_once"))
                .duration(20)
                .checkPointInterval(5)
                .timeout(30)
                .pulsar(PulsarProofConfig.builder()
                        .consumerConfig(Map.of("subscriptionType", "Shared"))
                        .build())
                .build();
        proof.setStartTime(DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(
                LocalDateTime.now().minusSeconds(25)));

        ProofTask task = spy(new ProofTask(proof, new Configs(Map.of(), Map.of()), driver));

        try {
            assertTrue(task.isSharedMode());
            // stop() requires running=true to proceed past the CAS guard
            getRunningFlag(task).set(true);

            ProducerCheckpoint producerCp = new ProducerCheckpoint();
            producerCp.addPublished("key0", new LongSeq(249, MessageMetadata.empty()));
            producerCp.addPublished("key1", new LongSeq(249, MessageMetadata.empty()));

            ConsumerCheckPoint consumerCp = new ConsumerCheckPoint();
            for (String key : List.of("key0", "key1")) {
                ConsumerCheckPoint.SeqRange range = new ConsumerCheckPoint.SeqRange();
                range.setStart(new LongSeq(0, MessageMetadata.empty()));
                range.setEnd(new LongSeq(249, MessageMetadata.empty()));
                TreeMap<String, ConsumerCheckPoint.SeqRange> rangeMap = new TreeMap<>();
                rangeMap.put("t0", range);
                consumerCp.addKey(key, rangeMap);
            }

            doReturn(Pair.of(producerCp, consumerCp)).when(task).aggregateCheckpoints();

            Method method = ProofTask.class.getDeclaredMethod("completeProofAfterDuration");
            method.setAccessible(true);
            method.invoke(task);

            // Shared mode: verified = sum of (watermark + 1)
            // Watermarks should be 249 per key → (249+1)*2 = 500
            ProofSummary summary = task.getSummary();
            assertEquals(summary.verified(), 500L);
        } finally {
            task.getExecutor().shutdownNow();
        }
    }

    private static AtomicBoolean getRunningFlag(ProofTask task) throws Exception {
        Field runningField = ProofTask.class.getDeclaredField("running");
        runningField.setAccessible(true);
        return (AtomicBoolean) runningField.get(task);
    }
}
