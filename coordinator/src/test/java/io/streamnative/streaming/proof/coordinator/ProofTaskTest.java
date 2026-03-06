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

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.streamnative.streaming.proof.common.ProofDriver;
import io.streamnative.streaming.proof.common.records.Configs;
import io.streamnative.streaming.proof.common.records.Proof;
import java.util.List;
import java.util.Map;
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
}
