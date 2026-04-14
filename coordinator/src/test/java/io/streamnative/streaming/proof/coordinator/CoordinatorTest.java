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
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

import io.streamnative.streaming.proof.common.records.Configs;
import io.streamnative.streaming.proof.common.records.Driver;
import io.streamnative.streaming.proof.common.records.Proof;
import io.streamnative.streaming.proof.common.records.ProofReport;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class CoordinatorTest {

    @Test
    public void testListProofsSortsByStartTimeDescending() {
        Coordinator coordinator = new Coordinator();
        addProof(coordinator, "oldest", "2026-03-31T10:00:00");
        addProof(coordinator, "newest", "2026-03-31T12:00:00");
        addProof(coordinator, "middle", "2026-03-31T11:00:00");

        List<Proof> proofs = coordinator.listProofs();

        assertEquals(
                proofs.stream().map(Proof::getId).toList(),
                List.of("newest", "middle", "oldest"));
    }

    @Test
    public void testListProofsWithStatusSortsByStartTimeDescending() {
        Coordinator coordinator = new Coordinator();
        addProof(coordinator, "without-time", null);
        addProof(coordinator, "newest", "2026-03-31T12:00:00");
        addProof(coordinator, "oldest", "2026-03-31T10:00:00");

        List<Map<String, Object>> proofs = coordinator.listProofsWithStatus();

        assertEquals(
                proofs.stream().map(item -> (String) item.get("id")).toList(),
                List.of("newest", "oldest", "without-time"));
        assertEquals(proofs.getFirst().get("status"), "running");
    }

    @Test
    public void testCreateProofRejectsNegativeFinalWaitSeconds() {
        Coordinator coordinator = new Coordinator();
        coordinator.updateConfigs(new Configs(
                Map.of(),
                Map.of("test-driver", new Driver("kafka", Map.of(), null))));

        Proof proof = Proof.builder()
                .name("invalid-final-wait")
                .driver("test-driver")
                .features(List.of("at_least_once"))
                .topic("test-topic")
                .finalWaitSeconds(-1)
                .build();

        IllegalArgumentException error = expectThrows(IllegalArgumentException.class,
                () -> coordinator.createProof(proof));

        assertEquals(error.getMessage(), "Final wait seconds must be 0 or greater.");
    }

    private static void addProof(Coordinator coordinator, String id, String startTime) {
        Proof proof = Proof.builder()
                .id(id)
                .name(id)
                .startTime(startTime)
                .build();
        ProofTask task = mock(ProofTask.class);
        doReturn(proof).when(task).getProof();
        doReturn(new ProofReport(proof, "running", null, null, null, null, null, null))
                .when(task).getReport();
        coordinator.getProofs().put(id, task);
    }
}
