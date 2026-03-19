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

import static org.testng.Assert.assertTrue;

import io.streamnative.streaming.proof.common.CoordinatorHttpClient;
import io.streamnative.streaming.proof.common.records.Configs;
import io.streamnative.streaming.proof.common.records.Driver;
import io.streamnative.streaming.proof.common.records.Proof;
import io.streamnative.streaming.proof.common.records.ProofDetails;
import io.streamnative.streaming.proof.common.records.PulsarProofConfig;
import io.streamnative.streaming.proof.worker.WorkerStarter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Dsl;
import org.awaitility.Awaitility;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class PulsarIntegrationTest {

    private CoordinatorStarter server;
    private WorkerStarter worker;
    private final AsyncHttpClient client = Dsl.asyncHttpClient();
    private CoordinatorHttpClient httpClient;
    private PulsarContainer pulsarContainer;

    @BeforeClass
    public void setUp() throws Exception {
        int port = getFreePort();
        int workerPort = getFreePort();

        pulsarContainer = new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:4.0.4"));
        pulsarContainer.start();

        server = new CoordinatorStarter();
        worker = new WorkerStarter();
        server.start(port);
        worker.start(workerPort);
        this.httpClient = new CoordinatorHttpClient(client, "localhost", port);

        Configs configs = new Configs(
                Map.of("worker1", "http://localhost:" + workerPort),
                Map.of("pulsar_driver", new Driver("pulsar", Map.of(
                        "pulsar.service.url", pulsarContainer.getPulsarBrokerUrl(),
                        "pulsar.admin.url", pulsarContainer.getHttpServiceUrl()))));
        httpClient.putConfigs(configs).join();
    }

    @AfterClass
    public void tearDown() throws Exception {
        server.stop();
        worker.stop();
        if (pulsarContainer != null) {
            pulsarContainer.stop();
        }
    }

    @Test
    public void testKeySharedSubscription() throws Exception {
        Proof proof = Proof.builder()
                .name(UUID.randomUUID().toString())
                .driver("pulsar_driver")
                .keys(50)
                .partitions(1)
                .producers(2)
                .consumers(4)
                .features(List.of("at_least_once", "ordering"))
                .checkPointInterval(5)
                .msgRate(500)
                .timeout(180)
                .pulsar(PulsarProofConfig.builder()
                        .consumerConfig(Map.of("subscriptionType", "Key_Shared"))
                        .build())
                .build();
        runProofAndVerify(proof);
    }

    @Test
    public void testFailoverSubscription() throws Exception {
        Proof proof = Proof.builder()
                .name(UUID.randomUUID().toString())
                .driver("pulsar_driver")
                .keys(50)
                .partitions(1)
                .producers(2)
                .consumers(1)
                .features(List.of("at_least_once", "ordering"))
                .checkPointInterval(5)
                .msgRate(500)
                .timeout(180)
                .build();
        runProofAndVerify(proof);
    }

    @Test
    public void testExclusiveSubscription() throws Exception {
        Proof proof = Proof.builder()
                .name(UUID.randomUUID().toString())
                .driver("pulsar_driver")
                .keys(50)
                .partitions(1)
                .producers(2)
                .consumers(1)
                .features(List.of("at_least_once", "ordering"))
                .checkPointInterval(5)
                .msgRate(500)
                .timeout(180)
                .pulsar(PulsarProofConfig.builder()
                        .consumerConfig(Map.of("subscriptionType", "Exclusive"))
                        .build())
                .build();
        runProofAndVerify(proof);
    }

    private void runProofAndVerify(Proof proof) throws Exception {
        httpClient.createProof(proof).join();
        String proofId = findProofId(proof.getName());
        try {
            Awaitility.await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> {
                ProofDetails details = httpClient.getProof(proofId).join();
                assertTrue(details.summary().verified() > 0);
            });
        } finally {
            httpClient.stopProof(proofId).join();
            httpClient.deleteProof(proofId).join();
        }
    }

    private String findProofId(String name) {
        return Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> httpClient.listProofs().join(),
                        proofs -> proofs.stream().anyMatch(p -> name.equals(p.getName())))
                .stream()
                .filter(p -> name.equals(p.getName()))
                .findFirst()
                .orElseThrow()
                .getId();
    }

    private static int getFreePort() {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
