/**
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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.streamnative.streaming.proof.common.CoordinatorHttpClient;
import io.streamnative.streaming.proof.common.records.Configs;
import io.streamnative.streaming.proof.common.records.Driver;
import io.streamnative.streaming.proof.common.records.Proof;
import io.streamnative.streaming.proof.common.records.ProofDetails;
import io.streamnative.streaming.proof.worker.WorkerStarter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.common.utils.Time;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Dsl;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CoordinatorAPITest {

    private final CoordinatorStarter server = new CoordinatorStarter();
    private final WorkerStarter worker = new WorkerStarter();
    private final AsyncHttpClient client = Dsl.asyncHttpClient();
    private CoordinatorHttpClient httpClient;
    private int workerPort;
    private int kafkaPort;
    private TestingServer zkServer;
    private KafkaServer kafkaServer;
    private String kafkaLogDir;

    @BeforeMethod
    public void setUp() throws Exception {
        int port = getFreePort();
        workerPort = getFreePort();
        kafkaPort = getFreePort();
        zkServer = new TestingServer();
        kafkaLogDir = System.getProperty("java.io.tmpdir") + "/kafka-logs-" + System.currentTimeMillis();
        Properties props = new Properties();
        props.put("zookeeper.connect", zkServer.getConnectString());
        props.put("broker.id", "1");
        props.put("listeners", "PLAINTEXT://localhost:" + kafkaPort);
        props.put("log.dirs", kafkaLogDir);
        props.put("default.replication.factor", 1);
        props.put("offsets.topic.replication.factor", "1");
        KafkaConfig config = new KafkaConfig(props);
        kafkaServer = new KafkaServer(config, Time.SYSTEM, scala.Option.empty(), false);
        kafkaServer.startup();
        this.server.start(port);
        this.worker.start(workerPort);
        this.httpClient = new CoordinatorHttpClient(client, "localhost", port);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        server.stop();
        worker.stop();
        if (kafkaServer != null) {
            kafkaServer.shutdown();
            kafkaServer.awaitShutdown();
        }
        if (zkServer != null) {
            zkServer.close();
        }
        // Clean up Kafka log directory
        if (kafkaLogDir != null) {
            java.nio.file.Path logPath = java.nio.file.Paths.get(kafkaLogDir);
            if (java.nio.file.Files.exists(logPath)) {
                java.nio.file.Files.walk(logPath)
                    .sorted(java.util.Comparator.reverseOrder())
                    .map(java.nio.file.Path::toFile)
                    .forEach(java.io.File::delete);
            }
        }
    }

    @Test
    public void testConfigsAPI() throws Exception {
        Configs configs = new Configs(Map.of("worker1", "http://localhost:" + workerPort), Map.of("kafka_driver",
                new Driver("kafka", Map.of("bootstrap.servers", "localhost:9092"))));
        httpClient.putConfigs(configs).join();
        Configs configs1 = httpClient.getConfigs().join();
        assertEquals(configs1, configs);
        httpClient.deleteConfigs(configs).join();
        Configs configs2 = httpClient.getConfigs().join();
        assertTrue(configs2.workers().isEmpty());
        assertTrue(configs2.drivers().isEmpty());
    }

    @Test
    public void testProofAPI() throws Exception {
        Configs configs = new Configs(Map.of("worker1", "http://localhost:" + workerPort), Map.of("kafka_driver",
                new Driver("kafka", Map.of("bootstrap.servers", "localhost:" + kafkaPort))));
        httpClient.putConfigs(configs).join();
        Configs configs1 = httpClient.getConfigs().join();
        assertEquals(configs1, configs);
        Proof proof = Proof.builder()
            .name(UUID.randomUUID().toString())
            .driver("kafka_driver")
            .keys(100)
            .partitions(10)
            .producers(4)
            .consumers(4)
            .features(List.of("at_least_once", "ordering"))
            .checkPointInterval(5)
            .msgRate(1000)
            .timeout(180)
            .build();
        httpClient.createProof(proof).join();
        List<Proof> proofs = httpClient.listProofs().join();
        assertEquals(proofs.size(), 1);
        Awaitility.await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> {
            ProofDetails details = httpClient.getProof(proofs.getFirst().getId()).join();
            assertTrue(details.summary().verified() > 0);
        });
        httpClient.stopProof(proofs.getFirst().getId()).join();
        List<Proof> proofs2 = httpClient.listProofs().join();
        assertEquals(proofs2.size(), 1);
        httpClient.deleteProof(proofs.getFirst().getId()).join();
        List<Proof> proofs3 = httpClient.listProofs().join();
        assertEquals(proofs3.size(), 0);
    }

    private static int getFreePort() {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
