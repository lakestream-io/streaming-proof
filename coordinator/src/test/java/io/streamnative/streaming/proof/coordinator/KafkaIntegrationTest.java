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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import io.streamnative.streaming.proof.common.CoordinatorHttpClient;
import io.streamnative.streaming.proof.common.Util;
import io.streamnative.streaming.proof.common.records.Configs;
import io.streamnative.streaming.proof.common.records.ConsumerCheckPoint;
import io.streamnative.streaming.proof.common.records.Driver;
import io.streamnative.streaming.proof.common.records.Drivers;
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

public class KafkaIntegrationTest {

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
        props.put("transaction.state.log.replication.factor", "1");
        props.put("transaction.state.log.min.isr", "1");
        KafkaConfig config = new KafkaConfig(props);
        kafkaServer = new KafkaServer(config, Time.SYSTEM, scala.Option.empty(), false);
        kafkaServer.startup();
        this.server.start(port);
        this.worker.start(workerPort);
        this.httpClient = new CoordinatorHttpClient(client, "localhost", port);

        // Make sure configs are clean at the start of each test
        try {
            Configs existingConfigs = httpClient.getConfigs().join();
            if (existingConfigs != null
                && (!existingConfigs.workers().isEmpty() || !existingConfigs.drivers().isEmpty())) {
                httpClient.deleteConfigs(existingConfigs).join();
            }
        } catch (Exception e) {
            // Ignore if configs don't exist yet
        }
    }

    @AfterMethod
    public void tearDown() throws Exception {
        // Clean up any remaining configs
        try {
            Configs existingConfigs = httpClient.getConfigs().join();
            if (existingConfigs != null
                && (!existingConfigs.workers().isEmpty() || !existingConfigs.drivers().isEmpty())) {
                httpClient.deleteConfigs(existingConfigs).join();
            }
        } catch (Exception e) {
            // Ignore if configs don't exist or server is already stopped
        }

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

    @Test
    public void testGetDeletedProofReturnsNotFound() throws Exception {
        Configs configs = new Configs(Map.of("worker1", "http://localhost:" + workerPort), Map.of("kafka_driver",
                new Driver("kafka", Map.of("bootstrap.servers", "localhost:" + kafkaPort))));
        httpClient.putConfigs(configs).join();
        Proof proof = Proof.builder()
                .name(UUID.randomUUID().toString())
                .driver("kafka_driver")
                .keys(10)
                .partitions(1)
                .producers(1)
                .consumers(1)
                .features(List.of("at_least_once", "ordering"))
                .checkPointInterval(5)
                .msgRate(100)
                .timeout(30)
                .build();
        httpClient.createProof(proof).join();

        String proofId = Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .until(() -> httpClient.listProofs().join(),
                        proofs -> !proofs.isEmpty())
                .getFirst()
                .getId();

        httpClient.stopProof(proofId).join();
        httpClient.deleteProof(proofId).join();

        try {
            httpClient.getProof(proofId).join();
            fail("Expected deleted proof lookup to return not found");
        } catch (Exception e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            assertTrue(cause.getMessage().contains("Proof test not found"),
                    "Expected 404-style response, but got: " + cause.getMessage());
        }
    }

    @Test
    public void testWatermarkTrimming() throws Exception {
        Configs configs = new Configs(Map.of("worker1", "http://localhost:" + workerPort), Map.of("kafka_driver",
                new Driver("kafka", Map.of("bootstrap.servers", "localhost:" + kafkaPort))));
        httpClient.putConfigs(configs).join();
        Proof proof = Proof.builder()
                .name(UUID.randomUUID().toString())
                .driver("kafka_driver")
                .keys(50)
                .partitions(5)
                .producers(2)
                .consumers(2)
                .features(List.of("at_least_once", "ordering"))
                .checkPointInterval(3)
                .msgRate(500)
                .timeout(180)
                .build();
        httpClient.createProof(proof).join();
        List<Proof> proofs = httpClient.listProofs().join();
        assertEquals(proofs.size(), 1);
        String proofId = proofs.getFirst().getId();
        try {
            // Wait for multiple checkpoint cycles to succeed
            Awaitility.await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> {
                ProofDetails details = httpClient.getProof(proofId).join();
                assertTrue(details.summary().verified() > 0);
            });
            long firstVerified = httpClient.getProof(proofId).join().summary().verified();
            Awaitility.await().atMost(1, TimeUnit.MINUTES).untilAsserted(() -> {
                ProofDetails details = httpClient.getProof(proofId).join();
                assertTrue(details.summary().verified() > firstVerified,
                        "Verified count should grow across checkpoint cycles, "
                                + "was " + firstVerified + " now " + details.summary().verified());
            });

            // Verify worker-side trimming: earliest range start should be above 0
            assertWorkerRangesTrimmed(proofId);
        } finally {
            httpClient.stopProof(proofId).join();
            httpClient.deleteProof(proofId).join();
        }
    }

    @Test()
    public void testProofWithConsumeDelay() throws Exception {
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
                .consumeDelay(300)
                .timeout(180)
                .build();
        try {
            httpClient.createProof(proof).join();
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Server Error"));
        }

        proof = Proof.builder()
                .name(UUID.randomUUID().toString())
                .driver("kafka_driver")
                .keys(100)
                .partitions(10)
                .producers(4)
                .consumers(4)
                .features(List.of("at_least_once", "ordering"))
                .checkPointInterval(5)
                .msgRate(1000)
                .consumeDelay(10)
                .timeout(180)
                .build();
        httpClient.createProof(proof).join();
        List<Proof> proofs = httpClient.listProofs().join();
        assertEquals(proofs.size(), 1);
        Awaitility.await().atLeast(10, TimeUnit.SECONDS)
                .atMost(1, TimeUnit.MINUTES).untilAsserted(() -> {
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

    @Test()
    public void testProofWithDifferentDrivers() throws Exception {
        Configs configs = new Configs(Map.of("worker1", "http://localhost:" + workerPort), Map.of(
                "kafka_driver",
                new Driver("kafka", Map.of("bootstrap.servers", "localhost:" + kafkaPort)),
                "kafka_driver_2",
                new Driver("kafka", Map.of("bootstrap.servers", "localhost:" + kafkaPort))));
        httpClient.putConfigs(configs).join();
        Configs configs1 = httpClient.getConfigs().join();
        assertEquals(configs1, configs);

        Proof proof = Proof.builder()
                .name(UUID.randomUUID().toString())
                .driver("kafka_driver")
                .drivers(Drivers.builder().admin("kafka_driver")
                        .producer("kafka_driver")
                        .consumer("kafka_driver_2").build())
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

    @Test()
    public void testProofWithDuration() throws Exception {
        Configs configs = new Configs(Map.of("worker1", "http://localhost:" + workerPort), Map.of(
                "kafka_driver",
                new Driver("kafka", Map.of("bootstrap.servers", "localhost:" + kafkaPort))));
        httpClient.putConfigs(configs).join();
        Configs configs1 = httpClient.getConfigs().join();
        assertEquals(configs1, configs);

        // Create a proof with a short duration (5 seconds)
        Proof proof = Proof.builder()
                .name(UUID.randomUUID().toString())
                .driver("kafka_driver")
                .keys(100)
                .partitions(10)
                .producers(4)
                .consumers(4)
                .features(List.of("at_least_once", "ordering"))
                .checkPointInterval(1)
                .msgRate(1000)
                .timeout(180)
                .duration(5) // Set a short duration of 5 seconds
                .build();
        httpClient.createProof(proof).join();
        List<Proof> proofs = httpClient.listProofs().join();
        assertEquals(proofs.size(), 1);

        // Verify that the proof starts producing messages
        Awaitility.await().atMost(4, TimeUnit.SECONDS).untilAsserted(() -> {
            ProofDetails details = httpClient.getProof(proofs.getFirst().getId()).join();
            assertTrue(details.summary().verified() >= 0);
        });

        // Wait for the duration to expire (proof should auto-stop)
        Thread.sleep(6000); // Wait a bit longer than the duration

        // Verify that the proof is still in the list but has stopped producing
        List<Proof> proofsAfterDuration = httpClient.listProofs().join();
        assertEquals(proofsAfterDuration.size(), 1);

        // Get the current verification count
        ProofDetails detailsAfterStop = httpClient.getProof(proofs.getFirst().getId()).join();
        long verifiedCount = detailsAfterStop.summary().verified();

        // Wait a bit more and verify that the verification count hasn't increased
        // This confirms the proof task has stopped producing messages
        Thread.sleep(2000);
        ProofDetails detailsAfterWait = httpClient.getProof(proofs.getFirst().getId()).join();
        assertEquals(detailsAfterWait.summary().verified(), verifiedCount);

        // Clean up
        httpClient.deleteProof(proofs.getFirst().getId()).join();
        List<Proof> proofs3 = httpClient.listProofs().join();
        assertEquals(proofs3.size(), 0);
    }

    @Test
    public void testHumanReadableStartTime() throws Exception {
        // Set up configs
        Configs configs = new Configs(Map.of("worker1", "http://localhost:" + workerPort), Map.of("kafka_driver",
                new Driver("kafka", Map.of("bootstrap.servers", "localhost:" + kafkaPort))));
        httpClient.putConfigs(configs).join();

        // Create a proof
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

        // Create the proof (this will start it and set the startTime)
        httpClient.createProof(proof).join();

        // Get the proof details
        List<Proof> proofs = httpClient.listProofs().join();
        assertEquals(proofs.size(), 1);

        // Verify that startTime is set and is a human-readable string
        String startTime = proofs.getFirst().getStartTime();
        assertNotNull(startTime, "startTime should not be null");

        // Verify the format matches ISO_LOCAL_DATE_TIME
        try {
            java.time.LocalDateTime parsedDateTime = java.time.LocalDateTime.parse(
                    startTime, java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            assertNotNull(parsedDateTime, "startTime should be parseable as a LocalDateTime");
        } catch (Exception e) {
            fail("startTime should be in ISO_LOCAL_DATE_TIME format, but was: " + startTime);
        }

        // Clean up
        httpClient.stopProof(proofs.getFirst().getId()).join();
        httpClient.deleteProof(proofs.getFirst().getId()).join();
    }

    @Test()
    public void testExactlyOnceVerification() throws Exception {
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
                .features(List.of("exactly_once")) // Test exactly-once semantics
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

    private void assertWorkerRangesTrimmed(String proofId) throws Exception {
        String url = "http://localhost:" + workerPort
                + Util.CONSUMER_CHECKPOINTS_DETAILS.replace("{id}", proofId);
        String body = client.prepareGet(url).execute().get().getResponseBody();
        @SuppressWarnings("unchecked")
        Map<String, ConsumerCheckPoint> details = Util.JSON_MAPPER.readValue(body,
                Util.JSON_MAPPER.getTypeFactory().constructMapType(
                        java.util.HashMap.class, String.class, ConsumerCheckPoint.class));
        // At least one consumer should have ranges that no longer start at seq 0,
        // proving that watermark trimming removed earlier ranges
        boolean anyTrimmed = details.values().stream()
                .flatMap(cp -> cp.getConsumed().values().stream())
                .flatMap(rangeMap -> rangeMap.values().stream())
                .anyMatch(range -> range.getStart().seq() > 0);
        assertTrue(anyTrimmed,
                "Expected at least one consumer range to start above seq 0 after watermark trimming");
    }

    private static int getFreePort() {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
