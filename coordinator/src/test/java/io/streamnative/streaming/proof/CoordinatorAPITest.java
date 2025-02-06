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
package io.streamnative.streaming.proof;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.streamnative.streaming.proof.common.CoordinatorHttpClient;
import io.streamnative.streaming.proof.common.records.Configs;
import io.streamnative.streaming.proof.common.records.Driver;
import io.streamnative.streaming.proof.coordinator.CoordinatorStarter;
import io.streamnative.streaming.proof.worker.WorkerStarter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.util.Map;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Dsl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class CoordinatorAPITest {

    private final CoordinatorStarter server = new CoordinatorStarter();
    private final WorkerStarter worker = new WorkerStarter();
    private final AsyncHttpClient client = Dsl.asyncHttpClient();
    private CoordinatorHttpClient httpClient;
    private int workerPort;

    @BeforeClass
    public void setUp() {
        int port = getFreePort();
        workerPort = getFreePort();
        this.server.start(port);
        this.worker.start(workerPort);
        this.httpClient = new CoordinatorHttpClient(client, "localhost", port);
    }

    @AfterClass
    public void tearDown() {
        this.server.stop();
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

    private static int getFreePort() {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
