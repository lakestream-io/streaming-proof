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

import io.streamnative.streaming.proof.common.records.Configs;
import io.streamnative.streaming.proof.common.records.Driver;
import io.streamnative.streaming.proof.common.records.Proof;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.BeforeMethod;

public class ProofTaskTest {
    private static final String TEST_KEY = "key1";
    private ProofTask proofTask;

    @BeforeMethod
    public void setup() {
        Proof proof = Proof.builder()
                .id("test-proof")
                .driver("kafka")
                .build();
        
        Map<String, String> workers = Map.of(
                "worker1", "http://worker1:8080",
                "worker2", "http://worker2:8080"
        );
        
        Map<String, Driver> drivers = Map.of(
                "kafka", new Driver("kafka", new HashMap<>())
        );
        
        proofTask = new ProofTask(proof, new Configs(workers, drivers));
    }
}