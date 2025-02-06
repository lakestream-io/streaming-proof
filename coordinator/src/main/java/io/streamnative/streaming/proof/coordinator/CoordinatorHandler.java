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

import static io.streamnative.streaming.proof.common.Util.CREATE_PROOF;
import static io.streamnative.streaming.proof.common.Util.DELETE_CONFIG;
import static io.streamnative.streaming.proof.common.Util.DELETE_PROOF;
import static io.streamnative.streaming.proof.common.Util.GET_CONFIG;
import static io.streamnative.streaming.proof.common.Util.GET_PROOF;
import static io.streamnative.streaming.proof.common.Util.LIST_PROOFS;
import static io.streamnative.streaming.proof.common.Util.PUT_CONFIG;
import static io.streamnative.streaming.proof.common.Util.STOP_PROOF;

import io.javalin.Javalin;
import io.javalin.http.Context;
import io.streamnative.streaming.proof.common.Util;
import io.streamnative.streaming.proof.common.records.Configs;
import io.streamnative.streaming.proof.common.records.Proof;
import io.streamnative.streaming.proof.common.records.ProofDetails;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

/**
 * HTTP request handler for the Streaming Proof Coordinator.
 * This class manages all REST API endpoints for the coordinator service,
 * handling proof test management and system configuration operations.
 *
 * <p>The handler supports the following operations:
 * <ul>
 *   <li>Creating, retrieving, stopping, and deleting proof tests</li>
 *   <li>Listing all active proof tests</li>
 *   <li>Managing system configurations for workers and drivers</li>
 * </ul>
 */
@Slf4j
public class CoordinatorHandler {

    /** The coordinator instance that manages proof tests and configurations */
    private final Coordinator coordinator;

    /**
     * Creates a new CoordinatorHandler and registers all REST API endpoints.
     *
     * @param app The Javalin application instance to register endpoints with
     * @param coordinator The coordinator instance to delegate operations to
     */
    public CoordinatorHandler(Javalin app, Coordinator coordinator) {
        this.coordinator = coordinator;
        app.post(CREATE_PROOF, this::handleCreateProof);
        app.get(GET_PROOF, this::handleGetProof);
        app.put(STOP_PROOF, this::handleStopProof);
        app.delete(DELETE_PROOF, this::handleDeleteProof);
        app.get(LIST_PROOFS, this::handleListProofs);
        app.put(PUT_CONFIG, this::handlePutConfig);
        app.get(GET_CONFIG, this::handleGetConfig);
        app.delete(DELETE_CONFIG, this::handleDeleteConfig);
    }

    /**
     * Handles requests to create a new proof test.
     * Deserializes the proof configuration from the request body and
     * delegates to the coordinator to create and start the test.
     *
     * @param ctx The Javalin context containing the request details
     * @throws Exception if there's an error processing the request
     */
    private void handleCreateProof(Context ctx) throws Exception {
        log.info("New create proof request: {}", ctx.body());
        Proof proof = Util.JSON_MAPPER.readValue(ctx.body(), Proof.class);
        if (proof.getTopic() == null || proof.getTopic().isEmpty()) {
            proof.setTopic(UUID.randomUUID().toString());
        }
        coordinator.createProof(proof);
        ctx.result(Util.JSON_WRITER.writeValueAsString(proof));
    }

    /**
     * Handles requests to retrieve details about a specific proof test.
     *
     * @param ctx The Javalin context containing the proof ID in path parameters
     * @throws Exception if there's an error processing the request
     */
    private void handleGetProof(Context ctx) throws Exception {
        String id = ctx.pathParam("id");
        ProofDetails proofDetails = coordinator.getProof(id);
        if (proofDetails == null) {
            ctx.status(404).result("Proof test not found: " + id);
            return;
        }
        ctx.result(Util.JSON_WRITER.writeValueAsString(proofDetails));
    }

    private void handleStopProof(Context ctx) throws Exception {
        String id = ctx.pathParam("id");
        if (!coordinator.getProofs().containsKey(id)) {
            ctx.status(404).result("Proof test not found: " + id);
            return;
        }
        log.info("New stop proof request: {}", id);
        coordinator.stopProof(id);
    }

    private void handleDeleteProof(Context ctx) throws Exception {
        String id = ctx.pathParam("id");
        if (!coordinator.getProofs().containsKey(id)) {
            ctx.status(404).result("Proof test not found: " + id);
            return;
        }
        log.info("New delete proof request: {}", id);
        coordinator.removeProof(id);
    }

    /**
     * Handles requests to list all active proof tests.
     *
     * @param ctx The Javalin context for the response
     * @throws Exception if there's an error processing the request
     */
    private void handleListProofs(Context ctx) throws Exception {
        ctx.result(Util.JSON_WRITER.writeValueAsString(coordinator.listProofs()));
    }

    /**
     * Handles requests to update or create system configurations.
     * The configuration is deserialized from the request body and
     * applied to the system.
     *
     * @param ctx The Javalin context containing the configuration in the request body
     * @throws Exception if there's an error processing the request
     */
    private void handlePutConfig(Context ctx) throws Exception {
        log.info("New put config request: {}", ctx.body());
        Configs configs = Util.JSON_MAPPER.readValue(ctx.body(), Configs.class);
        coordinator.updateConfigs(configs);
    }

    /**
     * Handles requests to retrieve current system configurations.
     *
     * @param ctx The Javalin context for the response
     * @throws Exception if there's an error processing the request
     */
    private void handleGetConfig(Context ctx) throws Exception {
        log.info("New get config request: {}", ctx.body());
        Configs configs = coordinator.getConfigs();
        if (configs == null) {
            ctx.status(404).result("No configuration found");
            return;
        }
        ctx.result(Util.JSON_WRITER.writeValueAsString(configs));
    }

    /**
     * Handles requests to delete specific system configurations.
     * The configurations to delete are specified in the request body.
     *
     * @param ctx The Javalin context containing the configurations to delete
     * @throws Exception if there's an error processing the request
     */
    private void handleDeleteConfig(Context ctx) throws Exception {
        log.info("New delete config request: {}", ctx.body());
        Configs configs = Util.JSON_MAPPER.readValue(ctx.body(), Configs.class);
        coordinator.deleteConfigs(configs);
    }
}
