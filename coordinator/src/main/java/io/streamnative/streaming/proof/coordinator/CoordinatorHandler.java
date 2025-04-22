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
import static io.streamnative.streaming.proof.common.Util.GET_PROOF_DETAILS;
import static io.streamnative.streaming.proof.common.Util.LIST_PROOFS;
import static io.streamnative.streaming.proof.common.Util.PUT_CONFIG;
import static io.streamnative.streaming.proof.common.Util.STOP_PROOF;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.streamnative.streaming.proof.common.Util;
import io.streamnative.streaming.proof.common.records.Configs;
import io.streamnative.streaming.proof.common.records.Proof;
import io.streamnative.streaming.proof.common.records.ProofDetails;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;

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
    private final Coordinator coordinator;
    private final Path configPath;
    private volatile boolean running = true;
    private WatchService watchService;

    public CoordinatorHandler(Javalin app, Coordinator coordinator) {
        this(app, coordinator,
                Paths.get("/mnt/streaming-proof/configs")
        );
    }

    public CoordinatorHandler(Javalin app, Coordinator coordinator, Path configPath) {
        this.coordinator = coordinator;
        this.configPath = configPath;
        try {
            this.watchService = FileSystems.getDefault().newWatchService();
            configPath.register(watchService, StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_DELETE, StandardWatchEventKinds.ENTRY_MODIFY);
            loadExistingFiles();
            startFileWatcher();
        } catch (IOException e) {
            log.error("Failed to init watch service", e);
        }

        app.post(CREATE_PROOF, this::handleCreateProof);
        app.get(GET_PROOF, this::handleGetProof);
        app.get(GET_PROOF_DETAILS, this::handleGetProofDetails);
        app.put(STOP_PROOF, this::handleStopProof);
        app.delete(DELETE_PROOF, this::handleDeleteProof);
        app.get(LIST_PROOFS, this::handleListProofs);
        app.put(PUT_CONFIG, this::handlePutConfig);
        app.get(GET_CONFIG, this::handleGetConfig);
        app.delete(DELETE_CONFIG, this::handleDeleteConfig);


    }

    private final YAMLMapper yamlMapper = new YAMLMapper();

    private void loadExistingFiles() {
        try {
            if (Files.exists(configPath)) {
                Files.list(configPath)
                    .filter(path -> path.toString().endsWith(".yaml"))
                    .forEach(file -> {
                        try {
                            Configs configs = yamlMapper.readValue(Files.readString(file), Configs.class);
                            coordinator.updateConfigs(configs);
                            log.info("Loaded existing config file: {}", file);
                        } catch (Exception e) {
                            log.error("Error loading config file: {}", file, e);
                        }
                    });
            } else {
                Files.createDirectories(configPath);
                log.info("Created config directory: {}", configPath);
            }

        } catch (IOException e) {
            log.error("Error loading existing files", e);
        }
    }

    private void startFileWatcher() {
        Thread watchThread = new Thread(() -> {
            while (running) {
                try {
                    WatchKey key = watchService.take();
                    Path dir = (Path) key.watchable();

                    for (WatchEvent<?> event : key.pollEvents()) {
                        WatchEvent.Kind<?> kind = event.kind();
                        Path fileName = (Path) event.context();
                        Path fullPath = dir.resolve(fileName);

                        if (dir.equals(configPath)) {
                            handleConfigFileChange(kind, fullPath);
                        }
                    }
                    key.reset();
                } catch (InterruptedException e) {
                    log.warn("File watcher interrupted", e);
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("Error in file watcher", e);
                }
            }
        }, "FileWatcherThread");
        watchThread.setDaemon(true);
        watchThread.start();
    }

    private void handleConfigFileChange(WatchEvent.Kind<?> kind, Path file) {
        try {
            coordinator.clearAllConfigs();
            loadExistingFiles();
        } catch (Exception e) {
            log.error("Error handling config file change: {}", file, e);
        }
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
            proof.setTopic(RandomStringUtils.secure().nextAlphanumeric(5));
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
        ProofDetails proofDetails = coordinator.getProofSummary(id);
        if (proofDetails == null) {
            ctx.status(404).result("Proof test not found: " + id);
            return;
        }
        ctx.result(Util.JSON_WRITER.writeValueAsString(proofDetails));
    }

    private void handleGetProofDetails(Context ctx) throws Exception {
        String id = ctx.pathParam("id");
        ProofDetails proofDetails = coordinator.getProofDetails(id);
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
