/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package io.lakestream.streaming.proof.common;

import static java.net.HttpURLConnection.HTTP_OK;

import com.fasterxml.jackson.core.type.TypeReference;
import io.lakestream.streaming.proof.common.records.Configs;
import io.lakestream.streaming.proof.common.records.Proof;
import io.lakestream.streaming.proof.common.records.ProofDetails;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.asynchttpclient.AsyncHttpClient;

/**
 * HTTP client for interacting with the Streaming Proof Coordinator service.
 * This client provides asynchronous methods to manage configurations and proofs
 * through REST API calls to the coordinator.
 *
 * <p>The client uses AsyncHttpClient for non-blocking HTTP operations and returns
 * CompletableFuture for all operations to support asynchronous programming patterns.
 *
 * <p>Example usage:
 * <pre>{@code
 * AsyncHttpClient asyncClient = Dsl.asyncHttpClient();
 * CoordinatorHttpClient client = new CoordinatorHttpClient(asyncClient, "localhost", 8080);
 *
 * // Managing configurations
 * Configs configs = new Configs(...);
 * client.putConfigs(configs)
 *       .thenCompose(v -> client.getConfigs())
 *       .thenAccept(System.out::println);
 *
 * // Managing proofs
 * Proof proof = Proof.builder()
 *                    .name("test-proof")
 *                    .build();
 * client.createProof(proof)
 *       .thenCompose(v -> client.getProof(proof.getId()))
 *       .thenAccept(System.out::println);
 * }</pre>
 */
public class CoordinatorHttpClient {

    private final AsyncHttpClient client;
    private final String baseUrl;

    /**
     * Creates a new CoordinatorHttpClient instance.
     *
     * @param client The AsyncHttpClient instance to use for HTTP requests
     * @param host The hostname of the coordinator service
     * @param port The port number of the coordinator service
     */
    public CoordinatorHttpClient(AsyncHttpClient client, String host, int port) {
        this.client = client;
        this.baseUrl = String.format("http://%s:%d", host, port);
    }

    /**
     * Updates or creates configurations in the coordinator.
     *
     * @param configs The configuration to update or create
     * @return A CompletableFuture that completes when the operation is finished
     * @throws Exception if the request fails to be sent
     */
    public CompletableFuture<Void> putConfigs(Configs configs) throws Exception {
        return client.preparePut(baseUrl + Util.PUT_CONFIG)
                .setBody(Util.JSON_WRITER.writeValueAsBytes(configs))
                .execute()
                .toCompletableFuture()
                .thenAccept(response -> {
                    if (response.getStatusCode() != HTTP_OK) {
                        throw new RuntimeException("Failed to put configs: " + response.getStatusCode());
                    }
                });
    }

    /**
     * Retrieves the current configurations from the coordinator.
     *
     * @return A CompletableFuture that completes with the current configurations
     */
    public CompletableFuture<Configs> getConfigs() {
        return client.prepareGet(baseUrl + Util.GET_CONFIG)
                .execute()
                .toCompletableFuture()
                .thenApply(response -> {
                    if (response.getStatusCode() != HTTP_OK) {
                        throw new RuntimeException("Failed to get configs: " + response.getResponseBody());
                    }
                    try {
                        return Util.JSON_MAPPER.readValue(response.getResponseBody(), Configs.class);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /**
     * Deletes specified configurations from the coordinator.
     *
     * @param configs The configurations to delete
     * @return A CompletableFuture that completes when the operation is finished
     * @throws Exception if the request fails to be sent
     */
    public CompletableFuture<Void> deleteConfigs(Configs configs) throws Exception {
        return client.prepareDelete(baseUrl + Util.DELETE_CONFIG)
                .setBody(Util.JSON_WRITER.writeValueAsBytes(configs))
                .execute()
                .toCompletableFuture()
                .thenAccept(response -> {
                    if (response.getStatusCode() != HTTP_OK) {
                        throw new RuntimeException("Failed to delete configs: " + response.getResponseBody());
                    }
                });
    }

    /**
     * Creates a new proof test in the coordinator.
     *
     * @param proof The proof test configuration to create
     * @return A CompletableFuture that completes when the operation is finished
     * @throws Exception if the request fails to be sent
     */
    public CompletableFuture<Void> createProof(Proof proof) throws Exception {
        return client.preparePost(baseUrl + Util.CREATE_PROOF)
                .setBody(Util.JSON_WRITER.writeValueAsBytes(proof))
                .execute()
                .toCompletableFuture()
                .thenAccept(response -> {
                    if (response.getStatusCode() != HTTP_OK) {
                        throw new RuntimeException("Failed to create proof: " + response.getResponseBody());
                    }
                });
    }

    /**
     * Retrieves a specific proof test by its ID.
     *
     * @param id The ID of the proof test to retrieve
     * @return A CompletableFuture that completes with the requested proof test
     */
    public CompletableFuture<ProofDetails> getProof(String id) {
        return client.prepareGet(baseUrl + Util.GET_PROOF.replace("{id}", id))
                .execute()
                .toCompletableFuture()
                .thenApply(response -> {
                    if (response.getStatusCode() != HTTP_OK) {
                        throw new RuntimeException("Failed to get proof: " + response.getResponseBody());
                    }
                    try {
                        return Util.JSON_MAPPER.readValue(response.getResponseBody(), ProofDetails.class);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /**
     * Stops a running proof test.
     *
     * @param id The ID of the proof test to stop
     * @return A CompletableFuture that completes when the operation is finished
     * @throws Exception if the request fails to be sent
     */
    public CompletableFuture<Void> stopProof(String id) throws Exception {
        return client.preparePut(baseUrl + Util.STOP_PROOF.replace("{id}", id))
                .execute()
                .toCompletableFuture()
                .thenAccept(response -> {
                    if (response.getStatusCode() != HTTP_OK) {
                        throw new RuntimeException("Failed to update proof: " + response.getResponseBody());
                    }
                });
    }

    /**
     * Deletes a proof test from the coordinator.
     *
     * @param id The ID of the proof test to delete
     * @return A CompletableFuture that completes when the operation is finished
     */
    public CompletableFuture<Void> deleteProof(String id) {
        return client.prepareDelete(baseUrl + Util.DELETE_PROOF.replace("{id}", id))
                .execute()
                .toCompletableFuture()
                .thenAccept(response -> {
                    if (response.getStatusCode() != HTTP_OK) {
                        throw new RuntimeException("Failed to delete proof: " + response.getResponseBody());
                    }
                });
    }

    /**
     * Retrieves a list of all proof tests from the coordinator.
     *
     * @return A CompletableFuture that completes with the list of all proof tests
     */
    public CompletableFuture<List<Proof>> listProofs() {
        return client.prepareGet(baseUrl + Util.LIST_PROOFS)
                .execute()
                .toCompletableFuture()
                .thenApply(response -> {
                    if (response.getStatusCode() != HTTP_OK) {
                        throw new RuntimeException("Failed to list proofs: " + response.getResponseBody());
                    }
                    try {
                        return Util.JSON_MAPPER.readValue(
                                response.getResponseBody(),
                                new TypeReference<List<Proof>>() {}
                        );
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }
}
